#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <getopt.h>
#include <fnmatch.h>
#include <poll.h>
#include <syslog.h>
#include <mosquitto.h>
#include <eibclient.h>

#include "lib/libt.h"

#define NAME "mqttknxd"
#ifndef VERSION
#define VERSION "<undefined version>"
#endif

/* generic error logging */
#define mylog(loglevel, fmt, ...) \
	({\
		syslog(loglevel, fmt, ##__VA_ARGS__); \
		if (loglevel <= LOG_ERR)\
			exit(1);\
	})
#define ESTR(num)	strerror(num)

/* program options */
static const char help_msg[] =
	NAME ": an EIB/KNX - MQTT coupler\n"
	"usage:	" NAME " [OPTIONS ...] [PATTERN ...]\n"
	"\n"
	" PATTERN	MQTT pattern to subscribe to\n"
	"		Multiple patterns are allowed\n"
	"		Default is '#'\n"
	"Options\n"
	" -V, --version		Show version\n"
	" -v, --verbose		Be more verbose\n"
	" -e, --eib=URI		Specify alternate EIB uri (default ip:localhost)\n"
	"			Like ip:xxx or usb: or ...\n"
	" -m, --mqtt=HOST[:PORT]Specify alternate MQTT host+port\n"
	" -s, --suffix=STR	Give EIB/KNX config topic suffix (default /eib)\n"
	" -S, --evsuffix=STR	Give EIB/KNX event config topic suffix (default /eibevent)\n"
	" -w, --write=STR	Give MQTT topic suffix for writing the topic (default empty)\n"
	" -f, --flags=BITFIELD	Specify default flags for EIB parameters (default 'wt1')\n"
	"			BITFIELD is a sequence of characters of\n"
	"			r	Respond to EIB read requests\n"
	"			w	Update value on EIB writes/transmits\n"
	"			t	Transmit value to EIB on change\n"
	"			1	1bit payloads\n"
	"			4	4bit payloads\n"
	"			8|1B	1byte payloads\n"
	"			16|2B	2byte payloads\n"
	"			32|4B	4byte payloads\n"
	;

#ifdef _GNU_SOURCE
static struct option long_opts[] = {
	{ "help", no_argument, NULL, '?', },
	{ "version", no_argument, NULL, 'V', },
	{ "verbose", no_argument, NULL, 'v', },
	{ "flags", required_argument, NULL, 'f', },

	{ "eib", required_argument, NULL, 'e', },
	{ "mqtt", required_argument, NULL, 'm', },
	{ "suffix", required_argument, NULL, 's', },
	{ "evsuffix", required_argument, NULL, 'S', },
	{ "write", required_argument, NULL, 'w', },

	{ },
};
#else
#define getopt_long(argc, argv, optstring, longopts, longindex) \
	getopt((argc), (argv), (optstring))
#endif
static const char optstring[] = "Vv?f:e:m:s:S:w:";

/* signal handler */
static volatile int sigterm;

static void sighandler(int sig)
{
	sigterm = 1;
}

/* usefull tricks */
__attribute__((format(printf,1,2)))
static const char *csprintf(const char *fmt, ...)
{
	va_list va;
	static char *str;

	if (str)
		free(str);
	str = NULL;
	va_start(va, fmt);
	vasprintf(&str, fmt, va);
	va_end(va);
	return str;
}

/* MQTT parameters */
static const char *mqtt_host = "localhost";
static int mqtt_port = 1883;
static int mqtt_keepalive = 10;
static int mqtt_qos = 2;
static const char *mqtt_write_suffix;
static char *eib_suffix = "/eib";
static char *eibev_suffix = "/eibevent";
static char *default_options = "wt";

/* EIB parameters */
static const char *eib_uri = "ip:localhost";

/* state */
static struct mosquitto *mosq;
static EIBConnection *eib;

/* item */
struct item {
	struct item *next;
	struct item *prev;

	int mvalue; /* last value on MQTT */
	int eibnbits;
	eibaddr_t *paddr;
	int naddr;
	int saddr;
	char *options;
	char *topic;
	char *writetopic;
};
static struct item *items;

struct event {
	struct event *next;
	struct event *prev;

	char *topic;
	char *pattern;
};
static struct event *events;

static const char *eibgaddrtostr(eibaddr_t val);
static void my_eib_write(void *dat);
static void my_eib_response(void *dat);
static void my_eib_request(void *dat);

static inline int item_option(struct item *it, int c)
{
	return !!strchr(it->options ?: default_options, c);
}

static void item_add_addr(struct item *it, eibaddr_t addr)
{
	if (it->naddr >= it->saddr) {
		it->saddr += 4;
		it->paddr = realloc(it->paddr, it->saddr * sizeof(it->paddr[0]));
		if (!it->paddr)
			mylog(LOG_ERR, "realloc failed");
	}
	it->paddr[it->naddr++] = addr;
}

static void delete_item(struct item *it)
{
	libt_remove_timeout(my_eib_write, it);
	libt_remove_timeout(my_eib_response, it);
	libt_remove_timeout(my_eib_request, it);
	if (it->prev)
		it->prev->next = it->next;
	if (it->next)
		it->next->prev = it->prev;
	if (it->paddr)
		free(it->paddr);
	free(it->topic);
	if (it->writetopic)
		free(it->writetopic);
	free(it);
}

static struct item *topictoitem(const char *topic, const char *suffix, int create)
{
	struct item *it;
	int len, slen;

	len = strlen(topic);
	slen = strlen(suffix ?: "");

	for (it = items; it; it = it->next) {
		if (!strncmp(it->topic, topic, len - slen) && !it->topic[len])
			return it;
	}
	if (!create)
		return NULL;

	it = malloc(sizeof(*it));
	memset(it, 0, sizeof(*it));
	it->topic = strdup(topic);
	it->topic[len-slen] = 0;
	if (mqtt_write_suffix)
		asprintf(&it->writetopic, "%s%s", it->topic, mqtt_write_suffix);

	/* insert in linked list */
	it->next = items;
	if (it->next) {
		it->prev = it->next->prev;
		it->next->prev = it;
	} else
		it->prev = (struct item *)(((char *)&items) - offsetof(struct item, next));
	it->prev->next = it;
	return it;
}

static struct event *topictoevent(const char *topic, const char *suffix, int create)
{
	struct event *ev;
	int len, slen;

	len = strlen(topic);
	slen = strlen(suffix ?: "");
	for (ev = events; ev; ev = ev->next) {
		if (!strncmp(ev->topic, topic, len - slen) && !ev->topic[len])
			return ev;
	}

	if (!create)
		return NULL;
	ev = malloc(sizeof(*ev));
	memset(ev, 0, sizeof(*ev));
	ev->topic = strdup(topic);
	ev->topic[len-slen] = 0;

	/* insert in linked list */
	ev->next = events;
	if (ev->next) {
		ev->prev = ev->next->prev;
		ev->next->prev = ev;
	} else
		ev->prev = (struct event *)(((char *)&events) - offsetof(struct event, next));
	ev->prev->next = ev;
	return ev;
}

static void delete_event(struct event *ev)
{
	if (ev->prev)
		ev->prev->next = ev->next;
	if (ev->next)
		ev->next->prev = ev->prev;
	free(ev->topic);
	if (ev->pattern)
		free(ev->pattern);
	free(ev);
}

/* tools */
static const char *eibphaddrtostr(eibaddr_t val)
{
	static char buf[16];

	sprintf(buf, "%i.%i.%i", (val >> 12) & 0x0f, (val >> 8) & 0x0f, val & 0xff);
	return buf;
}
static const char *eibgaddrtostr(eibaddr_t val)
{
	static char buf[16];

	sprintf(buf, "%i/%i/%i", (val >> 11) & 0x1f, (val >> 8) & 0x07, val & 0xff);
	return buf;
}

static eibaddr_t strtoeibgaddr(const char *cstr, char **endp)
{
	char *str;
	eibaddr_t retval = 0, val = 0;

	if (endp)
		*endp = (char *)cstr;
	val = (strtoul(cstr, &str, 10)  & 0x1f) << 11;
	if (*str == '/') {
		val |= (strtoul(str+1, &str, 10)  & 0x7) << 8;
		if (*str == '/') {
			val |= (strtoul(str+1, &str, 10)  & 0xff) << 0;
			/* prepare successfull return values */
			retval = val;
			if (endp)
				*endp = str;
		}
	}
	return retval;
}

/* MQTT iface */
static void my_mqtt_connect(struct mosquitto *mosq, void *obj, int result)
{
	mylog(LOG_INFO, "MQTT connected, result %i", result);
}

static void my_mqtt_log(struct mosquitto *mosq, void *userdata, int level, const char *str)
{
	static const int logpri_map[] = {
		MOSQ_LOG_ERR, LOG_ERR,
		MOSQ_LOG_WARNING, LOG_WARNING,
		MOSQ_LOG_NOTICE, LOG_NOTICE,
		MOSQ_LOG_INFO, LOG_INFO,
		MOSQ_LOG_DEBUG, LOG_DEBUG,
		0,
	};
	int j;

	for (j = 0; logpri_map[j]; j += 2) {
		if (level & logpri_map[j]) {
			mylog(logpri_map[j+1], "[mosquitto] %s", str);
			return;
		}
	}
}

/* schedule next EIB transmission */
static double next_eib_timeslot(void)
{
#define PKT_DELAY	0.1
	static double filled_eib_slot = 0;
	double tnow = libt_now();

	if ((filled_eib_slot + PKT_DELAY) < tnow)
		filled_eib_slot = tnow;
	else
		filled_eib_slot += PKT_DELAY;
	return filled_eib_slot;
}

static const char *eibactions[16] = {
	[0] = "req",
	[1] = "resp",
	[2] = "write",
};

static void my_eib_send(eibaddr_t dst, uint16_t sdat, int value, int nbits)
{
	const char *eibaction;
	int j;
	uint8_t pkt[10];

	eibaction = (eibactions[(sdat >> 6) & 0xf]) ?: "?";

	pkt[0] = sdat >> 8;
	pkt[1] = sdat;
	if (nbits <= 6)
		pkt[1] |= value & 0x3f;
	else for (j = nbits/8; j; --j, value >>= 8)
		pkt[2+j-1] = value & 0xff;

	mylog(LOG_INFO, "eib:>%s %s %i\n", eibaction, eibgaddrtostr(dst), value);
	if (EIBSendGroup(eib, dst, 2+nbits/8, pkt) < 0)
		mylog(LOG_ERR, "eib: %s %s %i failed", eibaction, eibgaddrtostr(dst), value);
}

static void my_eib_write(void *dat)
{
	struct item *it = dat;

	if (it->naddr)
		my_eib_send(it->paddr[0], 0x0080, it->mvalue, it->eibnbits);
}

static void my_eib_response(void *dat)
{
	struct item *it = dat;

	if (it->naddr)
		my_eib_send(it->paddr[0], 0x0040, it->mvalue, it->eibnbits);
}

static void my_eib_request(void *dat)
{
	struct item *it = dat;

	if (it->naddr)
		my_eib_send(it->paddr[0], 0x0000, 0, 0);
}

static int test_suffix(const char *topic, const char *suffix)
{
	int len;

	len = strlen(topic ?: "") - strlen(suffix ?: "");
	if (len < 0)
		return 0;
	/* match suffix */
	return !strcmp(topic+len, suffix ?: "");
}

static void my_mqtt_msg(struct mosquitto *mosq, void *dat, const struct mosquitto_message *msg)
{
	struct item *it;
	struct event *ev;
	char *endp;

	mylog(LOG_INFO, "mqtt:<%s %s", msg->topic, (char *)msg->payload ?: "<null>");
	if (test_suffix(msg->topic, eib_suffix)) {
		/* this is an EIB config parameter */
		char *str;
		eibaddr_t addr;

		it = topictoitem(msg->topic, eib_suffix, msg->payloadlen);
		if (!it || !msg->payloadlen) {
			if (it)
				delete_item(it);
			return;
		}
		/* parse eibaddr */
		it->naddr = 0;
		for (str = strtok(msg->payload, " \t"); str && *str;) {
			addr = strtoeibgaddr(str, &endp);
			if (endp > str)
				item_add_addr(it, addr);
			else
				mylog(LOG_WARNING, "topic '%s': eib address[%i] '%s' invalid",
						it->topic, it->naddr, str);
			/* seek next , */
			str = strchr(endp, ',');
			if (str)
				++str;
			else
				break;
		}
		if (!it->naddr)
			mylog(LOG_WARNING, "%s: configured without EIB/KNX addr!", it->topic);

		/* parse flags */
		if (it->options)
			free(it->options);
		it->options = strtok(NULL, " \t");
		if (it->options)
			it->options = strdup(it->options);

		/* determine eib payload size */
		it->eibnbits = strtoul(strpbrk(it->options ?: "", "0123456789") ?: "1", &str, 10);
		if (str && *str == 'B')
			it->eibnbits *= 8;

		/* refresh cache */
		if (it->naddr && !item_option(it, 'r') && item_option(it, 'w') && item_option(it, 't'))
			/* schedule eib request */
			libt_add_timeouta(next_eib_timeslot(), my_eib_request, it);
		if (it->naddr && item_option(it, 't') && item_option(it, 'r'))
			/* propagate MQTT cached value to EIB */
			libt_add_timeouta(next_eib_timeslot(), my_eib_write, it);

	} else if (test_suffix(msg->topic, eibev_suffix)) {
		ev = topictoevent(msg->topic, eibev_suffix, msg->payloadlen);
		if (!ev || !msg->payloadlen) {
			if (ev)
				delete_event(ev);
			return;
		}
		if (ev->pattern)
			free(ev->pattern);
		ev->pattern = strdup(msg->payload);

	} else if (mqtt_write_suffix && test_suffix(msg->topic, mqtt_write_suffix)) {
		it = topictoitem(msg->topic, mqtt_write_suffix, 0);
		if (!it)
			return;

		if (!msg->retain && it->naddr && !item_option(it, 'r') && item_option(it, 't')) {
			/* only assign mvalue when about to transmit */
			it->mvalue = strtoul(msg->payload ?: "0", NULL, 0);
			/* forward non-local requests to EIB */
			libt_add_timeouta(next_eib_timeslot(), my_eib_write, it);
		}

	} else {
		/* find entry */
		it = topictoitem(msg->topic, NULL, 0);
		if (!it)
			return;

		it->mvalue = strtoul(msg->payload ?: "0", NULL, 0);
		/* schedule eib write */
		if (it->naddr && item_option(it, 't') && (item_option(it, 'r') || (!mqtt_write_suffix && !msg->retain)))
			libt_add_timeouta(next_eib_timeslot(), my_eib_write, it);
	}
}

/* EIB events */
static int eib_value(uint16_t hdr, const void *vdat, int len)
{
	int value;
	const uint8_t *dat = vdat;

	if (!len)
		return hdr & 0x3f;
	for (value = 0; len; --len, ++dat)
		value = (value << 8) + *dat;
	return value;
}

static void eib_msg(EIBConnection *eib, eibaddr_t src, eibaddr_t dst, uint16_t hdr,
		const void *vdat, int len)
{
	const uint8_t *dat = vdat;
	int cmd, ret, naddr, evalue;
	struct item *it;
	struct event *ev;
	char *dsttopic;
	static char sbuf[128];

	cmd = hdr & 0x03c0;
	switch (cmd) {
	case 0x0000:
		mylog(LOG_INFO, "eib:<%s %s", eibactions[cmd >> 6] ?: "?", eibgaddrtostr(dst));
		for (it = items; it; it = it->next) {
			if (!item_option(it, 'r'))
				continue;
			for (naddr = 0; naddr < it->naddr; ++naddr) {
				if (it->paddr[naddr] == dst) {
					libt_add_timeouta(next_eib_timeslot(), my_eib_response, it);
					break;
				}
			}
		}
		break;
	case 0x0040:
	case 0x0080:
		evalue = eib_value(hdr, dat, len);
		mylog(LOG_INFO, "eib:<%s %s %i", eibactions[cmd >> 6] ?: "?", eibgaddrtostr(dst), evalue);
		for (it = items; it; it = it->next) {
			if (it->naddr && it->paddr[0] == dst)
				/* remove pending request for this item */
				libt_remove_timeout(my_eib_request, it);

			for (naddr = 0; naddr < it->naddr; ++naddr) {
				if (it->paddr[naddr] != dst)
					continue;
				mylog(LOG_INFO, "%s matches %s:%i", eibgaddrtostr(dst), it->topic, naddr);
				/* process response */
				if (item_option(it, 'w')) {
					/* forward volatile or new or changed entries */
					sprintf(sbuf, "%i", evalue);
					/* push in MQTT, retain if not volatile */
					dsttopic = (item_option(it, 'r') && it->writetopic) ? it->writetopic : it->topic;
					mylog(LOG_INFO, "mqtt:>%s %s", dsttopic, sbuf);
					ret = mosquitto_publish(mosq, NULL, dsttopic, strlen(sbuf), sbuf, mqtt_qos, dsttopic == it->topic);
					if (ret)
						mylog(LOG_ERR, "mosquitto_publish %s '%s': %s", dsttopic, sbuf, mosquitto_strerror(ret));
				}
			}
		}
		if (cmd != 0x0080)
			return;
		/* emit events for matched patterns */
		sprintf(sbuf, "%s,%s,%i", eibphaddrtostr(src), eibgaddrtostr(dst), evalue);
		for (ev = events; ev; ev = ev->next) {
			if (fnmatch(ev->pattern, sbuf, 0))
				continue;
			ret = mosquitto_publish(mosq, NULL, ev->topic, strlen(sbuf), sbuf, mqtt_qos, 0);
			if (ret)
				mylog(LOG_ERR, "mosquitto_publish %s '%s': %s", ev->topic, sbuf, mosquitto_strerror(ret));
		}
		break;
	default:
		mylog(LOG_INFO, "eib:<%03x %s", cmd, eibgaddrtostr(dst));
		break;
	}
}

__attribute__((unused))
static void my_exit(void)
{
	if (mosq)
		mosquitto_disconnect(mosq);
	if (eib)
		EIBClose(eib);
}

static void test_config_seen(void *dat)
{
	if (!items && !events)
		mylog(LOG_WARNING, "no items or events have been configured, it seems");
}

int main(int argc, char *argv[])
{
	int opt, ret;
	struct pollfd pf[2] = {};
	char *str;
	eibaddr_t src, dst;
	int pkthdr;
	uint8_t buf[32];
	int logmask = LOG_UPTO(LOG_NOTICE);
	char **topics;

	/* argument parsing */
	while ((opt = getopt_long(argc, argv, optstring, long_opts, NULL)) >= 0)
	switch (opt) {
	case 'V':
		fprintf(stderr, "%s %s\nCompiled on %s %s\n",
				NAME, VERSION, __DATE__, __TIME__);
		exit(0);
	case '?':
		fputs(help_msg, stderr);
		exit(0);
	default:
		fprintf(stderr, "unknown option '%c'", opt);
		fputs(help_msg, stderr);
		exit(1);
		break;
	case 'v':
		switch (logmask) {
		case LOG_UPTO(LOG_NOTICE):
			logmask = LOG_UPTO(LOG_INFO);
			break;
		case LOG_UPTO(LOG_INFO):
			logmask = LOG_UPTO(LOG_DEBUG);
			break;
		}
		break;
	case 'e':
		eib_uri = optarg;
		break;
	case 'm':
		mqtt_host = optarg;
		str = strrchr(optarg, ':');
		if (str > mqtt_host && *(str-1) != ']') {
			/* TCP port provided */
			*str = 0;
			mqtt_port = strtoul(str+1, NULL, 10);
		}
		break;
	case 's':
		eib_suffix = optarg;
		break;
	case 'S':
		eibev_suffix = optarg;
		break;
	case 'w':
		mqtt_write_suffix = optarg;
		break;
	case 'f':
		default_options = optarg;
		break;
	}

	signal(SIGTERM, sighandler);
	signal(SIGINT, sighandler);
	//atexit(my_exit);

	openlog(NAME, LOG_PERROR, LOG_LOCAL2);
	setlogmask(logmask);
	/* MQTT start */
	mosquitto_lib_init();
	mosq = mosquitto_new(csprintf("eibd:%s #%i", eib_uri, getpid()), true, 0);
	if (!mosq)
		mylog(LOG_ERR, "mosquitto_new failed: %s", ESTR(errno));
	//mosquitto_will_set(mosq, mqtt_prefix, 0, NULL, mqtt_qos, 1);

	mosquitto_log_callback_set(mosq, my_mqtt_log);
	mosquitto_connect_callback_set(mosq, my_mqtt_connect);
	mosquitto_message_callback_set(mosq, my_mqtt_msg);

	ret = mosquitto_connect(mosq, mqtt_host, mqtt_port, mqtt_keepalive);
	if (ret)
		mylog(LOG_ERR, "mosquitto_connect %s:%i: %s", mqtt_host, mqtt_port, mosquitto_strerror(ret));

	/* subscribe to topics */
	topics = (optind >= argc) ? ((char *[]){ "#", NULL, }) : (argv+optind);
	for (; *topics; ++topics) {
		ret = mosquitto_subscribe(mosq, NULL, *topics, mqtt_qos);
		if (ret)
			mylog(LOG_ERR, "mosquitto_subscribe %s: %s", *topics, mosquitto_strerror(ret));
	}

	/* EIB start */
	eib = EIBSocketURL(eib_uri);
	if (!eib)
		mylog(LOG_ERR, "eib socket failed");
	ret = EIBOpen_GroupSocket(eib, 0);
	if (ret < 0)
		mylog(LOG_ERR, "EIB: open groupsocket failed");

	/* prepare poll */
	pf[0].fd = mosquitto_socket(mosq);
	pf[0].events = POLL_IN;
	pf[1].fd = EIB_Poll_FD(eib);
	pf[1].events = POLL_IN;

	/* run */
	libt_add_timeout(2, test_config_seen, NULL);
	while (!sigterm) {
		libt_flush();
		ret = libt_get_waittime();
		if ((ret > 1000) || (ret < 0))
			ret = 1000;
		ret = poll(pf, 2, ret);
		if (ret < 0 && errno == EINTR)
			continue;
		if (ret < 0)
			mylog(LOG_ERR, "poll ...");
		if (pf[0].revents) {
			/* read ... */
			ret = mosquitto_loop_read(mosq, 1);
			if (ret)
				mylog(LOG_ERR, "MQTT: read: %s", mosquitto_strerror(ret));
		}
		if (pf[1].revents) {
			ret = EIB_Poll_Complete(eib);
			if (ret < 0)
				mylog(LOG_ERR, "EIB: poll_complete");
			if (!ret)
				goto eibdone;
			/* received */
			ret = EIBGetGroup_Src(eib, sizeof (buf), buf, &src, &dst);
			if (ret < 0)
				mylog(LOG_ERR, "EIB: Get packet failed");
			if (ret < 2)
				/* packet too short */
				goto eibdone;
			pkthdr = (buf[0] << 8) + buf[1];
			eib_msg(eib, src, dst, pkthdr, buf+2, ret-2);
eibdone: ;
		}

		ret = mosquitto_loop_misc(mosq);
		if (ret)
			mylog(LOG_ERR, "MQTT: read: %s", mosquitto_strerror(ret));
		if (mosquitto_want_write(mosq)) {
			ret = mosquitto_loop_write(mosq, 1);
			if (ret)
				mylog(LOG_ERR, "MQTT: read: %s", mosquitto_strerror(ret));
		}
	}

	return 0;
}
