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
#include <poll.h>
#include <syslog.h>
#include <mosquitto.h>
#include <eibclient.h>

#include "lib/libt.h"

#define NAME "mqttnxd"
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
	"\n"
	" -o, --options=OPT[,OPT,...]	Specify Perform the protocol elements in the enumeration\n"
	"				Do not perform the elements that are prefixed with no-\n"
	"	 mqttcache	Use retained values from MQTT\n"
	"			Without this, retained msgs from MQTT are ignored, not propagated\n"
	"	 eibstate	Grab initial state from EIB/KNX\n"
	;

static char *const subopttable[] = {
	"mqttcache",
#define O_MQTTCACHE		(1 << 0)
	"eibstate",
#define O_EIBSTATE		(1 << 1)
	/* max 16 items, otherwise, modify FL_ macros */
	NULL,
};

static int options;

#ifdef _GNU_SOURCE
static struct option long_opts[] = {
	{ "help", no_argument, NULL, '?', },
	{ "version", no_argument, NULL, 'V', },
	{ "verbose", no_argument, NULL, 'v', },
	{ "options", required_argument, NULL, 'o', },

	{ "eib", required_argument, NULL, 'e', },
	{ "mqtt", required_argument, NULL, 'm', },
	{ "suffix", required_argument, NULL, 's', },

	{ },
};
#else
#define getopt_long(argc, argv, optstring, longopts, longindex) \
	getopt((argc), (argv), (optstring))
#endif
static const char optstring[] = "Vv?o:e:m:s:";

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
static char *eib_suffix = "/eib";
static int eib_suffixlen = 4;

/* EIB parameters */
static const char *eib_uri = "ip:localhost";

/* state */
static struct mosquitto *mosq;
static EIBConnection *eib;

/* item */
struct item {
	struct item *next;
	struct item *prev;

	int evalue; /* last value on EIB */
	int mvalue; /* last value on MQTT */
	int flags;
#define FL_EIB_SEEN		0x01
#define FL_ANSWER		0x02
#define FL_VOLATILE		0x04
#define FL_MQTT_SEEN		0x08
#define FL_PREF_EIB		0x10
#define FL_PREF_MQTT		0x20
	eibaddr_t addr;
	char *topic;
};
static struct item *eibtable[64*1024];
static struct item *items;

static const char *eibgaddrtostr(eibaddr_t val);

static inline void setresetflag(int *pflags, int mask, int set_nreset)
{
	if (set_nreset)
		*pflags |= mask;
	else
		*pflags &= ~mask;
}

static void add_item(struct item *it)
{
	/* insert in linked list */
	it->next = items;
	if (it->next) {
		it->prev = it->next->prev;
		it->next->prev = it;
	} else
		it->prev = (struct item *)(((char *)&items) - offsetof(struct item, next));
	it->prev->next = it;
}

static void delete_item(struct item *it)
{
	if (it->prev)
		it->prev->next = it->next;
	if (it->next)
		it->next->prev = it->prev;
	free(it->topic);
	free(it);
}

static struct item *topictoitem(const char *topic)
{
	struct item *it;

	for (it = items; it; it = it->next) {
		if (!strcmp(it->topic, topic))
			return it;
	}
	return NULL;
}

/* tools */
static const char *eibgaddrtostr(eibaddr_t val)
{
	static char buf[16];

	sprintf(buf, "%i/%i/%i", (val >> 11) & 0x1f, (val >> 8) & 0x07, val & 0xff);
	return buf;
}

static eibaddr_t strtoeibgaddr(const char *cstr, char **endp)
{
	char *str;
	eibaddr_t val = 0;

	val = (strtoul(cstr, &str, 10)  & 0x1f) << 11;
	if (*str == '/')
		val |= (strtoul(str+1, &str, 10)  & 0x7) << 8;
	if (*str == '/')
		val |= (strtoul(str+1, &str, 10)  & 0xff) << 0;
	if (endp)
		*endp = str;
	return val;
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

static inline void *compose_eib_param(eibaddr_t addr, int value, uint16_t sdat)
{
	return (void *)(long)(addr + ((value & 0x3f) << 16) + (((uint32_t)sdat & 0xffc0) << 16));
}

static const char *eibactions[16] = {
	[0] = "req",
	[1] = "resp",
	[2] = "write",
};
static void my_eib_send(void *dat)
{
	uint16_t sdat;
	uint8_t pkt[2];
	eibaddr_t dst;
	int value;
	const char *eibaction;

	dst = (long)dat & 0xffff;
	sdat = (long)dat >> 16;
	eibaction = eibactions[sdat >> 6] ?: "?";
	value = sdat & 0x3f;

	pkt[0] = sdat >> 8;
	pkt[1] = sdat;
	mylog(LOG_INFO, "eib:>%s %s %i\n", eibaction, eibgaddrtostr(dst), value);
	if (EIBSendGroup(eib, dst, sizeof(pkt), pkt) < 0)
		mylog(LOG_ERR, "EIB: %s %s %i failed", eibaction, eibgaddrtostr(dst), value);
}

static void my_mqtt_clear_cache(void *dat)
{
	eibaddr_t dst = (long)dat & 0xffff;
	struct item *it;

	it = eibtable[dst];
	mylog(LOG_INFO, "mqtt:clear %s\n", it->topic);
	/* try to publish an empty message.
	 * This clear the MQTT cache.
	 * The result is ignored here
	 * RETAIN is always set, and effectively clears the retained cache
	 */
	mosquitto_publish(mosq, NULL, it->topic, 0, NULL, mqtt_qos, 1);
}

static void my_eib_send_or_clear_cache(void *dat)
{
	my_eib_send(dat);
	libt_add_timeout(1, my_mqtt_clear_cache, dat);
}

static void my_mqtt_msg(struct mosquitto *mosq, void *dat, const struct mosquitto_message *msg)
{
	struct item *it;
	int len;

	mylog(LOG_INFO, "mqtt:<%s %s", msg->topic, (char *)msg->payload ?: "<null>");
	len = strlen(msg->topic);
	if (len > eib_suffixlen && !strcmp(msg->topic + len - eib_suffixlen, eib_suffix)) {
		/* this is an EIB config parameter */
		char *topic = strdup(msg->topic);
		char *str;
		eibaddr_t addr;

		topic[len - eib_suffixlen] = 0;
		it = topictoitem(topic);

		if (!msg->payload) {
			free(topic);
			if (it) {
				if (eibtable[it->addr] == it)
					eibtable[it->addr] = NULL;
				delete_item(it);
			}
			return;
		}
		/* parse eibaddr */
		addr = strtoeibgaddr(msg->payload, &str);
		/* flush info */
		if (!it) {
			it = malloc(sizeof(*it));
			memset(it, 0, sizeof(*it));
			add_item(it);
			it->topic = topic;
			it->addr = addr;
			eibtable[addr] = it;
		} else {
			free(topic);
			/* change addr ? */
			if (addr != it->addr) {
				if (eibtable[it->addr] == it)
					eibtable[it->addr] = NULL;
				/* clear pending requests for old addr ... */
				libt_remove_timeout(my_mqtt_clear_cache, compose_eib_param(it->addr, 0, 0x0000));
				libt_remove_timeout(my_eib_send_or_clear_cache, compose_eib_param(it->addr, 0, 0x0000));
				it->flags &= ~FL_EIB_SEEN;
				it->addr = addr;
				/* process new addr */
				if (eibtable[addr]) {
					mylog(LOG_WARNING, "%s = %s: eib addr is occupied by %s",
							msg->topic, (char *)msg->payload,
							eibtable[addr]->topic);
					/* we're about to kick the previous entry
					 * first copy the EIB cache :-)
					 */
					it->flags |= eibtable[addr]->flags & FL_EIB_SEEN;
					it->evalue = eibtable[addr]->evalue;
				}
				eibtable[addr] = it;
			}
		}
		/* parse flags */
		setresetflag(&it->flags, FL_VOLATILE, !!strchr(str, 'v'));
		setresetflag(&it->flags, FL_ANSWER, !!strchr(str, 'a'));
		setresetflag(&it->flags, FL_PREF_EIB, strchr(str, 'e') || (!(it->flags & FL_VOLATILE) && (options & O_EIBSTATE)));
		setresetflag(&it->flags, FL_PREF_MQTT, strchr(str, 'm') || (!(it->flags & FL_VOLATILE) && (options & O_MQTTCACHE)));
		if ((it->flags & (FL_VOLATILE | FL_PREF_EIB | FL_PREF_MQTT)) == FL_VOLATILE)
			mylog(LOG_WARNING, "%s: configured volatile with no preferred source!", it->topic);

		/* refresh cache */
		if ((it->flags & (FL_PREF_EIB | FL_EIB_SEEN | FL_VOLATILE)) == FL_PREF_EIB)
			/* schedule eib request */
			libt_add_timeouta(next_eib_timeslot(), my_eib_send_or_clear_cache, compose_eib_param(it->addr, 0, 0x0000));
		if ((it->flags & (FL_PREF_MQTT | FL_MQTT_SEEN | FL_VOLATILE)) == (FL_PREF_MQTT | FL_MQTT_SEEN))
			/* propagate MQTT cached value to EIB */
			libt_add_timeouta(next_eib_timeslot(), my_eib_send, compose_eib_param(it->addr, it->mvalue, 0x0080));
		return;
	}
	/* find entry */
	it = topictoitem(msg->topic);
	if (!it)
		return;
	it->mvalue = strtoul(msg->payload ?: "0", NULL, 0);

	if ((it->flags & FL_VOLATILE) && !(it->flags & FL_PREF_MQTT))
		/* volatile message configured to flow from KNX to MQTT */
		goto blocked;
	if (!(it->flags & FL_VOLATILE) && !(it->flags & FL_PREF_MQTT) && msg->retain)
		/* ignore this MQTT retained msg, cache is not taken from MQTT */
		goto blocked;
	if (!(it->flags & FL_VOLATILE) && (it->flags & FL_MQTT_SEEN) && (it->evalue == it->mvalue))
		/* skip identical messages */
		goto blocked;
	/* schedule eib write */
	libt_add_timeouta(next_eib_timeslot(), my_eib_send, compose_eib_param(it->addr, it->mvalue, 0x0080));
blocked:
	it->flags |= FL_MQTT_SEEN;
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
	int cmd, ret;
	struct item *it;
	static char sbuf[32*2+1];

	it = eibtable[dst];
	if (!it)
		return;

	cmd = hdr & 0x03c0;
	switch (cmd) {
	case 0:
		/* read */
		mylog(LOG_INFO, "eib:<%s %s", eibactions[cmd >> 6] ?: "?", eibgaddrtostr(dst));
		if (it->flags & FL_ANSWER)
			libt_add_timeouta(next_eib_timeslot(), my_eib_send, compose_eib_param(dst, it->mvalue, 0x0040));
		break;
	case 0x0040:
	case 0x0080:
		/* response */
		libt_remove_timeout(my_mqtt_clear_cache, compose_eib_param(dst, 0, 0x0000));
		it->evalue = eib_value(hdr, dat, len);
		mylog(LOG_INFO, "eib:<%s %s %i", eibactions[cmd >> 6] ?: "?", eibgaddrtostr(dst), it->evalue);
		if (((it->flags & FL_VOLATILE) && (it->flags & FL_PREF_EIB)) ||
				(!(it->flags & FL_VOLATILE) && (!(it->flags & FL_EIB_SEEN) || (it->evalue != it->mvalue)))) {
			/* forward volatile or new or changed entries */
			sprintf(sbuf, "%i", it->evalue);
			/* push in MQTT, retain if not volatile */
			mylog(LOG_INFO, "mqtt:>%s %s", it->topic, sbuf);
			ret = mosquitto_publish(mosq, NULL, it->topic, strlen(sbuf), sbuf, mqtt_qos, !(it->flags & FL_VOLATILE));
			if (ret)
				mylog(LOG_ERR, "mosquitto_publish %s '%s': %s", it->topic, sbuf, mosquitto_strerror(ret));
		}
		it->flags |= FL_EIB_SEEN;
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
	if (!items)
		mylog(LOG_WARNING, "no items have been configured, it seems");
}

int main(int argc, char *argv[])
{
	int opt, ret, not;
	struct pollfd pf[2] = {};
	char *str, *subopts;
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
		eib_suffixlen = strlen(eib_suffix);
		break;
	case 'o':
		subopts = optarg;
		while (*subopts) {
			not = !strncmp(subopts, "no-", 3);
			if (not)
				subopts += 3;
			opt = getsubopt(&subopts, subopttable, &optarg);
			if (opt < 0)
				break;
			setresetflag(&options, 1 << opt, !not);
		}
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
