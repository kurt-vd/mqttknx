#include <errno.h>
#include <math.h>
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
#include <syslog.h>
#include <mosquitto.h>
#include <eibclient.h>

#include "lib/libt.h"
#include "lib/libe.h"

#define NAME "mqttknxd"
#ifndef VERSION
#define VERSION "<undefined version>"
#endif

/* generic error logging */
static int max_loglevel = LOG_NOTICE;
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
	" -w, --write=STR	Give MQTT topic suffix for writing the topic (default /set)\n"
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
	"			x	1st eib addr is the set request, 2nd is the \n"
	" -t, --delay=TIME	delay between sent packets (default 0.1s)\n"
	"MQTT topics\n"
	" cfg/"NAME"/\n"
	"	loglevel	(0..7) modify loglevel\n"
	"	delay		SEC, set delay between EIB packets\n"
	"	repeat		NUMBER, number of repeats of MQTT-owned items\n"
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
	{ "delay", required_argument, NULL, 't', },

	{ },
};
#else
#define getopt_long(argc, argv, optstring, longopts, longindex) \
	getopt((argc), (argv), (optstring))
#endif
static const char optstring[] = "Vv?f:e:m:s:S:w:t:";

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

/* return string with trailing decimal 0's stripped */
__attribute__((unused))
static const char *mydtostr(double d)
{
	static char buf[64];
	char *str;
	int ptpresent = 0;

	sprintf(buf, "%lg", d);
	for (str = buf; *str; ++str) {
		if (*str == '.')
			ptpresent = 1;
		else if (*str == 'e')
			/* nothing to do anymore */
			break;
		else if (ptpresent && *str == '0') {
			int len = strspn(str, "0");
			if (!str[len]) {
				/* entire string (after .) is '0' */
				*str = 0;
				if (str > buf && *(str-1) == '.')
					/* remote '.' too */
					*(str-1) = 0;
				break;
			}
		}
	}
	return buf;
}

/* MQTT parameters */
static const char *mqtt_host = "localhost";
static int mqtt_port = 1883;
static int mqtt_keepalive = 10;
static int mqtt_qos = 2;
static const char *mqtt_write_suffix = "/set";
static char *eib_suffix = "/eib";
static char *eibev_suffix = "/eibevent";
static char *default_options = "wt";
static double pktdelay = 0.1;
static int max_repeats = 3;

/* EIB parameters */
static const char *eib_uri = "ip:localhost";

/* dimmer steps for 4bit EIB relative dimming */
static const double eib_dimsteps[8] = { 0, 1.0/64, 1.0/32, 1.0/16, 1.0/8, 1.0/4, 1.0/2, 1.0/1 };

/* state */
static struct mosquitto *mosq;
static EIBConnection *eib;

/* item */
struct item {
	struct item *next;
	struct item *prev;

	double mvalue; /* last value on MQTT */
	double mtvalue; /* my last sent value on MQTT */
	int evalue; /* last value on EIB */
	int etvalue; /* my last sent value on EIB */
	int eqvalue; /* queued value for EIB write */
	double mul, off; /* multiplier & offset from MQTT values */
	int flags;
#define MQTT_PUBLISHED	(1 << 0)
#define EIB_PUBLISHED	(1 << 1)
	double changed; /* libt_now() of last change, used for repeating */
	int repeats; /* number of transmissions */
	int eibnbits;
	eibaddr_t *paddr;
	int naddr;
	int saddr;
	eibaddr_t dimaddr;
	int eibdimval; /* current pending dimming value, used in both directions */
	int eibdimcnt; /* count repeated dimmer commands, stop after 15sec or so */
	char *options;
	char *topic;
	int topiclen;
	char *writetopic;
	char *dimtopic;
};
static struct item *items;

struct event {
	struct event *next;
	struct event *prev;

	char *topic;
	char *pattern;
};
static struct event *events;

struct addrsock {
	struct addrsock *next;
	struct addrsock *prev;

	eibaddr_t addr;
	int cnt;
	EIBConnection *eib;
};
static struct addrsock *localgrps;

static const char *eibgaddrtostr(eibaddr_t val);
static void my_eib_write(void *dat);
static void my_eib_write_repeat(void *dat);
static void my_eib_response(void *dat);
static void my_eib_request(void *dat);
static void register_local_grp(eibaddr_t val);
static void unregister_local_grp(eibaddr_t val);
static void recvd_eib_grp(int fd, void *dat);
static void my_eib_send_dim(void *dat);
static void my_eib_repeat_dim(void *dat);
static void my_mqtt_repeat_dim(void *dat);
static void mqtt_sub(const char *sub, int opts);
static void mqtt_unsub(const char *sub, int opts);

static inline int mqtttoeib(double value, struct item *it)
{
	return lround((value*it->mul)+it->off);
}
static inline double eibtomqtt(int value, struct item *it)
{
	return (value-it->off)/it->mul;
}
static inline int dblcmp(double a, double b, double diff)
{
	if (isnan(a) && isnan(b))
		return 0;
	else if (isnan(a))
	       return -1;
	else if (isnan(b))
	       return 1;

	else if (fpclassify(a) == FP_ZERO && fpclassify(b) == FP_ZERO)
		/* avoid /0 */
		return 0;
	else if (fabs(2*(a-b)/(a+b)) < diff)
		return 0;
	else if (a < b)
		return -1;
	else
		return 1;
}

static inline int item_option(struct item *it, int c)
{
	return !!strchr(it->options ?: default_options, c);
}

static inline int mqtt_owned(struct item *it)
{
	return item_option(it, 'r');
}
static inline int eib_owned(struct item *it)
{
	return !mqtt_owned(it);
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
	libt_remove_timeout(my_eib_write_repeat, it);
	libt_remove_timeout(my_eib_response, it);
	libt_remove_timeout(my_eib_request, it);
	libt_remove_timeout(my_eib_send_dim, it);
	libt_remove_timeout(my_eib_repeat_dim, it);
	libt_remove_timeout(my_mqtt_repeat_dim, it);
	if (it->prev)
		it->prev->next = it->next;
	if (it->next)
		it->next->prev = it->prev;
	if (it->paddr)
		free(it->paddr);
	free(it->topic);
	if (it->writetopic)
		free(it->writetopic);
	if (it->dimtopic)
		free(it->dimtopic);
	free(it);
}

static struct item *topictoitem(const char *topic, const char *suffix, int create)
{
	struct item *it;
	int len;

	len = strlen(topic ?: "") - strlen(suffix ?: "");
	if (len < 0)
		return NULL;
	/* match suffix */
	if (strcmp(topic+len, suffix ?: ""))
		return NULL;
	/* match base topic */
	for (it = items; it; it = it->next) {
		if ((it->topiclen == len) && !strncmp(it->topic ?: "", topic, len))
			return it;
	}
	if (!create)
		return NULL;

	it = malloc(sizeof(*it));
	memset(it, 0, sizeof(*it));
	it->mul = 1;
	it->off = 0;
	it->topic = strdup(topic);
	it->topic[len] = 0;
	it->topiclen = len;
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
#if 0
		MOSQ_LOG_NOTICE, LOG_NOTICE,
		MOSQ_LOG_INFO, LOG_INFO,
		MOSQ_LOG_DEBUG, LOG_DEBUG,
#endif
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
	static double filled_eib_slot = 0;
	double tnow = libt_now();

	if ((filled_eib_slot + pktdelay) < tnow)
		filled_eib_slot = tnow;
	else
		filled_eib_slot += pktdelay;
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
	int idx;

	/* select response addr */
	idx = (item_option(it, 'x') && eib_owned(it)) ? 1 : 0;

	if (idx >= it->naddr)
		return;
	my_eib_send(it->paddr[idx], 0x0080, it->eqvalue, it->eibnbits);
	it->etvalue = it->eqvalue;
	it->flags |= EIB_PUBLISHED;

	if (mqtt_owned(it) && item_option(it, 't') && (item_option(it, 'x') || !item_option(it, 'w'))) {
		/* repeat transmission regularly */
		double now = libt_now();
		double delay;

		if ((now - it->changed) < 65)
			delay = 10;
		else if ((now - it->changed) < 330)
			delay = 60;
		else
			delay = 300;

		++it->repeats;
		if (max_repeats < 0 || it->repeats < max_repeats)
			libt_add_timeout(delay, my_eib_write_repeat, it);
	}
}

static void my_eib_write_repeat(void *dat)
{
	struct item *it = dat;

	libt_add_timeouta(next_eib_timeslot(), my_eib_write, it);
}

static void my_eib_response(void *dat)
{
	struct item *it = dat;

	if (it->naddr) {
		my_eib_send(it->paddr[0], 0x0040, it->etvalue, it->eibnbits);
		it->etvalue = mqtttoeib(it->mvalue, it);
		it->flags |= EIB_PUBLISHED;
	}
}

static void my_eib_request(void *dat)
{
	struct item *it = dat;

	if (it->naddr)
		my_eib_send(it->paddr[0], 0x0000, 0, 0);
}

static void my_eib_send_dim(void *dat)
{
	struct item *it = dat;

	my_eib_send(it->dimaddr, 0x0080, it->eibdimval, 4);
}

static void my_eib_repeat_dim(void *dat)
{
	struct item *it = dat;

	libt_add_timeouta(next_eib_timeslot(), my_eib_send_dim, it);
	if (it->eibdimval) {
		if (it->eibdimcnt++ > 30) {
			mylog(LOG_NOTICE, "eib repeat dim: stop after %i iterations", it->eibdimcnt);
			return;
		}
		/* schedule repeat when dimval != 0 */
		libt_add_timeout(0.5, my_eib_repeat_dim, it);
	}
}

/* dim one step */
static void my_mqtt_dim(struct item *it, int eibdimval)
{
	int ret;
	char buf[32];

	if (eibdimval) {
		int dir = ((eibdimval >> 3) & 1) ? 1 : -1; /* turn 0/1 into -1/+1 */
		int stp = (eibdimval >> 0) & 7;

		sprintf(buf, "%+.3f", eib_dimsteps[stp]*dir);
	} else {
		strcpy(buf, "0");
	}

	if (!it->dimtopic)
		asprintf(&it->dimtopic, "%s/dim", it->topic);

	ret = mosquitto_publish(mosq, NULL, it->dimtopic, strlen(buf), buf, mqtt_qos, 0);
	if (ret)
		mylog(LOG_ERR, "mosquitto_publish %s '%s': %s", it->dimtopic, buf, mosquitto_strerror(ret));
}

/* locally repeat dimmer until 0 */
static void my_mqtt_repeat_dim(void *dat)
{
	struct item *it = dat;

	my_mqtt_dim(it, it->eibdimval);
	if (it->eibdimcnt++ > 30) {
		mylog(LOG_NOTICE, "eib repeat dim: stop after %i iterations", it->eibdimcnt);
		return;
	}
	libt_add_timeout(0.5, my_mqtt_repeat_dim, it);
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

static int test_prefix(const char *topic, const char *prefix, char **result)
{
	int len;
	int ret;

	len = strlen(prefix);
	ret = strncmp(topic, prefix, len);
	if (!ret && result)
		*result = (char *)topic + len;
	return !ret;
}

static void my_mqtt_msg(struct mosquitto *mosq, void *dat, const struct mosquitto_message *msg)
{
	struct item *it;
	struct event *ev;
	char *endp;
	char *topic;

	mylog(LOG_DEBUG, "mqtt:<%s %s", msg->topic, (char *)msg->payload ?: "<null>");
	if (test_prefix(msg->topic, "cfg/"NAME"/", &topic)) {
		if (!strcmp(topic, "loglevel")) {
			max_loglevel = strtol((char *)msg->payload ?: "", NULL, 0);
			setlogmask(LOG_UPTO(max_loglevel));
			mylog(LOG_NOTICE, "changed verbose %i", max_loglevel);

		} else if (!strcmp(topic, "delay")) {
			pktdelay = strtod((char *)msg->payload ?: "", NULL);
			mylog(LOG_NOTICE, "changed #delay %.2f", pktdelay);

		} else if (!strcmp(topic, "repeat")) {
			max_repeats = strtoul((char *)msg->payload ?: "", NULL, 0);
			mylog(LOG_NOTICE, "changed #repeat %i", max_repeats);
		}
	} else if (test_suffix(msg->topic, eib_suffix)) {
		/* this is an EIB config parameter */
		char *str, *tok;
		eibaddr_t addr;
		int j;

		it = topictoitem(msg->topic, eib_suffix, msg->payloadlen);
		if (it) {
			for (j = 0; j < it->naddr; ++j)
				unregister_local_grp(it->paddr[j]);
			if (it->dimaddr) {
				if (mqtt_owned(it)) {
					unregister_local_grp(it->dimaddr);
				} else {
					mqtt_unsub(it->dimtopic, 0);
				}
			}
			mqtt_sub((eib_owned(it) && it->writetopic) ? it->writetopic : it->topic, 0);
		}
		if (!it || !msg->payloadlen) {
			if (it)
				delete_item(it);
			return;
		}
		/* parse eibaddr */
		it->naddr = 0;
		it->dimaddr = 0;
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

		for (tok = strtok(NULL, " \t"); tok; tok = strtok(NULL, " \t")) {
			str = strchr(tok, '=');
			if (str)
				*str++ = 0;
			if (!strcmp(tok, "mul"))
				it->mul = strtod(str, NULL);
			else if (!strcmp(tok, "div"))
				it->mul = 1/strtod(str, NULL);
			else if (!strcmp(tok, "offset"))
				it->off = strtod(str, NULL);
			else if (!strcmp(tok, "dim"))
				it->dimaddr = strtoeibgaddr(str, &endp);
			else
				mylog(LOG_WARNING, "%s: unknown option %s", it->topic, tok);
		}

		/* determine eib payload size */
		it->eibnbits = strtoul(strpbrk(it->options ?: "", "0123456789") ?: "1", &str, 10);
		if (str && *str == 'B')
			it->eibnbits *= 8;

		if (!item_option(it, 'w')) {
			/* don't respond */
		} else if (item_option(it, 'x') && eib_owned(it)) {
			register_local_grp(it->paddr[0]);
		} else if (item_option(it, 'x') && mqtt_owned(it)) {
			for (j = 1; j < it->naddr; ++j)
				register_local_grp(it->paddr[j]);
		} else for (j = 0; j < it->naddr; ++j) {
			register_local_grp(it->paddr[j]);
		}
		mqtt_sub((eib_owned(it) && it->writetopic) ? it->writetopic : it->topic, 0);

		/* relative DIM */
		if (it->dimaddr) {
			if (!it->dimtopic)
				asprintf(&it->dimtopic, "%s/dim", it->topic);
			if (mqtt_owned(it)) {
				register_local_grp(it->dimaddr);
			} else {
				mqtt_sub(it->dimtopic, 0);
			}
		} else {
			if (it->dimtopic)
				free(it->dimtopic);
			it->dimtopic = NULL;
		}

		/* refresh cache */
		if (it->naddr && eib_owned(it) && item_option(it, 'w'))
			/* schedule eib request */
			libt_add_timeouta(next_eib_timeslot(), my_eib_request, it);
		if (it->naddr && item_option(it, 't') && mqtt_owned(it)) {
			/* propagate MQTT cached value to EIB */
			it->eqvalue = mqtttoeib(it->mvalue, it);
			it->changed = libt_now();
			it->repeats = 0;
			libt_add_timeouta(next_eib_timeslot(), my_eib_write, it);
		}
		/* clear flags that may be wrong due to a changed addr */
		it->flags &= ~EIB_PUBLISHED;

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
		if (msg->retain)
			/* ignore retained request */
			return;
		it = topictoitem(msg->topic, mqtt_write_suffix, 0);
		if (!it)
			return;
		if (it->naddr && eib_owned(it)) {
			/* only assign mvalue when about to transmit */
			it->mvalue = strtod(msg->payload ?: "0", NULL);
			/* forward non-local requests to EIB */
			it->eqvalue = mqtttoeib(it->mvalue, it);
			it->changed = libt_now();
			it->repeats = 0;
			libt_add_timeouta(next_eib_timeslot(), my_eib_write, it);
		}

	} else if (test_suffix(msg->topic, "/dim")) {
		if (msg->retain)
			/* ignore retained request */
			return;
		it = topictoitem(msg->topic, "/dim", 0);
		if (!it)
			return;
		if (it->dimaddr && eib_owned(it)) {
			double value = strtod(msg->payload ?: "0", NULL);
			int eibval;
			int j;

			for (j = 0; j < 7; ++j) {
				if (fabs(value) < (eib_dimsteps[j]+eib_dimsteps[j+1])/2)
					break;
			}
			eibval = j;
			/* the test '< 0' is a bit dangerous close to 0,
			 * but close to 0, j would also be 0
			 */
			if (j && value > 0)
				eibval |= 0x8;
			/* remember dimming value */
			it->eibdimval = eibval;
			it->eibdimcnt = 0;
			libt_add_timeout(0, my_eib_repeat_dim, it);
		}

	} else {
		/* find entry */
		it = topictoitem(msg->topic, NULL, 0);
		if (!it)
			return;

		it->mvalue = strtod(msg->payload ?: "0", NULL);
		if (!msg->retain && !dblcmp(it->mtvalue, it->mvalue, 1e-3) && (it->flags & MQTT_PUBLISHED)) {
			it->flags &= ~MQTT_PUBLISHED;
			/* avoid loops here, drop my echo */
			return;
		}

		if (eib_owned(it) && (it->writetopic || msg->retain))
			/* ignore values for EIB items when expecting requests */
			/* ignore retained values for EIB items */
			return;

		/* schedule eib write */
		if (it->naddr && item_option(it, 't')) {
			it->eqvalue = mqtttoeib(it->mvalue, it);
			it->changed = libt_now();
			it->repeats = 0;
			libt_add_timeouta(next_eib_timeslot(), my_eib_write, it);
		}
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
static const char *eib_str_value(const void *vdat, int len)
{
	static char buf[256];

	memcpy(buf, vdat, len);
	buf[len] = 0;
	return buf;
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
	const char *svalue;

	cmd = hdr & 0x03c0;
	switch (cmd) {
	case 0x0000:
		mylog(LOG_INFO, "eib:<%s %s", eibactions[cmd >> 6] ?: "?", eibgaddrtostr(dst));
		for (it = items; it; it = it->next) {
			if (eib_owned(it))
				/* we provide no response for */
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
		if (len > 4) {
			svalue = eib_str_value(dat, len);
			mylog(LOG_INFO, "eib:<%s %s '%s'", eibactions[cmd >> 6] ?: "?", eibgaddrtostr(dst), svalue);
		} else {
			mylog(LOG_INFO, "eib:<%s %s %i", eibactions[cmd >> 6] ?: "?", eibgaddrtostr(dst), evalue);
			svalue = "";
		}
		for (it = items; it; it = it->next) {
			if (it->naddr && it->paddr[0] == dst)
				/* remove pending request for this item */
				libt_remove_timeout(my_eib_request, it);

			if (mqtt_owned(it) && it->dimaddr && dst == it->dimaddr) {
				int dimval = evalue & 0xf;

				if ((dimval & 0x7) == 1) {
					/* dim in steps of 1%, use local repeat dimming
					 * in 8 steps
					 */
					it->eibdimval = (dimval & 0x8) | 0x4;
					it->eibdimcnt = 0;
					libt_add_timeout(0, my_mqtt_repeat_dim, it);

				} else {
					/* propagate directly, with no repeater */
					libt_remove_timeout(my_mqtt_repeat_dim, it);
					my_mqtt_dim(it, dimval);
				}
			}
			for (naddr = 0; naddr < it->naddr; ++naddr) {
				if (it->paddr[naddr] != dst)
					continue;
				if (item_option(it, 'x')) {
					/* req/resp item ... */
					if (naddr > 0 && eib_owned(it)) {
						/* ignore set request for eib-owned items
						 * we expect a response from eib soon
						 */
						break;
					} else if (naddr == 0 && mqtt_owned(it)) {
						/* ignore status report for mqtt items */
						break;
					}
				}
				it->evalue = evalue;
				mylog(LOG_INFO, "%s matches %s:%i", eibgaddrtostr(dst), it->topic, naddr);
				if (it->etvalue == evalue && (it->flags & EIB_PUBLISHED) && naddr == 0) {
					it->flags &= ~EIB_PUBLISHED;
					if (!it->writetopic || mqtt_owned(it))
						/* avoid loops here, drop my echo */
						break;
				}
				/* process response */
				if (item_option(it, 'w')) {
					double devalue = eibtomqtt(evalue, it);
					/* forward volatile or new or changed entries */
					const char *vstr = (len <= 4) ? mydtostr(devalue) : svalue;
					/* push in MQTT, retain if not volatile */
					dsttopic = (mqtt_owned(it) && it->writetopic) ? it->writetopic : it->topic;
					mylog(LOG_INFO, "mqtt:>%s %s", dsttopic, vstr);
					ret = mosquitto_publish(mosq, NULL, dsttopic, strlen(vstr), vstr, mqtt_qos, dsttopic == it->topic);
					if (ret)
						mylog(LOG_ERR, "mosquitto_publish %s '%s': %s", dsttopic, vstr, mosquitto_strerror(ret));
					else if (!item_option(it, 'x')) {
						it->flags |= MQTT_PUBLISHED;
						it->mtvalue = devalue;
					}
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

/* local group address management */
static struct addrsock *find_addrsock(eibaddr_t val, struct addrsock *list)
{
	for (; list; list = list->next) {
		if (val == list->addr)
			break;
	}
	return list;
}

static void register_local_grp(eibaddr_t val)
{
	struct addrsock *as;
	int ret;

	as = find_addrsock(val, localgrps);
	if (as) {
		as->cnt += 1;
		mylog(LOG_NOTICE, "inc local EIB groupaddr %s: %i", eibgaddrtostr(val), as->cnt);
		return;
	}
	/* create new entry */
	as = malloc(sizeof(*as));
	memset(as, 0, sizeof(*as));
	as->addr = val;
	as->cnt = 1;

	/* insert in linked list */
	as->next = localgrps;
	if (as->next) {
		as->prev = as->next->prev;
		as->next->prev = as;
	} else
		as->prev = (struct addrsock *)(((char *)&localgrps) - offsetof(struct addrsock, next));
	as->prev->next = as;

	as->eib = EIBSocketURL(eib_uri);
	if (!as->eib)
		mylog(LOG_ERR, "eib socket failed");
	ret = EIBOpenT_Group(as->eib, val, 0);
	if (ret < 0)
		mylog(LOG_ERR, "EIB: open group failed");

	mylog(LOG_NOTICE, "add local EIB groupaddr %s", eibgaddrtostr(val));
	libe_add_fd(EIB_Poll_FD(as->eib), recvd_eib_grp, as->eib);
}

static void unregister_local_grp(eibaddr_t val)
{
	struct addrsock *as;

	as = find_addrsock(val, localgrps);
	if (!as)
		return;
	as->cnt -= 1;
	if (as->cnt > 0) {
		mylog(LOG_NOTICE, "dec local EIB groupaddr %s: %i", eibgaddrtostr(val), as->cnt);
		return;
	}
	mylog(LOG_NOTICE, "del local EIB groupaddr %s", eibgaddrtostr(val));
	/* cleanup resources */
	if (as->prev)
		as->prev->next = as->next;
	if (as->next)
		as->next->prev = as->prev;
	if (as->eib)
		EIBClose(as->eib);
	free(as);
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

/* epoll handlers */
static void mqtt_maintenance(void *dat)
{
	int ret;
	struct mosquitto *mosq = dat;

	ret = mosquitto_loop_misc(mosq);
	if (ret)
		mylog(LOG_ERR, "mosquitto_loop_misc: %s", mosquitto_strerror(ret));
	libt_add_timeout(2.3, mqtt_maintenance, dat);
}

static void recvd_mosq(int fd, void *dat)
{
	struct mosquitto *mosq = dat;
	int evs = libe_fd_evs(fd);
	int ret;

	if (evs & LIBE_RD) {
		/* mqtt read ... */
		ret = mosquitto_loop_read(mosq, 1);
		if (ret)
			mylog(LOG_ERR, "mosquitto_loop_read: %s", mosquitto_strerror(ret));
	}
	if (evs & LIBE_WR) {
		/* flush mqtt write queue _after_ the timers have run */
		ret = mosquitto_loop_write(mosq, 1);
		if (ret)
			mylog(LOG_ERR, "mosquitto_loop_write: %s", mosquitto_strerror(ret));
	}
}

static void mosq_update_flags(void)
{
	if (mosq)
		libe_mod_fd(mosquitto_socket(mosq), LIBE_RD | (mosquitto_want_write(mosq) ? LIBE_WR : 0));
}

static uint8_t buf[256];
static void recvd_eib(int fd, void *dat)
{
	EIBConnection *eib = dat;
	eibaddr_t src, dst;
	int ret, pkthdr;

	ret = EIB_Poll_Complete(eib);
	if (ret < 0)
		mylog(LOG_ERR, "EIB: poll_complete");
	if (!ret)
		return;
	/* received */
	ret = EIBGetGroup_Src(eib, sizeof (buf), buf, &src, &dst);
	if (ret < 0)
		mylog(LOG_ERR, "EIB: Get packet failed");
	if (ret < 2)
		/* packet too short */
		return;

	pkthdr = (buf[0] << 8) + buf[1];
	eib_msg(eib, src, dst, pkthdr, buf+2, ret-2);
}

static void recvd_eib_grp(int fd, void *dat)
{
	EIBConnection *eib = dat;
	eibaddr_t src;
	int ret;

	ret = EIB_Poll_Complete(eib);
	if (ret < 0)
		mylog(LOG_ERR, "EIB: poll_complete");
	if (!ret)
		return;
	/* received: read and forget
	 * This is only to tell eibd to ack this group addr */
	EIBGetAPDU_Src(eib, sizeof (buf), buf, &src);
}

static void mqtt_sub(const char *sub, int opts)
{
	int ret;

	ret = mosquitto_subscribe(mosq, NULL, sub, mqtt_qos);
	if (ret)
		mylog(LOG_ERR, "mosquitto_subscribe %s: %s", sub, mosquitto_strerror(ret));
}

__attribute__((unused))
static void mqtt_unsub(const char *sub, int opts)
{
	int ret;

	ret = mosquitto_unsubscribe(mosq, NULL, sub);
	if (ret)
		mylog(LOG_ERR, "mosquitto_unsubscribe %s: %s", sub, mosquitto_strerror(ret));
}

int main(int argc, char *argv[])
{
	int opt, ret;
	char *str;
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
		++max_loglevel;
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
		if (!*mqtt_write_suffix)
			mqtt_write_suffix = NULL;
		break;
	case 'f':
		default_options = optarg;
		break;
	case 't':
		pktdelay = strtod(optarg, NULL);
		break;
	}

	signal(SIGTERM, sighandler);
	signal(SIGINT, sighandler);
	//atexit(my_exit);

	openlog(NAME, LOG_PERROR, LOG_LOCAL2);
	setlogmask(LOG_UPTO(max_loglevel));
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
	if (optind >= argc) {
		mqtt_sub("#", 0);
	} else {
		mqtt_sub("cfg/"NAME"/#", 0);
		for (topics = argv+optind; *topics; ++topics)
			mqtt_sub(*topics, 0);
	}
	libt_add_timeout(0, mqtt_maintenance, mosq);
	libe_add_fd(mosquitto_socket(mosq), recvd_mosq, mosq);

	/* EIB start */
	eib = EIBSocketURL(eib_uri);
	if (!eib)
		mylog(LOG_ERR, "eib socket failed");
	ret = EIBOpen_GroupSocket(eib, 0);
	if (ret < 0)
		mylog(LOG_ERR, "EIB: open groupsocket failed");
	libe_add_fd(EIB_Poll_FD(eib), recvd_eib, eib);

	/* run */
	libt_add_timeout(2, test_config_seen, NULL);
	while (!sigterm) {
		libt_flush();
		mosq_update_flags();
		ret = libe_wait(libt_get_waittime());
		if (ret >= 0)
			libe_flush();
	}

	return 0;
}
