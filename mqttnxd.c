#include <errno.h>
#include <signal.h>
#include <stdarg.h>
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
	"usage:	" NAME " [OPTIONS ...]\n"
	"\n"
	"Options\n"
	" -V, --version		Show version\n"
	" -v, --verbose		Be more verbose\n"
	" -e, --eib=URI		Specify alternate EIB uri\n"
	"			Like ip:xxx or usb: or ...\n"
	" -m, --mqtt=HOST[:PORT]Specify alternate MQTT host+port\n"
	" -p, --prefix=STR	Give MQTT topic prefix for EIB messages (default 'eib')\n"
	;

#ifdef _GNU_SOURCE
static struct option long_opts[] = {
	{ "help", no_argument, NULL, '?', },
	{ "version", no_argument, NULL, 'V', },
	{ "verbose", no_argument, NULL, 'v', },

	{ "eib", required_argument, NULL, 'e', },
	{ "mqtt", required_argument, NULL, 'm', },
	{ "prefix", required_argument, NULL, 'p', },

	{ },
};
#else
#define getopt_long(argc, argv, optstring, longopts, longindex) \
	getopt((argc), (argv), (optstring))
#endif
static const char optstring[] = "Vv?e:m:p:";

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
static const char *mqtt_prefix = "eib";
static int mqtt_keepalive = 10;
static int mqtt_qos = 2;

/* EIB parameters */
static const char *eib_uri = "ip:localhost";

/* state */
static struct mosquitto *mosq;
static EIBConnection *eib;

/* cache */
struct cache {
	int emvalue;
	int mevalue;
	int epending;
	int mpending;
};

static struct cache *cache[64*1024];

static struct cache *getcache(eibaddr_t addr)
{
	if (!cache[addr]) {
		cache[addr] = malloc(sizeof(struct cache));
		memset(cache[addr], 0, sizeof(struct cache));
	}
	return cache[addr];
}

/* tools */
static const char *eibgaddrtostr(eibaddr_t val)
{
	static char buf[16];

	sprintf(buf, "%i/%i/%i", (val >> 11) & 0x1f, (val >> 8) & 0x07, val & 0xff);
	return buf;
}

static eibaddr_t strtoeibgaddr(const char *str, char **pendp)
{
	int addr[3], n = 0;
	char *endp;

	if (!pendp)
		pendp = &endp;

	addr[n++] = strtoul(str, pendp, 0);
	if (**pendp == '/')
		addr[n++] = strtoul(*pendp+1, pendp, 10);
	if (**pendp == '/')
		addr[n++] = strtoul(*pendp+1, pendp, 10);

	/* create item */
	switch (n) {
	case 1:
		return addr[0];
	case 2:
		return ((addr[0] & 0x1f) << 11) + (addr[1] & 0x7ff);
	case 3:
		return ((addr[0] & 0x1f) << 11) + ((addr[1] & 0x7) << 8) + (addr[2] & 0xff);
	}
	return ~0;
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
	static double filled_eib_slot;
	double tnow = libt_now();

	if ((filled_eib_slot + PKT_DELAY) < tnow)
		filled_eib_slot = tnow;
	else
		filled_eib_slot += PKT_DELAY;
	return filled_eib_slot;
}

static inline void *eibaddr_to_param(eibaddr_t addr, int value)
{
	return (void *)(long)(addr + (value << 16));
}
static inline eibaddr_t param_to_eibaddr(void *dat)
{
	return (long)dat & 0xffff;
}
static inline eibaddr_t param_to_value(void *dat)
{
	return (long)dat >> 16;
}

static void my_eib_send(void *dat)
{
	uint8_t shortdat[2] = { 0, 0x80, };
	eibaddr_t dst = param_to_eibaddr(dat);
	int value = param_to_value(dat);
	struct cache *c = getcache(dst);

	mylog(LOG_INFO, "eib:send %s %i\n", eibgaddrtostr(dst), value);
	if (value >= 0 && value < 64) {
		shortdat[1] |= value;
		if (EIBSendGroup(eib, dst, sizeof(shortdat), shortdat) < 0)
			mylog(LOG_ERR, "EIB: send %s %i failed", eibgaddrtostr(dst), value);
		++c->epending;
		c->mevalue = value;
	}
}

static void my_eib_send_request(void *dat)
{
	eibaddr_t dst = param_to_eibaddr(dat);
	static const uint8_t shortdat[2] = { 0, 0, };

	mylog(LOG_INFO, "eib:req %s\n", eibgaddrtostr(dst));
	if (EIBSendGroup(eib, dst, sizeof(shortdat), shortdat) < 0)
		mylog(LOG_ERR, "EIB: send %s failed", eibgaddrtostr(dst));
}

static void my_mqtt_clear_cache(void *dat)
{
	eibaddr_t dst = param_to_eibaddr(dat);
	const char *topic;
	struct cache *c = getcache(dst);

	mylog(LOG_INFO, "mqtt:clear %s\n", eibgaddrtostr(dst));
	topic = csprintf("%s/%s", mqtt_prefix, eibgaddrtostr(dst));
	/* try to publish an empty message.
	 * According to the docs, this clear the MQTT cache.
	 * The result is ignored here
	 */
	mosquitto_publish(mosq, NULL, topic, 0, NULL, mqtt_qos, 1);
	++c->mpending;
	c->emvalue = 0;
}

static void my_eib_send_request_or_clear_cache(void *dat)
{
	my_eib_send_request(dat);
	libt_add_timeout(1, my_mqtt_clear_cache, dat);
}

static void my_mqtt_msg(struct mosquitto *mosq, void *dat, const struct mosquitto_message *msg)
{
	eibaddr_t dst;
	char *topicsuffix;
	int value;
	struct cache *c;

	/* don't test for eib topic, it is required by the subscription */

	dst = strtoeibgaddr(msg->topic + strlen(mqtt_prefix) + 1/* '/' */, &topicsuffix);
	if (!strcmp(topicsuffix, "/?")) {
		libt_add_timeouta(next_eib_timeslot(), my_eib_send_request, eibaddr_to_param(dst, 0));
	} else if (!strcmp(topicsuffix, "/set")) {
		value = strtoul(msg->payload ?: "0", NULL, 0);
		libt_add_timeouta(next_eib_timeslot(), my_eib_send, eibaddr_to_param(dst, value));
	} else if (!*topicsuffix) {
		c = getcache(dst);
		if (!c->mpending) {
			/* write to main topic which is not our echo!
			 * This is probably a retained cache from the mqtt broker
			 * Refresh the EIB value, and reset the MQTT broker cache
			 * when not received in time
			 */
			libt_add_timeouta(next_eib_timeslot(), my_eib_send_request_or_clear_cache, eibaddr_to_param(dst, 0));
		} else {
			value = strtoul(msg->payload ?: "0", NULL, 0);
			mylog(LOG_INFO, "mqtt:publish %s: %i", eibgaddrtostr(dst), value);
			--c->mpending;
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

static void my_mqtt_push_value(eibaddr_t dst, int value)
{
	static char sbuf[32*2+1];
	const char *topic;
	int ret, mid;
	struct cache *c = getcache(dst);

	sprintf(sbuf, "%i", value);
	topic = csprintf("%s/%s", mqtt_prefix, eibgaddrtostr(dst));
	ret = mosquitto_publish(mosq, &mid, topic, strlen(sbuf), sbuf, mqtt_qos, 1);
	if (ret)
		mylog(LOG_ERR, "mosquitto_publish %s '%s': %s", topic, sbuf, mosquitto_strerror(ret));
	++c->mpending;
	c->emvalue = value;
}

static void eib_msg(EIBConnection *eib, eibaddr_t src, eibaddr_t dst, uint16_t hdr,
		const void *vdat, int len)
{
	const uint8_t *dat = vdat;
	struct cache *c = getcache(dst);
	int value, cmd;

	cmd = hdr & 0x03c0;
	switch (cmd) {
	case 0:
		/* read */
		/* TODO: handle */
		break;
	case 0x0040:
		/* response */
		libt_remove_timeout(my_mqtt_clear_cache, (void *)(long)dst);
		value = eib_value(hdr, dat, len);
		mylog(LOG_INFO, "eib:%s %s %i", "resp", eibgaddrtostr(dst), value);
		if (value != c->emvalue) {
			c->emvalue = value;
			my_mqtt_push_value(dst, value);
		}
		break;
	case 0x0080:
		/* write */
		value = eib_value(hdr, dat, len);
		if (c->epending) {
			--c->epending;
		} else {
			mylog(LOG_INFO, "eib:%s %s %i", "write", eibgaddrtostr(dst), value);
			my_mqtt_push_value(dst, value);
		}
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

int main(int argc, char *argv[])
{
	int opt, ret;
	struct pollfd pf[2] = {};
	char *str;
	eibaddr_t src, dst;
	int pkthdr;
	uint8_t buf[32];
	int logmask = LOG_UPTO(LOG_NOTICE);

	/* argument parsing */
	while ((opt = getopt_long(argc, argv, optstring, long_opts, NULL)) >= 0)
	switch (opt) {
	case 'V':
		fprintf(stderr, "%s %s\nCompiled on %s %s\n",
				NAME, VERSION, __DATE__, __TIME__);
		exit(0);
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
	case 'p':
		mqtt_prefix = optarg;
		break;

	case '?':
		fputs(help_msg, stderr);
		exit(0);
	default:
		fprintf(stderr, "unknown option '%c'", opt);
		fputs(help_msg, stderr);
		exit(1);
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
	mosquitto_will_set(mosq, mqtt_prefix, 0, NULL, mqtt_qos, 1);

	mosquitto_log_callback_set(mosq, my_mqtt_log);
	mosquitto_connect_callback_set(mosq, my_mqtt_connect);
	mosquitto_message_callback_set(mosq, my_mqtt_msg);

	ret = mosquitto_connect(mosq, mqtt_host, mqtt_port, mqtt_keepalive);
	if (ret)
		mylog(LOG_ERR, "mosquitto_connect %s:%i: %s", mqtt_host, mqtt_port, mosquitto_strerror(ret));

	ret = mosquitto_subscribe(mosq, NULL, csprintf("%s/#", mqtt_prefix), mqtt_qos);
	if (ret)
		mylog(LOG_ERR, "mosquitto_subscribe %s/#: %s", mqtt_prefix, mosquitto_strerror(ret));

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
