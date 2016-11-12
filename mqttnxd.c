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
#include <mosquitto.h>
#include <eibclient.h>

#define NAME "mqttnxd"
#ifndef VERSION
#define VERSION "<undefined version>"
#endif

/* generic error logging */
#define LOG_ERR		3
#define LOG_WARNING	4
#define LOG_NOTICE	5
#define LOG_INFO	6
#define LOG_DEBUG	8
#define mylog(loglevel, fmt, ...) \
	({\
		fprintf(stderr, "%s: " fmt "\n", NAME, ##__VA_ARGS__);\
		fflush(stderr);\
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
	" -e, --eib=URI		Specify alternate EIB uri\n"
	"			Like ip:xxx or usb: or ...\n"
	" -m, --mqtt=HOST[:PORT]Specify alternate MQTT host+port\n"
	" -p, --prefix=STR	Give MQTT topic prefix for EIB messages (default 'eib')\n"
	;

#ifdef _GNU_SOURCE
static struct option long_opts[] = {
	{ "help", no_argument, NULL, '?', },
	{ "version", no_argument, NULL, 'V', },

	{ "eib", required_argument, NULL, 'e', },
	{ "mqtt", required_argument, NULL, 'm', },
	{ "prefix", required_argument, NULL, 'p', },

	{ },
};
#else
#define getopt_long(argc, argv, optstring, longopts, longindex) \
	getopt((argc), (argv), (optstring))
#endif
static const char optstring[] = "V?e:m:p:";

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
	int dir;
#define DIR_NONE 0
#define DIR_M_E	0x01
#define DIR_E_M 0x02
	int size;
	int len;
	void *dat;
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

static void updcache(eibaddr_t addr, int dir, const void *dat, int len)
{
	struct cache *c = getcache(addr);

	c->dir = dir;
	if (len > c->size) {
		/* increment in blocks of 32 */
		c->size = (len+31) & ~31;
		c->dat = realloc(c->dat, c->size);
	}
	memcpy(c->dat, dat, len);
	c->len = len;
}

static int cmpcache(eibaddr_t addr, int dir, const void *dat, int len)
{
	struct cache *c = getcache(addr);

	if (c->dir != dir)
		return -1;
	if (c->len != len)
		return -1;
	return memcmp(c->dat, dat, len);
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
	int logpri;

	if (level & MOSQ_LOG_ERR)
		logpri = LOG_ERR;
	else if (level & MOSQ_LOG_WARNING)
		logpri = LOG_WARNING;
	else if (level & MOSQ_LOG_NOTICE)
		logpri = LOG_NOTICE;
	else if (level & MOSQ_LOG_INFO)
		logpri = LOG_INFO;
	else if (level & MOSQ_LOG_DEBUG)
		return;//logpri = LOG_DEBUG;
	else
		return;

	mylog(logpri, "[mosquitto] %s", str);
}

static void my_mqtt_msg(struct mosquitto *mosq, void *dat, const struct mosquitto_message *msg)
{
	int ret;
	eibaddr_t dst;
	char *topicsuffix;
	int value;

	/* don't test for eib topic, it is required by the subscription */

	dst = strtoeibgaddr(msg->topic + strlen(mqtt_prefix) + 1/* '/' */, &topicsuffix);
	if (!strcmp(topicsuffix, "/request")) {
		static const uint8_t shortdat[2] = { 0, 0, };

		ret = EIBSendGroup(eib, dst, sizeof(shortdat), shortdat);
		if (ret < 0)
			mylog(LOG_ERR, "EIB: send %s failed", eibgaddrtostr(dst));
	} else if (!*topicsuffix) {
		uint8_t shortdat[2] = { 0, 0x80, };

		if (!cmpcache(dst, DIR_E_M, msg->payload, msg->payloadlen)) {
			/* ignore my echo */
			updcache(dst, DIR_NONE, msg->payload, msg->payloadlen);
			return;
		}
		/* write */
		value = strtoul(msg->payload, NULL, 0);
		if (value < 64) {
			shortdat[1] |= value;
			ret = EIBSendGroup(eib, dst, sizeof(shortdat), shortdat);
			if (ret < 0)
				mylog(LOG_ERR, "EIB: send %s %i failed", eibgaddrtostr(dst), value);
			updcache(dst, DIR_M_E, msg->payload, msg->payloadlen);
		}
	}
}

/* EIB events */
static void eib_msg(EIBConnection *eib, eibaddr_t src, eibaddr_t dst, uint16_t hdr,
		const void *vdat, int len)
{
	const uint8_t *dat = vdat;
	int ret, mid;
	const char *topic;
	char *str;
	static char sbuf[32*2+1];

	switch (hdr & 0x03c0) {
	case 0:
		/* read */
		/* TODO: handle */
		break;
	case 0x0040:
		/* response */
	case 0x0080:
		/* write */
		if (!len)
			sprintf(sbuf, "%i", hdr & 0x3f);
		else
			for (str = sbuf; len; --len, ++dat)
				str += sprintf(str, "%02x", *dat);
		if (!cmpcache(dst, DIR_M_E, sbuf, strlen(sbuf))) {
			/* ignore my echo */
			updcache(dst, DIR_NONE, sbuf, strlen(sbuf));
			return;
		}

		topic = csprintf("%s/%s", mqtt_prefix, eibgaddrtostr(dst));
		ret = mosquitto_publish(mosq, &mid, topic, strlen(sbuf), sbuf, mqtt_qos, 1);
		if (ret)
			mylog(LOG_ERR, "mosquitto_publish %s '%s': %s", topic, sbuf, mosquitto_strerror(ret));
		updcache(dst, DIR_E_M, sbuf, strlen(sbuf));
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

	/* argument parsing */
	while ((opt = getopt_long(argc, argv, optstring, long_opts, NULL)) >= 0)
	switch (opt) {
	case 'V':
		fprintf(stderr, "%s %s\nCompiled on %s %s\n",
				NAME, VERSION, __DATE__, __TIME__);
		exit(0);
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
		ret = poll(pf, 2, 1000);
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
