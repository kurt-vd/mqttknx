#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <unistd.h>
#include <getopt.h>
#include <mosquitto.h>

#define NAME "mqttoff"
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
	NAME ": an MQTT timeout-turnoff daemon\n"
	"usage:	" NAME " [OPTIONS ...] [PATTERN] ...\n"
	"\n"
	"Options\n"
	" -V, --version		Show version\n"
	" -m, --mqtt=HOST[:PORT]Specify alternate MQTT host+port\n"
	" -s, --suffix=STR	Give MQTT topic suffix for timeouts (default '~')\n"
	"\n"
	"Paramteres\n"
	" PATTERN	A pattern to subscribe for\n"
	;

#ifdef _GNU_SOURCE
static struct option long_opts[] = {
	{ "help", no_argument, NULL, '?', },
	{ "version", no_argument, NULL, 'V', },

	{ "mqtt", required_argument, NULL, 'm', },
	{ "suffix", required_argument, NULL, 's', },

	{ },
};
#else
#define getopt_long(argc, argv, optstring, longopts, longindex) \
	getopt((argc), (argv), (optstring))
#endif
static const char optstring[] = "V?m:s:";

/* signal handler */
static volatile int sigterm;

/* MQTT parameters */
static const char *mqtt_host = "localhost";
static int mqtt_port = 1883;
static const char *mqtt_suffix = "/timeoff";
static int mqtt_suffixlen = 1;
static int mqtt_keepalive = 10;
static int mqtt_qos = 1;

/* state */
static struct mosquitto *mosq;

struct item {
	struct item *next;

	char *topic;
	char *resetvalue;
	int delay;
	time_t ontime;
};

struct item *items;

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

static struct item *get_item(const char *topic)
{
	struct item *it;

	for (it = items; it; it = it->next)
		if (!strcmp(it->topic, topic))
			return it;
	/* not found, create one */
	it = malloc(sizeof(*it));
	memset(it, 0, sizeof(*it));
	it->topic = strdup(topic);

	/* insert in linked list */
	it->next = items;
	items = it;
	return it;
}

static void reset_item(struct item *it)
{
	int ret;

	ret = mosquitto_publish(mosq, NULL, it->topic, strlen(it->resetvalue ?: "0"), it->resetvalue ?: "0", mqtt_qos, 1);
	if (ret < 0)
		mylog(LOG_ERR, "mosquitto_publish %s: %s", it->topic, mosquitto_strerror(ret));
	/* clear cache too */
	it->ontime = 0;
	mylog(LOG_INFO, "%s = %s", it->topic, it->resetvalue ?: "0");
}

static void my_mqtt_msg(struct mosquitto *mosq, void *dat, const struct mosquitto_message *msg)
{
	int len;
	char *tok, *topic;
	struct item *it;

	len = strlen(msg->topic);
	if (len > mqtt_suffixlen && !strcmp(msg->topic + len - mqtt_suffixlen, mqtt_suffix)) {
		/* this is a timeout set msg */
		topic = strdup(msg->topic);
		topic[len-mqtt_suffixlen] = 0;

		it = get_item(topic);
		/* don't need this copy anymore */
		free(topic);

		/* process timeout spec */
		if (msg->payloadlen) {
			tok = strtok(msg->payload, " \t");
			if (tok) {
				it->delay = strtoul(tok, &tok, 0);
				switch (tolower(*tok)) {
				case 'w':
					it->delay *= 7;
				case 'd':
					it->delay *= 24;
				case 'h':
					it->delay *= 60;
				case 'm':
					it->delay *= 60;
					break;
				}
			}
			tok = strtok(NULL, " \t");
			if (tok)
				it->resetvalue = strdup(tok);
			else if (it->resetvalue) {
				free(it->resetvalue);
				it->resetvalue = NULL;
			}
		}
		mylog(LOG_INFO, "timeoff spec for %s: %us '%s'", it->topic, it->delay, it->resetvalue ?: "");

		if (it->delay && it->ontime &&
				((it->ontime + it->delay) < time(NULL)))
			reset_item(it);
		return;
	}
	/* find topic */
	it = get_item(msg->topic);

	if (!msg->payloadlen)
		return;
	if (it->resetvalue && !strcmp(it->resetvalue, (char *)msg->payload))
		it->ontime = 0;
	else if (!it->resetvalue && !strtoul(msg->payload, NULL, 0))
		/* clear ontime */
		it->ontime = 0;
	else if (!it->ontime)
		/* set ontime only on first set */
		it->ontime = time(NULL);
}

static void my_exit(void)
{
	if (mosq)
		mosquitto_disconnect(mosq);
}

int main(int argc, char *argv[])
{
	int opt, ret;
	struct item *it;
	char *str;
	time_t now;
	char mqtt_name[32];

	/* argument parsing */
	while ((opt = getopt_long(argc, argv, optstring, long_opts, NULL)) >= 0)
	switch (opt) {
	case 'V':
		fprintf(stderr, "%s %s\nCompiled on %s %s\n",
				NAME, VERSION, __DATE__, __TIME__);
		exit(0);
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
		mqtt_suffix = optarg;
		mqtt_suffixlen = strlen(mqtt_suffix);
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

	atexit(my_exit);

	/* MQTT start */
	mosquitto_lib_init();
	sprintf(mqtt_name, "%s-%i", NAME, getpid());
	mosq = mosquitto_new(mqtt_name, true, 0);
	if (!mosq)
		mylog(LOG_ERR, "mosquitto_new failed: %s", ESTR(errno));
	/* mosquitto_will_set(mosq, "TOPIC", 0, NULL, mqtt_qos, 1); */

	mosquitto_log_callback_set(mosq, my_mqtt_log);
	mosquitto_connect_callback_set(mosq, my_mqtt_connect);
	mosquitto_message_callback_set(mosq, my_mqtt_msg);

	ret = mosquitto_connect(mosq, mqtt_host, mqtt_port, mqtt_keepalive);
	if (ret)
		mylog(LOG_ERR, "mosquitto_connect %s:%i: %s", mqtt_host, mqtt_port, mosquitto_strerror(ret));

	if (optind >= argc) {
		ret = mosquitto_subscribe(mosq, NULL, "#", mqtt_qos);
		if (ret)
			mylog(LOG_ERR, "mosquitto_subscribe '#': %s", mosquitto_strerror(ret));
	} else for (; optind < argc; ++optind) {
		ret = mosquitto_subscribe(mosq, NULL, argv[optind], mqtt_qos);
		if (ret)
			mylog(LOG_ERR, "mosquitto_subscribe %s: %s", argv[optind], mosquitto_strerror(ret));
	}

	while (1) {
		time(&now);
		for (it = items; it; it = it->next) {
			if (it->ontime && it->delay && ((it->ontime + it->delay) < now))
				reset_item(it);
		}
		ret = mosquitto_loop(mosq, 1000, 1);
		if (ret)
			mylog(LOG_ERR, "mosquitto_loop: %s", mosquitto_strerror(ret));
	}
	return 0;
}
