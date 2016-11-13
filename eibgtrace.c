#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <unistd.h>
#include <getopt.h>
#include <syslog.h>
#include <eibclient.h>

#define NAME "eibgtrace"
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
	NAME ": trace EIB groups\n"
	"usage:	" NAME " [OPTIONS ...] [URI]\n"
	"\n"
	"Options\n"
	" -V, --version		Show version\n"
	;

#ifdef _GNU_SOURCE
static struct option long_opts[] = {
	{ "help", no_argument, NULL, '?', },
	{ "version", no_argument, NULL, 'V', },
	{ },
};
#else
#define getopt_long(argc, argv, optstring, longopts, longindex) \
	getopt((argc), (argv), (optstring))
#endif
static const char optstring[] = "V?";

struct item {
	struct item *next;
	eibaddr_t group;
	int delay;
};

/* EIB parameters */
static const char *eib_uri = "ip:localhost";

/* State */
static EIBConnection *eib;

static void my_exit(void)
{
	if (eib)
		EIBClose(eib);
}

const char *eibphysstr(int val)
{
	static char buf[16];

	sprintf(buf, "%i.%i.%i", (val >> 12) & 0x0f, (val >> 8) & 0x0f, val & 0xff);
	return buf;
}

const char *eibgroupstr(int val)
{
	static char buf[16];

	sprintf(buf, "%i/%i/%i", (val >> 11) & 0x1f, (val >> 8) & 0x07, val & 0xff);
	return buf;
}

const char *nowstr(void)
{
	struct timespec ts;
	int ret;
	static char buf[64];

	ret = clock_gettime(CLOCK_MONOTONIC, &ts);
	if (ret < 0)
		mylog(LOG_ERR, "clock_gettime failed: %s", ESTR(errno));
	sprintf(buf, "%lu.%06lu", ts.tv_sec, ts.tv_nsec / 1000);
	return buf;
}

int main(int argc, char *argv[])
{
	int opt, ret, j;
	uint16_t pkthdr;
	uint32_t value;
	eibaddr_t src, dst;
	uint8_t buf[32];

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
	}

	atexit(my_exit);
	openlog(NAME, LOG_CONS | LOG_PERROR, LOG_DAEMON);

	if (optind >= argc) {
		fputs(help_msg, stderr);
		exit(1);
	}

	if (optind < argc)
		eib_uri = argv[optind++];

	/* EIB start */
	eib = EIBSocketURL(eib_uri);
	if (!eib)
		mylog(LOG_ERR, "eib socket failed");
	ret = EIBOpen_GroupSocket(eib, 0);
	if (ret < 0)
		mylog(LOG_ERR, "EIB: open groupsocket failed");

	/* grab values */
	/* run */
	while (1) {
		ret = EIBGetGroup_Src(eib, sizeof (buf), buf, &src, &dst);
		if (ret < 0)
			mylog(LOG_ERR, "EIB: Get packet failed");
		if (ret < 2)
			/* too short, just ignore */
			continue;
		pkthdr = (buf[0] << 8) + buf[1];
		if (((pkthdr & 0x03c0) != 0x0040) && (pkthdr & 0x03c0) != 0x0080)
			/* only process response & write */
			continue;
		/* write */
		value = pkthdr & 0x3f;
		if (ret > 2) {
			ret -= 2;
			if (ret > 4)
				/* limit to 4 bytes */
				ret = 4;
			value = 0;
			for (j = 0; j < ret; ++j)
				value = (value << 8) + buf[2+j];
		}
		printf("%s\t%s\t%s\t%u\n", nowstr(), eibphysstr(src), eibgroupstr(dst), value);
		fflush(stdout);
	}

	return 0;
}
