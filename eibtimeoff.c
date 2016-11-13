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
#include <eibclient.h>

#define NAME "eibtimeoff"
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
	NAME ": turn off an EIB device after timeout\n"
	"usage:	" NAME " [OPTIONS ...] GROUP=TIMEOUT\n"
	"\n"
	"Options\n"
	" -V, --version		Show version\n"
	" -e, --eib=URI		Specify alternate EIB uri\n"
	"			Like ip:xxx or usb: or ...\n"
	;

#ifdef _GNU_SOURCE
static struct option long_opts[] = {
	{ "help", no_argument, NULL, '?', },
	{ "version", no_argument, NULL, 'V', },

	{ "eib", required_argument, NULL, 'e', },
	{ },
};
#else
#define getopt_long(argc, argv, optstring, longopts, longindex) \
	getopt((argc), (argv), (optstring))
#endif
static const char optstring[] = "V?e:";

struct item {
	struct item *next;
	eibaddr_t group;
	int delay;
};

/* EIB parameters */
static const char *eib_uri = "ip:localhost";
static struct item *items;

static void parse_add_item(const char *str)
{
	int addr[3], n = 0;
	char *endp;
	struct item *item;

	addr[n++] = strtoul(str, &endp, 0);
	if (*endp == '/')
		addr[n++] = strtoul(endp+1, &endp, 10);
	if (*endp == '/')
		addr[n++] = strtoul(endp+1, &endp, 10);
	if (*endp != '=')
		mylog(LOG_ERR, "cannot parse '%s'", str);

	/* create item */
	item = malloc(sizeof(*item));
	memset(item, 0, sizeof(*item));

	switch (n) {
	case 1:
		item->group = addr[0];
		break;
	case 2:
		item->group = ((addr[0] & 0x1f) << 11) + (addr[1] & 0x7ff);
		break;
	case 3:
		item->group = ((addr[0] & 0x1f) << 11) + ((addr[1] & 0x7) << 8) + (addr[2] & 0xff);
		break;
	}

	item->delay = strtoul(endp+1, &endp, 10);
	switch (*endp) {
	case 'w':
		item->delay *= 7;
	case 'd':
		item->delay *= 24;
	case 'h':
		item->delay *= 60;
	case 'm':
		item->delay *= 60;
		break;
	}

	/* insert */
	item->next = items;
	items = item;
}

/* State */
static EIBConnection *eib;
static volatile int sigalrm;

static void sigalrmhandler(int sig)
{
	sigalrm = 1;
	signal(sig, sigalrmhandler);
}

static void my_exit(void)
{
	if (eib)
		EIBClose(eib);
}

const char *eibgroupstr(int val)
{
	static char buf[16];

	sprintf(buf, "%i/%i/%i", (val >> 11) & 0x1f, (val >> 8) & 0x07, val & 0xff);
	return buf;
}

int main(int argc, char *argv[])
{
	int opt, ret, j;
	struct pollfd pf[1] = {};
	struct item *it;
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
	case 'e':
		eib_uri = optarg;
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

	signal(SIGALRM, sigalrmhandler);
	atexit(my_exit);
	openlog(NAME, LOG_CONS | LOG_PERROR, LOG_LOCAL2);

	if (optind >= argc) {
		fputs(help_msg, stderr);
		exit(1);
	}

	for (; optind < argc; ++optind)
		parse_add_item(argv[optind]);

	/* EIB start */
	eib = EIBSocketURL(eib_uri);
	if (!eib)
		mylog(LOG_ERR, "eib socket failed");
	ret = EIBOpen_GroupSocket(eib, 0);
	if (ret < 0)
		mylog(LOG_ERR, "EIB: open groupsocket failed");

	/* grab values */
	for (it = items; it; it = it->next) {
		static const uint8_t dat[2] = { 0, 0, };

		ret = EIBSendGroup(eib, it->group, sizeof(dat), dat);
		if (ret < 0)
			mylog(LOG_ERR, "EIB: read %s failed", eibgroupstr(it->group));
	}

	pf[0].fd = EIB_Poll_FD(eib);
	pf[0].events = POLL_IN;
	/* run */
	while (1) {
		if (sigalrm) {
			static uint8_t dat[2] = { 0x00, 0x80, };

			sigalrm = 0;
			ret = EIBSendGroup(eib, items->group, sizeof(dat), dat);
			if (ret < 0)
				mylog(LOG_ERR, "EIB: send %s failed", eibgroupstr(items->group));
		}
		ret = poll(pf, 1, 1000);
		if (ret < 0 && errno == EINTR)
			continue;
		if (ret < 0)
			mylog(LOG_ERR, "poll ...");
		if (!pf[0].revents)
			continue;
		ret = EIB_Poll_Complete(eib);
		if (ret < 0)
			mylog(LOG_ERR, "EIB: poll_complete");
		if (!ret)
			continue;
		/* received */
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
		if (dst == items->group) {
			/* schedule (or cancel) next alarm */
			alarm(value ? items->delay : 0);
		}
	}

	return 0;
}
