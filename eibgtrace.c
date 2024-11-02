#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <unistd.h>
#include <endian.h>
#include <getopt.h>
#include <syslog.h>
#include <eibclient.h>

#define NAME "eibgtrace"
#ifndef VERSION
#define VERSION "<undefined version>"
#endif

#define USE_GRP 0

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

	ret = clock_gettime(CLOCK_REALTIME, &ts);
	if (ret < 0)
		mylog(LOG_ERR, "clock_gettime failed: %s", ESTR(errno));
	sprintf(buf, "%llu.%06lu", (long long)ts.tv_sec, ts.tv_nsec / 1000);
	return buf;
}

uint16_t getbe16(const void *dat)
{
	uint16_t val;

	memcpy(&val, dat, 2);
	return be16toh(val);

}

int eib_checksum(const void *vdat, int size)
{
	const uint8_t *dat = vdat;
	uint8_t sum = 0;

	for (; size; --size, ++dat)
		sum ^= *dat;
	return ~sum;
}

int main(int argc, char *argv[])
{
	int opt, ret, j;
	uint16_t pkthdr;
	uint16_t cmd;
	uint32_t value;
	eibaddr_t src, dst;
	uint8_t buf[32];
	uint8_t *pkt;
	int pktlen;

	/* argument parsing */
	while ((opt = getopt_long(argc, argv, optstring, long_opts, NULL)) >= 0)
	switch (opt) {
	case 'V':
		fprintf(stderr, "%s %s\nCompiled on %s %s\n",
				NAME, VERSION, __DATE__, __TIME__);
		exit(0);

	default:
		fprintf(stderr, "unknown option '%c'", opt);
	case '?':
		fputs(help_msg, stderr);
		exit(1);
		break;
	}

	atexit(my_exit);
	openlog(NAME, LOG_CONS | LOG_PERROR, LOG_LOCAL2);

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
#if USE_GRP
	ret = EIBOpen_GroupSocket(eib, 0);
#else
	ret = EIBOpenVBusmonitor(eib);
#endif
	if (ret < 0)
		mylog(LOG_ERR, "EIB: open socket failed");

	/* grab values */
	/* run */
	for (;;) {
		pkt = buf;
#if USE_GRP
		pktlen = ret = EIBGetGroup_Src(eib, sizeof(buf), buf, &src, &dst);
#else
		pktlen = ret = EIBGetBusmonitorPacket(eib, sizeof(buf), buf);
#endif
		if (ret < 0)
			mylog(LOG_ERR, "EIB: Get packet failed");
		printf("%s ", nowstr());
		for (j = 0; j < pktlen; ++j) {
#if 0
			if (!(j % 4))
				printf(" ");
#endif
			printf("%02x", pkt[j]);
		}

#if USE_GRP
		if (pktlen >= 2) {
#else
		if (!(pkt[0] & 0x33)) {
			printf(" %s", (pkt[0] & 0xc0) ? "ack" : "nack");
			if (!(pkt[0] & 0x0c))
				printf(",busy");

		} else if ((pkt[0] & 0x10) && pktlen >= 8 +1) {
			/* data frame */
			src = getbe16(pkt+1);
			dst = getbe16(pkt+3);
			pktlen -= 7;
			pkt += 6;
#endif
			pkthdr = getbe16(pkt);
			cmd = (pkthdr >> 6) & 0xf;
#if 0
			if (((pkthdr & 0x03c0) != 0x0040) && (pkthdr & 0x03c0) != 0x0080)
				/* only process response & write */
				continue;
#endif
			/* write */
			value = pkthdr & 0x3f;
			if (pktlen > 2) {
				int vallen = pktlen - 2;
				if (vallen > 4)
					/* limit to 4 bytes */
					vallen = 4;
				value = 0;
				for (j = 0; j < vallen; ++j)
					value = (value << 8) + pkt[2+j];
			}
			printf(" %s %s %u", eibphysstr(src), eibgroupstr(dst), value);
#if ! USE_GRP
			pkt -= 6;
			pktlen -= 6;

			static const char *cmdstrs[16] = {
				[0] = "rd",
				[1] = "wr",
				[2] = "tx",
			};

			static const char *priostr[4] = {
				[0] = "sys",
				[1] = "hi",
				[2] = "norm",
				[3] = "low",
			};

			printf(" (");
			printf("pri:%s", priostr[(pkt[0] >> 2) & 0x3]);
			if (cmdstrs[cmd])
				printf(",%s", cmdstrs[cmd]);
			if (!((pkt[0] & 0x60) == 0x20))
				printf(",%s", "repeat");
			if (pktlen > 1 && eib_checksum(pkt, pktlen-1) != pkt[pktlen-1])
				printf(",%s", "crcnok");
			printf(")");
#endif
		}
		printf("\n");
		fflush(stdout);
	}

	return 0;
}
