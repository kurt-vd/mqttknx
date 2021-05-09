PROGS	= mqttknxd eibtimeoff eibgtrace
default	: $(PROGS)

PREFIX	= /usr/local

CC	= gcc
CFLAGS	= -Wall
CPPFLAGS= -D_GNU_SOURCE
LDLIBS	= -lm
INSTOPTS= -s

VERSION := $(shell git describe --tags --always)

-include config.mk

CPPFLAGS += -DVERSION=\"$(VERSION)\"

eibtimeoff eibgtrace: LDLIBS:=-leibclient $(LDLIBS) -lrt
mqttknxd: LDLIBS:=-lmosquitto -leibclient $(LDLIBS)
mqttknxd: lib/libt.o

install: $(PROGS)
	$(foreach PROG, $(PROGS), install -vp -m 0777 $(INSTOPTS) $(PROG) $(DESTDIR)$(PREFIX)/bin/$(PROG);)

clean:
	rm -rf $(wildcard *.o lib/*.o) $(PROGS)
