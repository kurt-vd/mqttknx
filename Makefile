PROGS	= mqttnxd eibtimeoff eibgtrace mqttoff
default	: $(PROGS)

PREFIX	= /usr/local

CC	= gcc
CFLAGS	= -Wall
CPPFLAGS= -D_GNU_SOURCE
LDLIBS	=
INSTOPTS= -s

VERSION := $(shell git describe --tags --always)

-include config.mk

CPPFLAGS += -DVERSION=\"$(VERSION)\"

eibtimeoff eibgtrace: LDLIBS:=-leibclient $(LDLIBS) -lrt
mqttnxd: LDLIBS:=-lmosquitto -leibclient $(LDLIBS)
mqttoff: LDLIBS:=-lmosquitto $(LDLIBS)
mqttoff: lib/libt.o
mqttnxd: lib/libt.o

install: $(PROGS)
	$(foreach PROG, $(PROGS), install -vp -m 0777 $(INSTOPTS) $(PROG) $(DESTDIR)$(PREFIX)/bin/$(PROG);)

clean:
	rm -rf $(wildcard *.o lib/*.o) $(PROGS)
