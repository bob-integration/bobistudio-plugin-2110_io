# SPDX-License-Identifier: GPL-3.0-or-later
# Build du receiver MTL natif (Phase C). Requiert libmtl + DPDK installés dans le container
# (template LXC « MTL »). En Phase A le binaire n'est pas utilisé (le plugin tourne en simu).
#
# Usage : make            # compile mtl_rx (échoue proprement si libmtl absent)
#         make clean

CC      ?= cc
CFLAGS  ?= -O2 -Wall
# libmtl est installé sous /usr/local → exporter PKG_CONFIG_PATH si besoin :
#   export PKG_CONFIG_PATH=/usr/local/lib/x86_64-linux-gnu/pkgconfig:/usr/local/lib/pkgconfig
PKG     := $(shell pkg-config --exists mtl 2>/dev/null && echo yes)
ifeq ($(PKG),yes)
  CFLAGS  += $(shell pkg-config --cflags mtl)
  LDLIBS  += $(shell pkg-config --libs mtl)
else
  LDLIBS  += -lmtl
endif
LDLIBS  += -lpthread -lm

mtl_rx: mtl_rx.c
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

clean:
	rm -f mtl_rx *.o

.PHONY: clean
