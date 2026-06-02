# SPDX-License-Identifier: GPL-3.0-or-later
# Build du receiver MTL natif (Phase C). Requiert libmtl + DPDK installés dans le container
# (template LXC « MTL »). En Phase A le binaire n'est pas utilisé (le plugin tourne en simu).
#
# Usage : make            # compile mtl_rx (échoue proprement si libmtl absent)
#         make clean

CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra
# Drapeaux libmtl/DPDK via pkg-config si disponible (Phase C) :
PKG     := $(shell pkg-config --exists mtl 2>/dev/null && echo yes)
ifeq ($(PKG),yes)
  CFLAGS  += $(shell pkg-config --cflags mtl)
  LDLIBS  += $(shell pkg-config --libs mtl)
else
  LDLIBS  += -lmtl
endif

mtl_rx: mtl_rx.c
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

clean:
	rm -f mtl_rx *.o

.PHONY: clean
