#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# RX+TX pacing RL sur le MÊME port dpdk (narrow matériel mono-port) : l'attache d'un shaper RL
# TX (dev_tx_queue_set_rl_rate → rte_tm_hierarchy_commit, mt_dev.c:747) STOPPE le port
# (dev_started=0) le temps du commit. libmtl encadre ce commit par inf->resetting=true/false
# (mt_dev.c:745-749 ; champ « The port is temporarily off, e.g. during rte_tm_hierarchy_commit »,
# mt_main.h:706-707), mais SEUL mt_cni.c:347 lisait ce flag. Les DEUX datapaths (RX ET TX) pollent
# le port stoppé sans garde → « ETHDEV: lcore called {rx,tx}_pkt_burst for not ready port 0 » →
# SIGSEGV, crash-loop du daemon. (banc dl360-1 2026-07-07, corrélation 100 % avec un port qui fait
# RX+TX sous MTL_PACING=rl : SANS garde = 17× rx_pkt_burst not ready + crash ; garde RX SEULE =
# 0 rx mais le TX segfault encore via tx_pkt_burst → il faut garder les DEUX.)
#
# Parade : le flag inf->resetting est PORT-WIDE (les deux sens) → on garde les DEUX points uniques
# de burst du datapath (datapath/mt_queue.c) :
#   - mt_rxq_burst : draine TOUS les consommateurs RX (vidéo, ANC, fastmetadata, CNI, PTP, TX-RTCP)
#     via le vtable entry->burst.
#   - mt_txq_burst : draine TOUS les émetteurs TX (vidéo/audio/ANC) via entry->burst ; mt_txq_burst_busy
#     boucle dessus donc est couvert transitivement.
# Pendant que inf->resetting est vrai, le burst retourne 0 (rien reçu / rien émis) au lieu de toucher
# le port stoppé ; il reprend seul dès la fin du commit. Comme mt_rxq_entry/mt_txq_entry ne portent
# que `parent`(impl) et pas le port, on AJOUTE un champ `port` renseigné au create (mt_rxq_get/
# mt_txq_get ont l'enum mtl_port) ; inf = mt_if(impl, port) → inf->resetting (accès id. à mt_cni.c).
#
# NB : la CNI a déjà sa propre garde (mt_cni.c:347) ; la double garde est inoffensive (retour 0).
# Réserve (mesurée au banc) : le commit fige le port ~qq 100 ms→s sur ice → au-delà de ~6-7 TX
# créés en rafale, le détecteur de hang TX (video_trs_burst_fail, 1000 ms) peut déclencher un
# fatal-error/recreate avant que la garde ne suffise → À COUPLER au patch hiérarchie TM (#13) +
# créer les TX AVANT d'abonner la RX (minimise les commits à chaud). La garde rend le port-stop
# SURVIVABLE (plus de SIGSEGV du datapath) ; elle ne raccourcit pas le commit lui-même.
#
# Idempotent + fail-fast : si une ancre n'est pas trouvée (source MTL changée), on ÉCHOUE le build.
import sys

MARK = "/* bobi.studio: RX/TX resetting guard */"

# ---------------------------------------------------------------- 1) mt_queue.h : champ `port` (RX+TX)
FH = "lib/src/datapath/mt_queue.h"
h = open(FH).read()

if MARK in h:
    print("patch RX/TX resetting guard : déjà appliqué (mt_queue.h)")
else:
    OLD_RXS = (
        "struct mt_rxq_entry {\n"
        "  struct mtl_main_impl* parent;\n"
        "  uint16_t queue_id;\n"
    )
    NEW_RXS = (
        "struct mt_rxq_entry {\n"
        "  struct mtl_main_impl* parent;\n"
        "  " + MARK + " enum mtl_port port; /* pour lire mt_if(parent, port)->resetting */\n"
        "  uint16_t queue_id;\n"
    )
    OLD_TXS = (
        "struct mt_txq_entry {\n"
        "  struct mtl_main_impl* parent;\n"
        "  uint16_t queue_id;\n"
    )
    NEW_TXS = (
        "struct mt_txq_entry {\n"
        "  struct mtl_main_impl* parent;\n"
        "  " + MARK + " enum mtl_port port; /* pour lire mt_if(parent, port)->resetting */\n"
        "  uint16_t queue_id;\n"
    )
    for name, old in (("struct mt_rxq_entry", OLD_RXS), ("struct mt_txq_entry", OLD_TXS)):
        if old not in h:
            print("ERREUR: ancre '%s' introuvable dans %s (source MTL modifiée ?)" % (name, FH),
                  file=sys.stderr)
            sys.exit(1)
    h = h.replace(OLD_RXS, NEW_RXS, 1).replace(OLD_TXS, NEW_TXS, 1)
    open(FH, "w").write(h)

# ---------------------------------------------------------------- 2) mt_queue.c : sets + gardes
FC = "lib/src/datapath/mt_queue.c"
c = open(FC).read()

if MARK in c:
    print("patch RX/TX resetting guard : déjà appliqué (mt_queue.c)")
    sys.exit(0)

# 2a) renseigner entry->port au create. `entry->parent = impl;` apparaît dans mt_rxq_get PUIS
#     mt_txq_get (mêmes 3 lignes) → replace(count=2) touche les deux (chacun a un `port` en scope).
OLD_SET = "  entry->parent = impl;\n"
NEW_SET = (
    "  entry->parent = impl;\n"
    "  " + MARK + "\n"
    "  entry->port = port;\n"
)

# 2b) garde en tête du point unique de burst RX (draine tous les consommateurs via le vtable).
OLD_RXB = (
    "uint16_t mt_rxq_burst(struct mt_rxq_entry* entry, struct rte_mbuf** rx_pkts,\n"
    "                      const uint16_t nb_pkts) {\n"
    "  return entry->burst(entry, rx_pkts, nb_pkts);\n"
    "}\n"
)
NEW_RXB = (
    "uint16_t mt_rxq_burst(struct mt_rxq_entry* entry, struct rte_mbuf** rx_pkts,\n"
    "                      const uint16_t nb_pkts) {\n"
    "  " + MARK + "\n"
    "  /* Port temporairement stoppé pendant rte_tm_hierarchy_commit (attache shaper RL TX) :\n"
    "   * poller rte_eth_rx_burst sur un port not-ready segfault. On ne reçoit rien le temps du\n"
    "   * commit ; le burst reprend seul ensuite. */\n"
    "  if (rte_atomic32_read(&mt_if(entry->parent, entry->port)->resetting)) return 0;\n"
    "  return entry->burst(entry, rx_pkts, nb_pkts);\n"
    "}\n"
)

# 2c) garde symétrique en tête du point unique de burst TX (mt_txq_burst_busy boucle dessus → couvert).
OLD_TXB = (
    "uint16_t mt_txq_burst(struct mt_txq_entry* entry, struct rte_mbuf** tx_pkts,\n"
    "                      uint16_t nb_pkts) {\n"
    "  return entry->burst(entry, tx_pkts, nb_pkts);\n"
    "}\n"
)
NEW_TXB = (
    "uint16_t mt_txq_burst(struct mt_txq_entry* entry, struct rte_mbuf** tx_pkts,\n"
    "                      uint16_t nb_pkts) {\n"
    "  " + MARK + "\n"
    "  /* Même flag PORT-WIDE que la RX : émettre via rte_eth_tx_burst sur un port stoppé (commit\n"
    "   * RL en cours) déclenche « tx_pkt_burst for not ready port » puis un fatal-error/segfault.\n"
    "   * On n'émet rien le temps du commit (les mbufs restent en file, drainés au redémarrage). */\n"
    "  if (rte_atomic32_read(&mt_if(entry->parent, entry->port)->resetting)) return 0;\n"
    "  return entry->burst(entry, tx_pkts, nb_pkts);\n"
    "}\n"
)

for name, old in (("entry->parent set (rx+tx)", OLD_SET),
                  ("mt_rxq_burst", OLD_RXB), ("mt_txq_burst", OLD_TXB)):
    if old not in c:
        print("ERREUR: ancre '%s' introuvable dans %s (source MTL modifiée ?)" % (name, FC),
              file=sys.stderr)
        sys.exit(1)
if c.count(OLD_SET) < 2:
    print("ERREUR: `entry->parent = impl;` attendu 2× (rxq+txq) dans %s, trouvé %d"
          % (FC, c.count(OLD_SET)), file=sys.stderr)
    sys.exit(1)

c = c.replace(OLD_SET, NEW_SET, 2)   # mt_rxq_get + mt_txq_get
c = c.replace(OLD_RXB, NEW_RXB, 1)
c = c.replace(OLD_TXB, NEW_TXB, 1)
open(FC, "w").write(c)
print("patch RX/TX resetting guard : appliqué")
