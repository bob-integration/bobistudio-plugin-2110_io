#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# ADMET LE MULTICAST PTP (224.0.1.129, UDP 319/320) DANS LE PIPELINE NIC via une règle rte_flow,
# sur le chemin CNI/DPDK de libmtl.
#
# ── Cause racine (banc E810/ice DPDK pur, 2026-07-09) ──────────────────────────────────────────
# En DPDK pur sur E810/ice, le PTP interne de libmtl (ENGINE_PTP=libmtl, socle full-PF DPDK) ne
# reçoit AUCUN paquet PTP : la NIC droppe le mcast au niveau L2. Dans `ptp_init` (mt_ptp.c), la
# branche CNI `if (mt_has_cni(impl, port) && !mt_drv_mcast_in_dp(impl, port))` fait seulement
# `mt_mcast_join` + `mt_mcast_l2_join` — elle ne pose AUCUNE règle rte_flow. Or sur E810 c'est la
# règle rte_flow (dst-IP mcast + UDP dst-port -> QUEUE) qui ADMET le mcast dans le pipeline : une
# session `st20p_rx` en pose une (mt_flow.c, branche `mt_is_multicast_ip`) et REÇOIT bien son mcast.
# La queue CNI est une MT_RXQ_FLOW_F_SYS_QUEUE et dev/mt_dev.c saute la création de flow pour ces
# queues → rien n'admet le PTP. C'est le 2ᵉ blocage RX, après l'option IP Router Alert
# (cf. patch_igmp_router_alert.py) : le join arrive au switch, mais la NIC ne laisse pas entrer le
# trafic PTP faute de règle rte_flow.
#
# ── Le fix (« Fix A ») ─────────────────────────────────────────────────────────────────────────
# Dans la branche CNI de `ptp_init`, après `mt_mcast_l2_join(...)`, poser DEUX règles rte_flow
# (une port EVENT 319, une port GEN 320) dirigées vers la QUEUE de la rxq CNI, en réutilisant
# EXACTEMENT le mécanisme prouvé des sessions (`mt_rx_flow_create`, remplissage identique du
# `struct mt_rxq_flow` : dip=mcast, sip=mt_sip_addr, dst_port ; flags=0 → vraie règle matérielle
# vers la queue CNI, PAS FORCE_SOCKET/SYS_QUEUE). Les handles sont libérés dans `ptp_uinit`
# (`mt_rx_flow_free`). Deux champs sont ajoutés à `struct mt_ptp_impl` (mt_main.h) pour stocker
# les handles. N'affecte QUE le chemin CNI/DPDK (la branche socket est intouchée).

import sys

MARK = "bobi.studio: PTP mcast rte_flow"


def make_rep(label):
    def rep(c, old, new, n):
        cnt = c.count(old)
        if cnt != n:
            print("%s : ERREUR — '%s…' attendu %dx, trouvé %dx" % (label, old[:44], n, cnt))
            sys.exit(1)
        return c.replace(old, new)
    return rep


# ─────────────────────────────────────────────────────────────────────────────────────────────
# Fichier 1 : lib/src/mt_main.h — deux champs dans struct mt_ptp_impl pour les handles de flow
# ─────────────────────────────────────────────────────────────────────────────────────────────
FH = "lib/src/mt_main.h"
h = open(FH).read()
rep = make_rep("patch PTP mcast flow (mt_main.h)")

if MARK in h:
    print("patch PTP mcast flow (mt_main.h) : déjà appliqué")
else:
    h = rep(
        h,
        "  /* for no cni case */\n"
        "  struct mt_rxq_entry* gen_rxq;   /* for MT_PTP_UDP_GEN_PORT */\n"
        "  struct mt_rxq_entry* event_rxq; /* for MT_PTP_UDP_EVENT_PORT */\n"
        "  struct mt_sch_tasklet_impl* rxq_tasklet;",
        "  /* for no cni case */\n"
        "  struct mt_rxq_entry* gen_rxq;   /* for MT_PTP_UDP_GEN_PORT */\n"
        "  struct mt_rxq_entry* event_rxq; /* for MT_PTP_UDP_EVENT_PORT */\n"
        "  struct mt_sch_tasklet_impl* rxq_tasklet;\n"
        "  /* bobi.studio: PTP mcast rte_flow — handles des regles rte_flow admettant le\n"
        "     mcast PTP (319/320) vers la queue CNI sur le chemin DPDK (E810/ice) */\n"
        "  struct mt_rx_flow_rsp* cni_flow_rsp_evt;\n"
        "  struct mt_rx_flow_rsp* cni_flow_rsp_gen;",
        1)
    open(FH, "w").write(h)
    print("patch PTP mcast flow (mt_main.h) : appliqué")


# ─────────────────────────────────────────────────────────────────────────────────────────────
# Fichier 2 : lib/src/mt_ptp.c — création (ptp_init) + libération (ptp_uinit) des règles rte_flow
# ─────────────────────────────────────────────────────────────────────────────────────────────
FC = "lib/src/mt_ptp.c"
c = open(FC).read()
rep = make_rep("patch PTP mcast flow (mt_ptp.c)")

if MARK in c:
    print("patch PTP mcast flow (mt_ptp.c) : déjà appliqué")
    sys.exit(0)

# 1) include de mt_flow.h (mt_rx_flow_create / mt_rx_flow_free)
rep_c_include = '#include "mt_cni.h"\n'
c = rep(
    c,
    rep_c_include,
    '#include "mt_cni.h"\n'
    '#include "mt_flow.h" /* bobi.studio: PTP mcast rte_flow */\n',
    1)

# 2) ptp_init : après mt_mcast_l2_join, poser les 2 règles rte_flow vers la queue CNI
rep(  # sanity : le point d'ancrage existe une seule fois
    c,
    "    mt_mcast_l2_join(impl, &ptp_l2_multicast_eaddr, port);\n"
    "  } else {",
    "    mt_mcast_l2_join(impl, &ptp_l2_multicast_eaddr, port);\n"
    "  } else {",
    1)
c = c.replace(
    "    mt_mcast_l2_join(impl, &ptp_l2_multicast_eaddr, port);\n"
    "  } else {",
    "    mt_mcast_l2_join(impl, &ptp_l2_multicast_eaddr, port);\n"
    "\n"
    "    /* bobi.studio: PTP mcast rte_flow — sur E810/ice DPDK, joindre le groupe mcast\n"
    "       (mt_mcast_join) n'ADMET PAS le trafic 224.0.1.129 dans le pipeline NIC : seule une\n"
    "       regle rte_flow (dst mcast IP + UDP dst port -> QUEUE) le laisse entrer, exactement\n"
    "       comme une session st20p_rx. La queue systeme CNI saute la creation de flow, donc le\n"
    "       PTP n'arrive jamais. On pose deux regles (event 319 / general 320) vers la rxq CNI. */\n"
    "    {\n"
    "      struct mt_rxq_entry* cni_rxq = mt_get_cni(impl)->entries[port].rxq;\n"
    "      if (cni_rxq) {\n"
    "        uint16_t cni_q = mt_rxq_queue_id(cni_rxq);\n"
    "        struct mt_rxq_flow ptp_flow;\n"
    "        memset(&ptp_flow, 0, sizeof(ptp_flow));\n"
    "        rte_memcpy(ptp_flow.dip_addr, ptp->mcast_group_addr, MTL_IP_ADDR_LEN);\n"
    "        rte_memcpy(ptp_flow.sip_addr, mt_sip_addr(impl, port), MTL_IP_ADDR_LEN);\n"
    "        ptp_flow.dst_port = MT_PTP_UDP_EVENT_PORT;\n"
    "        ptp->cni_flow_rsp_evt = mt_rx_flow_create(impl, port, cni_q, &ptp_flow);\n"
    "        if (!ptp->cni_flow_rsp_evt)\n"
    "          warn(\"%s(%d), ptp event rte_flow create fail\\n\", __func__, port);\n"
    "        ptp_flow.dst_port = MT_PTP_UDP_GEN_PORT;\n"
    "        ptp->cni_flow_rsp_gen = mt_rx_flow_create(impl, port, cni_q, &ptp_flow);\n"
    "        if (!ptp->cni_flow_rsp_gen)\n"
    "          warn(\"%s(%d), ptp general rte_flow create fail\\n\", __func__, port);\n"
    "      } else {\n"
    "        warn(\"%s(%d), no cni rxq, ptp mcast rte_flow skipped\\n\", __func__, port);\n"
    "      }\n"
    "    }\n"
    "  } else {")

# 3) ptp_uinit : libérer les règles rte_flow avant la sortie de la branche CNI
c = rep(
    c,
    "    mt_mcast_l2_leave(impl, &ptp_l2_multicast_eaddr, port);\n"
    "    mt_mcast_leave(impl, mt_ip_to_u32(ptp->mcast_group_addr), 0, port);\n"
    "  }",
    "    /* bobi.studio: PTP mcast rte_flow — liberer les regles posees dans ptp_init */\n"
    "    if (ptp->cni_flow_rsp_evt) {\n"
    "      mt_rx_flow_free(impl, port, ptp->cni_flow_rsp_evt);\n"
    "      ptp->cni_flow_rsp_evt = NULL;\n"
    "    }\n"
    "    if (ptp->cni_flow_rsp_gen) {\n"
    "      mt_rx_flow_free(impl, port, ptp->cni_flow_rsp_gen);\n"
    "      ptp->cni_flow_rsp_gen = NULL;\n"
    "    }\n"
    "    mt_mcast_l2_leave(impl, &ptp_l2_multicast_eaddr, port);\n"
    "    mt_mcast_leave(impl, mt_ip_to_u32(ptp->mcast_group_addr), 0, port);\n"
    "  }",
    1)

open(FC, "w").write(c)
print("patch PTP mcast flow (mt_ptp.c) : appliqué")
