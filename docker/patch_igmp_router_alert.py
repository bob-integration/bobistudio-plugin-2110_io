#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# AJOUTE L'OPTION IP ROUTER ALERT (RFC 2113) AUX REPORTS IGMP de libmtl.
#
# ── Cause racine (banc SPAN Cisco dl360-1 2026-07-09) ─────────────────────────────────────────
# libmtl (mt_mcast.c mcast_fill_ipv4) forge ses reports IGMPv3 avec un en-tête IP de 20 octets
# (IHL=5, SANS options). Le kernel Linux, lui, met TOUJOURS l'option IP Router Alert. Un switch à
# IGMP snooping RFC-conforme (Cisco du plant) IGNORE un report IGMP sans Router Alert → il ne
# snoope pas le join → ne forwarde pas le multicast (dont le PTP 224.0.1.129) au port de la PF
# DPDK. Symptôme mesuré : `ptp4l` kernel LOCKE (14 ns) mais le PTP interne libmtl (ENGINE_PTP=
# libmtl, socle full-PF DPDK) reste « not connected » (0 paquet reçu) sur le MÊME port/sip.
# Capture paquet (raw socket, tcpdump absent du nœud) : report kernel IHL=24 + octets 94040000
# (Router Alert) ; report libmtl IHL=20, absent → le seul delta.
#
# ── Le fix ───────────────────────────────────────────────────────────────────────────────────
# IHL=6, insérer 4 octets d'option (0x94 0x04 0x00 0x00) ENTRE l'en-tête IP et l'IGMP, décaler la
# couche IGMP de 4, +4 à ip.total_length, et l3_len += 4 (checksum IP offload sur l'en-tête+option
# et pkt_len correct). N'affecte QUE mt_mcast.c (les autres users de mt_mbuf_init_ipv4 — dhcp, ptp
# — sont dans d'autres fichiers, intouchés).

import sys

F = "lib/src/mt_mcast.c"
c = open(F).read()

if "bobi.studio: option IP Router Alert" in c:
    print("patch IGMP Router Alert : déjà appliqué"); sys.exit(0)


def rep(old, new, n):
    global c
    cnt = c.count(old)
    if cnt != n:
        print("patch IGMP Router Alert : ERREUR — '%s…' attendu %dx, trouvé %dx" % (old[:44], n, cnt))
        sys.exit(1)
    c = c.replace(old, new)


# 1) mcast_fill_ipv4 : IHL=6 + écriture de l'option juste après l'en-tête IP (offset eth+20)
rep(
    "  ip_hdr->version_ihl = (4 << 4) | (sizeof(struct rte_ipv4_hdr) / 4);",
    "  ip_hdr->version_ihl = (4 << 4) | ((sizeof(struct rte_ipv4_hdr) + 4) / 4);\n"
    "  /* bobi.studio: option IP Router Alert (RFC 2113) apres l'en-tete IP (offset eth+20) */\n"
    "  {\n"
    "    uint8_t* _bobi_ra = rte_pktmbuf_mtod_offset(\n"
    "        pkt, uint8_t*, sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr));\n"
    "    _bobi_ra[0] = 0x94; _bobi_ra[1] = 0x04; _bobi_ra[2] = 0x00; _bobi_ra[3] = 0x00;\n"
    "  }",
    1)

# 2) offset IGMP décalé de 4 (query + report_on_query + report_on_action)
rep(
    "  hdr_offset += sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr);",
    "  hdr_offset += sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + 4;",
    3)

# 3) ip.total_length +4 (reports)
rep(
    "htons(sizeof(struct rte_ipv4_hdr) + mb_report_len)",
    "htons(sizeof(struct rte_ipv4_hdr) + 4 + mb_report_len)",
    2)

# 4) ip.total_length +4 (query — code sous #ifdef MCAST_ENABLE_QUERY, patché par cohérence)
rep(
    "htons(sizeof(struct rte_ipv4_hdr) + mb_query_len)",
    "htons(sizeof(struct rte_ipv4_hdr) + 4 + mb_query_len)",
    1)

# 5) l3_len += 4 (checksum IP offload sur en-tete+option ; pkt_len = l2+l3+igmp correct)
rep(
    "mt_mbuf_init_ipv4(pkt);",
    "mt_mbuf_init_ipv4(pkt); pkt->l3_len += 4; /* bobi.studio: +4 option Router Alert */",
    3)

open(F, "w").write(c)
print("patch IGMP Router Alert : appliqué (mt_mcast.c)")
