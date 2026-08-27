#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# RÉPOND AU PING (ICMP echo) SUR UN PORT MÉDIA EN KERNEL-BYPASS.
#
# ── Pourquoi (2026-08-22, prérequis d'intégration IPMEDIA) ─────────────────────────────────────
# Mesuré sur dl360-1 : les deux ports E810 (0000:11:00.0/.1) sont liés à **vfio-pci**, donc
# `ip -4 addr show` ne les voit PLUS — il n'y a pas de netdev, donc pas de pile IP noyau, donc
# personne pour répondre au ping. Et libmtl ne comble pas le trou : elle répond à l'ARP
# (`mt_arp_parse`, cf. mt_arp.c) mais son binaire ne contient **aucun** code ICMP (`strings
# libmtl.so | grep -i icmp` → zéro occurrence). Un port DPDK est donc muet au ping, par
# construction.
#
# Or l'exploitant d'un plant ST 2110 valide la connectivité de CHAQUE interface au ping depuis le
# routeur d'accès : « il est essentiel que lorsqu'on se place sur un routeur, chaque interface de
# l'équipement audiovisuel réponde à la commande Ping » (prérequis IPMEDIA Radio France, § 4). Sans
# réponse, l'intégration est refusée — et c'est le test le plus facile du monde à exécuter.
#
# Le repli « repasser en AF-XDP » (le netdev existe, le noyau répond) coûterait le gain de latence
# du socle DPDK narrow. On répond donc nous-mêmes.
#
# ── Le fix ─────────────────────────────────────────────────────────────────────────────────────
# Un répondeur echo dans mt_cni.c, sur le modèle EXACT de `arp_receive_request` (mt_arp.c) : même
# mempool système (`mt_sys_tx_mempool`), même file d'émission système (`mt_sys_queue_tx_burst`),
# même garde « est-ce pour NOUS » (`mt_sip_addr`). Un seul fichier touché, aucun meson.build à
# modifier.
#
# Deux détails qui comptent :
#  · La somme de contrôle ICMP est corrigée par DELTA (idiome test-pmd/icmpecho) et non recalculée :
#    seul l'octet `type` change (8 → 0), re-sommer la charge utile serait du travail pour rien.
#  · Le dispatch s'insère AVANT la branche IGMP dans la chaîne `next_proto_id`, là où l'ICMP tombait
#    jusqu'ici dans `cni_burst_to_kernel` — qui ne mène nulle part sans virtio_user.
#
# Aucun effet sur le plan de données : le CNI ne voit que le trafic NON média (les flux 2110 sont
# stéés par règle matérielle vers leurs files dédiées). Le coût est nul tant que personne ne pingue.

import sys

F = "lib/src/mt_cni.c"
MARK = "bobi.studio: repondeur ICMP echo"

c = open(F).read()
if MARK in c:
    print("patch ICMP echo : déjà appliqué"); sys.exit(0)


def rep(old, new, n=1):
    global c
    cnt = c.count(old)
    if cnt != n:
        print("patch ICMP echo : ERREUR — '%s…' attendu %dx, trouvé %dx" % (old[:60], n, cnt))
        sys.exit(1)
    c = c.replace(old, new)


# 1) En-tête ICMP de DPDK (rte_icmp_hdr, RTE_ICMP_TYPE_ECHO_REQUEST/REPLY). Ajouté explicitement :
#    mt_cni.c ne l'inclut aujourd'hui que par transitivité, ce qui n'est pas un contrat.
rep('#include "datapath/mt_queue.h"',
    '#include <rte_icmp.h>\n\n#include "datapath/mt_queue.h"')

# 2) Le répondeur, juste avant le dispatcher qui l'appelle.
HANDLER = r'''/* ── bobi.studio: repondeur ICMP echo ─────────────────────────────────────────
 * Un port lié à vfio-pci n'a plus de netdev noyau : plus personne ne répond au ping. libmtl répond
 * déjà à l'ARP (mt_arp_parse) mais n'avait AUCUN code ICMP. Or l'intégration sur un plant ST 2110
 * se valide interface par interface, au ping, depuis le routeur d'accès. On répond donc ici, sur le
 * modèle exact de arp_receive_request : même mempool système, même file d'émission système, même
 * garde « est-ce pour NOUS » (mt_sip_addr). Ne voit que le trafic non média (chemin CNI). */
#define MT_ICMP_ECHO_MAX (1400) /* charge utile bornée : au-delà, on ignore (jamais un ping de test) */
/* DPDK 26 a déprécié RTE_IP_ICMP_ECHO_* au profit de RTE_ICMP_TYPE_ECHO_* (RTE_DEPRECATED →
 * avertissement, et libmtl compile en -Werror). Repli pour un DPDK antérieur. */
#ifndef RTE_ICMP_TYPE_ECHO_REQUEST
#define RTE_ICMP_TYPE_ECHO_REQUEST (8)
#endif
#ifndef RTE_ICMP_TYPE_ECHO_REPLY
#define RTE_ICMP_TYPE_ECHO_REPLY (0)
#endif

static int cni_icmp_echo_reply(struct mt_cni_entry* cni, struct rte_mbuf* m,
                               struct rte_ipv4_hdr* req_ip, size_t l4_offset) {
  struct mtl_main_impl* impl = cni->impl;
  enum mtl_port port = cni->port;

  /* uniquement les echo request qui NOUS sont destinés (pas de relais, pas de broadcast) */
  if (req_ip->dst_addr != *(uint32_t*)mt_sip_addr(impl, port)) return -EINVAL;
  struct rte_icmp_hdr* req_icmp =
      rte_pktmbuf_mtod_offset(m, struct rte_icmp_hdr*, l4_offset);
  if (req_icmp->icmp_type != RTE_ICMP_TYPE_ECHO_REQUEST) return -EINVAL;

  int ip_hlen = req_ip->ihl * 4;
  int icmp_len = (int)ntohs(req_ip->total_length) - ip_hlen;
  if (icmp_len < (int)sizeof(struct rte_icmp_hdr) || icmp_len > MT_ICMP_ECHO_MAX) {
    dbg("%s(%d), icmp_len %d hors bornes\n", __func__, port, icmp_len);
    return -EINVAL;
  }
  /* le paquet doit tenir dans le segment courant : on ne recompose pas de mbuf chaîné */
  if ((size_t)(l4_offset + icmp_len) > rte_pktmbuf_data_len(m)) {
    dbg("%s(%d), echo segmente, ignore\n", __func__, port);
    return -EINVAL;
  }

  struct rte_mbuf* rpl = rte_pktmbuf_alloc(mt_sys_tx_mempool(impl, port));
  if (!rpl) {
    err_once("%s(%d), rpl_pkt alloc fail\n", __func__, port);
    return -ENOMEM;
  }
  rpl->pkt_len = rpl->data_len =
      sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + icmp_len;

  struct rte_ether_hdr* req_eth = rte_pktmbuf_mtod(m, struct rte_ether_hdr*);
  struct rte_ether_hdr* eth = rte_pktmbuf_mtod(rpl, struct rte_ether_hdr*);
  mt_macaddr_get(impl, port, mt_eth_s_addr(eth));
  rte_ether_addr_copy(mt_eth_s_addr(req_eth), mt_eth_d_addr(eth));
  eth->ether_type = htons(RTE_ETHER_TYPE_IPV4);

  struct rte_ipv4_hdr* ip = rte_pktmbuf_mtod_offset(rpl, struct rte_ipv4_hdr*,
                                                    sizeof(struct rte_ether_hdr));
  memset(ip, 0, sizeof(*ip));
  ip->version_ihl = 0x45; /* pas d'option : on ne recopie pas celles de la requête */
  ip->total_length = htons(sizeof(struct rte_ipv4_hdr) + icmp_len);
  ip->time_to_live = 64;
  ip->next_proto_id = IPPROTO_ICMP;
  ip->src_addr = req_ip->dst_addr; /* = notre sip, déjà vérifié ci-dessus */
  ip->dst_addr = req_ip->src_addr;
  ip->hdr_checksum = 0;
  ip->hdr_checksum = rte_ipv4_cksum(ip);

  struct rte_icmp_hdr* icmp = rte_pktmbuf_mtod_offset(
      rpl, struct rte_icmp_hdr*,
      sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr));
  /* identifiant, séquence ET charge utile sont renvoyés VERBATIM : c'est ce que vérifie ping(8) */
  rte_memcpy(icmp, req_icmp, icmp_len);
  icmp->icmp_type = RTE_ICMP_TYPE_ECHO_REPLY;
  /* Somme par DELTA (idiome test-pmd/icmpecho) : seul l'octet `type` a changé, la charge utile est
   * identique — la re-sommer entièrement serait du travail pour rien. */
  uint32_t cksum = ~req_icmp->icmp_cksum & 0xffff;
  cksum += ~htons(RTE_ICMP_TYPE_ECHO_REQUEST << 8) & 0xffff;
  cksum += htons(RTE_ICMP_TYPE_ECHO_REPLY << 8);
  cksum = (cksum & 0xffff) + (cksum >> 16);
  cksum = (cksum & 0xffff) + (cksum >> 16);
  icmp->icmp_cksum = (uint16_t)~cksum;

  uint16_t send = mt_sys_queue_tx_burst(impl, port, &rpl, 1);
  if (send < 1) {
    err_once("%s(%d), tx fail\n", __func__, port);
    rte_pktmbuf_free(rpl);
    return -EIO;
  }
  uint8_t* sip = (uint8_t*)&req_ip->src_addr;
  info_once("%s(%d), echo reply to %d.%d.%d.%d\n", __func__, port, sip[0], sip[1], sip[2],
            sip[3]);
  return 0;
}

static int cni_rx_handle('''

rep("static int cni_rx_handle(", HANDLER)

# 3) Dispatch : l'ICMP AVANT l'IGMP dans la chaîne next_proto_id. À ce point `hdr_offset` a déjà
#    absorbé ihl*4 (ligne « hdr_offset += ipv4_hdr->ihl * 4 ») → il pointe bien sur la couche 4.
rep("""      } else if (ipv4_hdr->next_proto_id == IPPROTO_IGMP) {""",
    """      } else if (ipv4_hdr->next_proto_id == IPPROTO_ICMP) {
        /* bobi.studio : sans netdev noyau (vfio-pci), c'est NOUS qui répondons au ping */
        cni_icmp_echo_reply(cni, m, ipv4_hdr, hdr_offset);
      } else if (ipv4_hdr->next_proto_id == IPPROTO_IGMP) {""")

open(F, "w").write(c)
print("patch ICMP echo : OK (cni_icmp_echo_reply ajouté à mt_cni.c)")
