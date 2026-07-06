#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# 2022-7 VRAIMENT hitless côté TX : un port AF_XDP dont le LIEN est mort ne draine plus son
# ring XSK. Sans ce patch, xdp_tx() retourne 0 en boucle (« tx prod full ») ; or les mbufs
# d'une trame sont PARTAGÉS entre les deux legs d'une session redondante → les frames ne se
# libèrent jamais, la session ENTIÈRE se fige (st20_tx_queue_fatal_error, « not dpdk user
# pmd, nothing to do » = irrécupérable en AF_XDP), le leg SAIN s'arrête aussi, et la
# récupération interne (audio) fuit des memzones DPDK jusqu'au plafond (2560) → TX mort
# définitif (banc 2026-07-06, nœud 30).
#
# Parade : carrier de la NIC lu en cache (100 ms) dans le chemin TX ; lien mort ⇒ les
# paquets sont consommés et LIBÉRÉS comme s'ils étaient émis. La session garde ses deux
# legs, cadence nominale sur le leg vivant, ZÉRO interruption ; au retour du lien, le leg
# réémet tout seul (drop levé au prochain rafraîchissement du cache).
#
# Idempotent + fail-fast : si une ancre n'est pas trouvée (source MTL changée), on ÉCHOUE
# le build.
import sys

F = "lib/src/dev/mt_af_xdp.c"
src = open(F).read()

MARK = "/* bobi.studio: 2022-7 hitless TX */"
if MARK in src:
    print("patch AF-XDP tx link drop : déjà appliqué"); sys.exit(0)

# 1) Champs d'état par queue (cache carrier + compteur de drop).
OLD_STRUCT = (
    "  uint64_t stat_tx_prod_reserve_fail;\n"
    "  uint64_t stat_tx_prod_full;\n"
)
NEW_STRUCT = OLD_STRUCT + (
    "  " + MARK + "\n"
    "  uint64_t stat_tx_link_drop;\n"
    "  uint64_t link_check_tsc;\n"
    "  bool link_up;\n"
)

# 2) Helper carrier (cache 100 ms) inséré juste avant xdp_tx().
ANCHOR_TX = "static uint16_t xdp_tx(struct mtl_main_impl* impl, struct mt_xdp_queue* xq,"
HELPER = (
    MARK + "\n"
    "/* Lien physique du port (cache 100 ms). Un lien mort ne draine plus le ring XSK :\n"
    " * emettre dessus fige la session entiere (mbufs partages entre les legs 2022-7). */\n"
    "static bool xdp_tx_link_up(struct mtl_main_impl* impl, struct mt_xdp_queue* xq) {\n"
    "  uint64_t now = mt_get_tsc(impl);\n"
    "  if (now >= xq->link_check_tsc) {\n"
    "    xq->link_check_tsc = now + (uint64_t)100 * NS_PER_MS;\n"
    "    bool up = true;\n"
    "    const char* if_name = mt_kernel_if_name(impl, xq->port);\n"
    "    if (if_name && if_name[0]) {\n"
    "      char path[128];\n"
    "      snprintf(path, sizeof(path), \"/sys/class/net/%s/carrier\", if_name);\n"
    "      FILE* f = fopen(path, \"r\");\n"
    "      if (f) {\n"
    "        up = (fgetc(f) != '0');\n"
    "        fclose(f);\n"
    "      }\n"
    "    }\n"
    "    if (up != xq->link_up)\n"
    "      info(\"%s(%d, %u), link %s\\n\", __func__, xq->port, xq->q, up ? \"UP\" : \"DOWN\");\n"
    "    xq->link_up = up;\n"
    "  }\n"
    "  return xq->link_up;\n"
    "}\n"
    "\n"
)

# 3) Garde en tête de xdp_tx : lien mort => drop-as-sent.
OLD_GUARD = (
    "  xdp_tx_check_free(xq); /* do we need check free threshold for every tx burst */\n"
)
NEW_GUARD = (
    "  " + MARK + "\n"
    "  /* lien mort => consommer/liberer comme emis (drop), la session ne se fige pas */\n"
    "  if (!xdp_tx_link_up(impl, xq)) {\n"
    "    xdp_tx_poll_done(xq); /* recupere les completions restantes d'avant la coupure */\n"
    "    for (uint16_t i = 0; i < nb_pkts; i++) rte_pktmbuf_free(tx_pkts[i]);\n"
    "    xq->stat_tx_link_drop += nb_pkts;\n"
    "    return nb_pkts;\n"
    "  }\n"
    + OLD_GUARD
)

# 4) Ligne de stats périodique (visibilité du drop dans les logs).
OLD_STAT = (
    "  if (xq->stat_tx_prod_full) {\n"
    "    info(\"%s(%d,%u), tx prod full %\" PRIu64 \"\\n\", __func__, port, q,\n"
    "         xq->stat_tx_prod_full);\n"
    "    xq->stat_tx_prod_full = 0;\n"
    "  }\n"
)
NEW_STAT = OLD_STAT + (
    "  if (xq->stat_tx_link_drop) {\n"
    "    warn(\"%s(%d,%u), tx link down, dropped %\" PRIu64 \" pkts\\n\", __func__, port, q,\n"
    "         xq->stat_tx_link_drop);\n"
    "    xq->stat_tx_link_drop = 0;\n"
    "  }\n"
)

for name, old in (("struct", OLD_STRUCT), ("xdp_tx", ANCHOR_TX),
                  ("garde", OLD_GUARD), ("stats", OLD_STAT)):
    if old not in src:
        print("ERREUR: ancre '%s' introuvable dans %s (source MTL modifiée ?)" % (name, F),
              file=sys.stderr)
        sys.exit(1)

src = src.replace(OLD_STRUCT, NEW_STRUCT, 1)
src = src.replace(ANCHOR_TX, HELPER + ANCHOR_TX, 1)
src = src.replace(OLD_GUARD, NEW_GUARD, 1)
src = src.replace(OLD_STAT, NEW_STAT, 1)
open(F, "w").write(src)
print("patch AF-XDP tx link drop : appliqué")
