#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour DPDK (appliqué au build de bobi-mtl, AVANT ./script/build_dpdk.sh).
# Fix RACINE des sessions TX mortes en cold-batch (cf. mémoire
# rl-commit-mempool-exhaustion-silent-tx-death + rapport 2026-07-13, moteur 140).
#
# ── Cause racine (capturée live, moteur 140, cold-batch 6 sessions TX) ─────────────────────────
#   ICE_DRIVER: ice_tm_setup_txq_node(): move lan queue 15 failed              (ice_tm.c:713)
#   ICE_DRIVER: ice_tx_queue_start(): Failed to set up txq TM node   (-EIO)
#   ICE_DRIVER: ice_dev_start(): fail to start Tx queue 15                     (ice_ethdev.c)
#   MTL: Error: bobi: port did NOT restart after commit (ret -5) -- left resetting=true
# Chaque création/modif de session TX (dev_tx_queue_set_rl_rate, mt_dev.c) fait un
# rte_tm_hierarchy_commit → rte_eth_dev_stop/start du PORT ENTIER. Au restart, pour CHAQUE queue,
# ice_ena_vsi_txq() (ice_rxtx.c) crée d'abord le nœud HW dans le groupe de scheduling PAR DÉFAUT
# du VSI (comportement firmware natif, pas configurable) ; ice_tx_queue_start() appelle ENSUITE
# ice_tm_setup_txq_node() (ice_tm.c) qui — puisque pf->tm_conf.committed est vrai dès qu'un arbre
# RL a été posé une fois — détecte hw_node->parent != sw_node->parent->sched_node et émet
# ice_aq_move_recfg_lan_txq() : un aller-retour admin-queue firmware qui REPARENTE le nœud vers
# notre groupe RL (patch_tm_hierarchy.py). C'EST DONC STRUCTUREL : le move a lieu à CHAQUE
# démarrage de port, pour CHAQUE queue déjà en hiérarchie — on ne peut pas l'éviter en choisissant
# un autre fan-out côté libmtl (essayé/écarté, cf. rapport ; le groupe par défaut du VSI est fixé
# par le firmware, pas par notre arbre TM). Le vrai problème est qu'AVANT ce patch :
#  1. ice_aq_move_recfg_lan_txq() n'était PAS retenté — un simple aller-retour firmware raté sous
#     rafale de commits (`patch_tx_burst_rendezvous.py` prouve déjà que le port lui-même est
#     retenté 5×20ms mais SEULEMENT au niveau rte_eth_dev_start() dans son ENSEMBLE, ce qui rejoue
#     ice_tx_queue_start() de TOUTES les queues depuis zéro et peut retomber sur le même échec) ;
#  2. un échec de move faisait ice_tm_setup_txq_node() retourner une erreur ⇒ ice_tx_queue_start()
#     de CETTE queue échoue ⇒ ice_dev_start() échoue pour LE PORT ENTIER (pas juste la queue en
#     cause) ⇒ plus AUCUNE queue TX ne draine ⇒ les mbufs restent bloqués dans les rings MTL ⇒
#     mempool vidé ⇒ build ret -207 permanent, silencieux (le hang detector natif ne vit que dans
#     le chemin burst, jamais atteint puisqu'un builder en échec d'alloc ne construit aucun
#     paquet). C'est aussi la landmine « PTP not connected définitif ».
#
# ── Le patch : retry borné + dégradation SANS tuer le port ─────────────────────────────────────
# Dans ice_tm_setup_txq_node() (drivers/net/intel/ice/ice_tm.c, DPDK 26.03) :
#   1. ice_aq_move_recfg_lan_txq() est retenté 5× / 20 ms (même borne que le « commit
#      port-restart guard » côté libmtl, patch_tx_burst_rendezvous.py — un aller-retour AQ normal
#      est bien plus rapide, 5×20ms est une marge large sous rafale) ;
#   2. si les 5 tentatives échouent encore : ON NE RETOURNE PLUS D'ERREUR. Le nœud HW est laissé
#      TEL QUEL (hw_node->parent et l'arbre sched ne sont PAS touchés tant que le move n'a pas
#      RÉELLEMENT réussi ⇒ aucun état « à moitié déplacé » possible) ; on applique quand même
#      ice_cfg_hw_node() (rate/priorité/poids) sur le nœud dans sa position actuelle — dégradé
#      (pas dans le bon groupe RL ce cycle) mais FONCTIONNEL : la queue continue de drainer, le
#      port démarre, ice_dev_start() réussit pour toutes les queues. Le move est retenté depuis
#      zéro au prochain commit (prochaine session TX) → auto-guérison, jamais de perte de TX.
#
# ── Mécanique d'injection (DPDK, pas libmtl) ────────────────────────────────────────────────────
# ice_tm.c vit dans l'arbre DPDK, cloné+patché par `./script/build_dpdk.sh` (PAS par ce dépôt) :
# ce script clone `dpdk-${DPDK_VER}.tar.gz` (versions.env de MTL) PUIS applique tous les
# `patches/dpdk/${DPDK_VER}/*.patch` du dépôt MTL (boucle glob triée). Notre point d'injection est
# donc de DÉPOSER notre propre .patch dans CE dossier, dans le clone MTL, AVANT que
# build_dpdk.sh ne tourne — il sera appliqué par le même mécanisme que les 8 patches Intel/MTL
# officiels de ce dossier (aucun ne touche ice_tm.c : pas de conflit). C'est du `patch -p1`
# standard (contexte de diff), pas du str-replace comme les patches libmtl de ce dossier — le
# fail-fast est NATIF à `patch` (contexte non trouvé ⇒ `patch` échoue ⇒ le RUN Docker échoue).
#
# Idempotent (skip si déjà déposé) ; fail-fast si le dossier patches/dpdk/<ver> n'existe pas
# (structure du dépôt MTL changée) ou si versions.env est absent/illisible.
import sys
import pathlib

DIFF = '--- a/drivers/net/intel/ice/ice_tm.c\n+++ b/drivers/net/intel/ice/ice_tm.c\n@@ -3,6 +3,8 @@\n  */\n #include <rte_ethdev.h>\n #include <rte_tm_driver.h>\n+/* bobi.studio: move retry */\n+#include <rte_cycles.h>\n \n #include "ice_ethdev.h"\n #include "ice_rxtx.h"\n@@ -695,6 +697,7 @@\n \t\tstruct ice_aqc_move_txqs_data *buf;\n \t\tuint8_t txqs_moved = 0;\n \t\tuint16_t buf_size = ice_struct_size(buf, txqs, 1);\n+\t\tint ret;\n \n \t\tbuf = ice_malloc(hw, buf_size);\n \t\tif (buf == NULL)\n@@ -707,12 +710,44 @@\n \t\tbuf->txqs[0].q_teid = hw_node->info.node_teid;\n \t\tbuf->txqs[0].txq_id = qid;\n \n-\t\tint ret = ice_aq_move_recfg_lan_txq(hw, 1, true, false, false, false, 50,\n-\t\t\t\t\t\tNULL, buf, buf_size, &txqs_moved, NULL);\n+\t\t/* bobi.studio: move retry\n+\t\t * ice_aq_move_recfg_lan_txq() is a plain admin-queue round-trip to firmware;\n+\t\t * under a burst of back-to-back rte_tm_hierarchy_commit() (cold-batch TX session\n+\t\t * creation) it has been observed to fail transiently (-EIO) on the LAST queue of\n+\t\t * a queue-group, which previously made this function return an error, which made\n+\t\t * the caller (ice_tx_queue_start(), ice_rxtx.c) fail, which made ice_dev_start()\n+\t\t * fail for the WHOLE port -- killing TX drain for every queue, not just this one,\n+\t\t * and leaking every in-flight mempool (rte_pktmbuf_alloc_bulk permanently failing,\n+\t\t * "build ret -207" downstream in MTL). Retry the AQ command a bounded number of\n+\t\t * times (5 x 20 ms, same idiom as MTL\'s own commit port-restart guard) before\n+\t\t * giving up -- see below for what happens if it still fails. */\n+\t\tint bobi_try;\n+\t\tfor (bobi_try = 0; bobi_try < 5; bobi_try++) {\n+\t\t\ttxqs_moved = 0;\n+\t\t\tret = ice_aq_move_recfg_lan_txq(hw, 1, true, false, false, false, 50,\n+\t\t\t\t\t\t\tNULL, buf, buf_size, &txqs_moved, NULL);\n+\t\t\tif (!ret && txqs_moved != 0)\n+\t\t\t\tbreak;\n+\t\t\trte_delay_ms(20);\n+\t\t}\n \t\tif (ret || txqs_moved == 0) {\n-\t\t\tPMD_DRV_LOG(ERR, "move lan queue %u failed", qid);\n+\t\t\t/* bobi.studio: move retry -- degrade, don\'t kill the port.\n+\t\t\t * All 5 retries failed: the queue is NOT moved (hw_node->parent and the\n+\t\t\t * sched tree are untouched, so there is no half-moved state to clean up).\n+\t\t\t * Do NOT return an error here: that would fail ice_tx_queue_start() for\n+\t\t\t * this ONE queue, which fails ice_dev_start() for ALL queues on the port.\n+\t\t\t * Instead, leave the queue under its current (native VSI) scheduler node\n+\t\t\t * for this cycle -- it keeps draining traffic, just without this queue\'s\n+\t\t\t * explicit RL placement -- and let ice_cfg_hw_node() below still apply the\n+\t\t\t * rate/priority/weight onto it as-is. The move is retried again from\n+\t\t\t * scratch on the next commit (dev restart), so this self-heals. */\n+\t\t\tPMD_DRV_LOG(ERR,\n+\t\t\t\t    "move lan queue %u failed after %d retries -- keeping it on "\n+\t\t\t\t    "its current scheduler node this cycle (degraded RL "\n+\t\t\t\t    "placement, port TX kept alive; retried again next commit)",\n+\t\t\t\t    qid, bobi_try);\n \t\t\tice_free(hw, buf);\n-\t\t\treturn ICE_ERR_PARAM;\n+\t\t\treturn ice_cfg_hw_node(hw, sw_node, hw_node);\n \t\t}\n \n \t\t/* now update the ice_sched_nodes to match physical layout */\n'

PATCH_NAME = "0100-bobi-studio-ice-tm-move-retry.patch"

versions_path = pathlib.Path("versions.env")
if not versions_path.exists():
    print("ERREUR: versions.env introuvable dans le clone MTL (cwd attendu = /src/MTL, "
          "structure du dépôt changée ?)", file=sys.stderr)
    sys.exit(1)

dpdk_ver = None
for line in versions_path.read_text().splitlines():
    line = line.strip()
    if line.startswith("DPDK_VER="):
        dpdk_ver = line.split("=", 1)[1].strip()
        break
if not dpdk_ver:
    print("ERREUR: DPDK_VER introuvable dans versions.env", file=sys.stderr)
    sys.exit(1)

patches_dir = pathlib.Path("patches") / "dpdk" / dpdk_ver
if not patches_dir.is_dir():
    print("ERREUR: dossier %s introuvable (DPDK_VER=%s, structure du dépôt MTL changée ?)"
          % (patches_dir, dpdk_ver), file=sys.stderr)
    sys.exit(1)

target = patches_dir / PATCH_NAME
if target.exists() and target.read_text() == DIFF:
    print("patch ice_tm move retry : déjà déposé (%s)" % target)
    sys.exit(0)

target.write_text(DIFF)
print("patch ice_tm move retry : déposé dans %s (appliqué par build_dpdk.sh)" % target)
