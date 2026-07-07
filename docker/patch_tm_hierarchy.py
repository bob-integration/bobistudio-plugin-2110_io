#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# LÈVE LE MUR DES 8 FILES TX RL (narrow matériel E810 >8 senders/port ET création en rafale).
#
# ── Cause racine (diag agent capacité, dl360-1 2026-07-07) ────────────────────────────────────
# L'E810 gère des milliers de files shapées (arbre traffic-manager 9 niveaux logiques,
# fan-out max 8 PAR NŒUD au niveau queue-group). Le « 8 » n'est PAS une limite HW : c'est la
# FORME de l'arbre que libmtl construit. `dev_rl_init_nonleaf_nodes` (mt_dev.c) crée une CHAÎNE
# LINÉAIRE de ST_TM_NONLEAF_NODES_NUM_PF=7 non-feuilles (chaque parent 1 seul enfant), le dernier
# maillon (node id 262, niveau 6 = queue-group) portant TOUTES les files-feuilles. Le PMD ice
# (drivers/net/intel/ice/ice_tm.c commit_new_hierarchy) calcule alors :
#     nb_qps = min(nodes_created_per_level[qg_lvl] * hw->max_children[qg_lvl],
#                  hw->layer_info[q_lvl].max_device_nodes)
# Avec UN SEUL nœud QG → nodes_created_per_level[qg]=1 → nb_qps = 1 × 8 = 8. La 9ᵉ file est
# rejetée (« ice_tx_queue_start: Failed to add lan txq » / « fail to start Tx queue 8 »).
# Côté PF (drv != IAVF), les feuilles ne sont PAS attachées en bloc (dev_init_ratelimit_all est
# réservé à IAVF) : chaque file TX est rattachée À LA DEMANDE par dev_tx_queue_set_rl_rate quand
# une session sender fixe son débit → c'est CE chemin qui bute au 8ᵉ sender (et en rafale).
#
# ── Le patch : arbre RAMIFIÉ au dernier niveau non-feuille ────────────────────────────────────
# Au lieu d'un unique nœud QG, on bâtit :
#   • une CHAÎNE de (nonleaf_nodes_num - 1) non-feuilles : niveaux 0..qg-1, ids 256..261 ;
#   • P nœuds QG au niveau qg (= nonleaf_nodes_num - 1 = 6 pour le PF), enfants du dernier maillon
#     de la chaîne (node 261, niveau 5), ids 262, 263, … ; chacun accueille ≤ ST_TM_QG_FANOUT (8)
#     feuilles. → nodes_created_per_level[qg] = P → nb_qps = P × 8.
#   • les feuilles (files TX) sont RÉPARTIES : la file q est rattachée au parent QG (262 + q/8),
#     niveau feuille (nonleaf_nodes_num = 7). q/8 ∈ [0,P) est déterministe → cohérent entre
#     l'attache initiale et les ré-attaches à chaud (changement de débit RL par session).
#
# P = ceil(nb_tx_q / 8), borné par ST_TM_RL_PARENTS_MAX (16 → 128 feuilles = MT_MAX_RL_ITEMS) ET
# par le fan-out réel du niveau parent (niveau 5) : si rte_tm_node_add refuse un parent QG
# (max_children[5] atteint, « insufficient number of child nodes supported »), on s'arrête et la
# capacité effective = P_créés × 8 (loggé). L'arbre reste VALIDE (fan-out respecté à chaque niveau).
# On garde ST_TM_NONLEAF_NODES_NUM_PF=7 inchangé (= niveau feuille) : cohérence avec le reste de
# libmtl (ST_TM_LAST_NONLEAF_NODE_ID_PF=262 = base des ids QG ; la chaîne fait juste 6 maillons
# au lieu de 7, le 7ᵉ « slot » devenant la rangée de P parents QG).
#
# Le contrôleur (controller.py) relève en conséquence le clamp tx_queues≤7 (0.39.1) — re-gaté sur
# le pacing RL uniquement (tsc/tsc_narrow ne construisent aucun arbre TM → jamais bornés).
#
# Idempotent + fail-fast : si une ancre n'est pas trouvée (source MTL changée), on ÉCHOUE le build.
import sys

MARK = "/* bobi.studio: RL TM hierarchy branched */"

FC = "lib/src/dev/mt_dev.c"
FH = "lib/src/mt_main.h"

# ---------------------------------------------------------------- 0) champ inf->tx_rl_nb_parents
h = open(FH).read()
if MARK in h:
    print("patch TM hierarchy : déjà appliqué (mt_main.h)")
else:
    OLD_FIELD = "  bool tx_rl_root_active;\n"
    NEW_FIELD = (
        "  bool tx_rl_root_active;\n"
        "  " + MARK + "\n"
        "  uint16_t tx_rl_nb_parents; /* nb de nœuds QG (arbre RL branché, fan-out > 8) */\n"
    )
    if OLD_FIELD not in h:
        print("ERREUR: ancre 'tx_rl_root_active' introuvable dans %s (source MTL modifiée ?)" % FH,
              file=sys.stderr)
        sys.exit(1)
    h = h.replace(OLD_FIELD, NEW_FIELD, 1)
    open(FH, "w").write(h)

# ---------------------------------------------------------------- lecture mt_dev.c
c = open(FC).read()
if MARK in c:
    print("patch TM hierarchy : déjà appliqué (mt_dev.c)")
    sys.exit(0)

# ---------------------------------------------------------------- 1) constantes fan-out branché
OLD_DEFS = "#define ST_DEFAULT_RL_BPS (1024 * 1024 * 1024 / 8) /* 1g bit per second */\n"
NEW_DEFS = (
    OLD_DEFS
    + MARK + "\n"
    + "#define ST_TM_QG_FANOUT 8       /* max enfants d'un nœud queue-group sur E810 (le mur) */\n"
    + "#define ST_TM_RL_PARENTS_MAX 16 /* plafond de nœuds QG → 16*8 = 128 = MT_MAX_RL_ITEMS */\n"
)
if OLD_DEFS not in c:
    print("ERREUR: ancre 'ST_DEFAULT_RL_BPS' introuvable dans %s" % FC, file=sys.stderr)
    sys.exit(1)
c = c.replace(OLD_DEFS, NEW_DEFS, 1)

# ---------------------------------------------------------------- 2) dev_rl_init_nonleaf_nodes
# Remplace la chaîne linéaire mono-parent par : chaîne (qg-1) maillons + P parents QG ramifiés.
OLD_INIT = (
    "  for (int i = 0; i < nonleaf_nodes_num; i++) {\n"
    "    node_id = ST_ROOT_NODE_ID + i;\n"
    "    ret = rte_tm_node_add(port_id, node_id, parent_id, 0, 1, i, &np, &error);\n"
    "    if (ret < 0) {\n"
    "      err(\"%s(%d), node add error: (%d)%s\\n\", __func__, port, ret,\n"
    "          mt_string_safe(error.message));\n"
    "      return ret;\n"
    "    }\n"
    "    parent_id = node_id;\n"
    "  }\n"
    "\n"
    "  inf->tx_rl_root_active = true;\n"
)
NEW_INIT = (
    "  " + MARK + "\n"
    "  /* Arbre RAMIFIÉ (lève le mur du fan-out 8, cf. patch_tm_hierarchy.py) : chaîne de\n"
    "   * (nonleaf_nodes_num-1) non-feuilles au-dessus du niveau queue-group, puis PLUSIEURS\n"
    "   * nœuds QG au dernier niveau non-feuille, chacun portant <= ST_TM_QG_FANOUT feuilles. */\n"
    "  uint32_t qg_level = nonleaf_nodes_num - 1;\n"
    "\n"
    "  /* 1) chaîne linéaire des non-feuilles (niveaux 0..qg_level-1) */\n"
    "  for (uint32_t i = 0; i < qg_level; i++) {\n"
    "    node_id = ST_ROOT_NODE_ID + i;\n"
    "    ret = rte_tm_node_add(port_id, node_id, parent_id, 0, 1, i, &np, &error);\n"
    "    if (ret < 0) {\n"
    "      err(\"%s(%d), nonleaf node %u add error: (%d)%s\\n\", __func__, port, node_id, ret,\n"
    "          mt_string_safe(error.message));\n"
    "      return ret;\n"
    "    }\n"
    "    parent_id = node_id;\n"
    "  }\n"
    "\n"
    "  /* 2) P nœuds QG (niveau qg_level), enfants du dernier maillon de la chaîne. P couvre le\n"
    "   *    budget de files (ceil(nb_tx_q/8)), borné par ST_TM_RL_PARENTS_MAX et par le fan-out\n"
    "   *    réel du niveau parent (node_add renvoie -EINVAL « insufficient ... child nodes »). */\n"
    "  uint16_t want = (inf->nb_tx_q + ST_TM_QG_FANOUT - 1) / ST_TM_QG_FANOUT;\n"
    "  if (want < 1) want = 1;\n"
    "  if (want > ST_TM_RL_PARENTS_MAX) want = ST_TM_RL_PARENTS_MAX;\n"
    "  uint16_t created = 0;\n"
    "  for (uint16_t k = 0; k < want; k++) {\n"
    "    node_id = ST_ROOT_NODE_ID + qg_level + k;\n"
    "    ret = rte_tm_node_add(port_id, node_id, parent_id, 0, 1, qg_level, &np, &error);\n"
    "    if (ret < 0) {\n"
    "      if (created == 0) {\n"
    "        err(\"%s(%d), first QG node %u add error: (%d)%s\\n\", __func__, port, node_id, ret,\n"
    "            mt_string_safe(error.message));\n"
    "        return ret;\n"
    "      }\n"
    "      warn(\"%s(%d), QG fan-out capped at %u parents (%u leaves) by HW: (%d)%s\\n\", __func__,\n"
    "           port, created, created * ST_TM_QG_FANOUT, ret, mt_string_safe(error.message));\n"
    "      break;\n"
    "    }\n"
    "    created++;\n"
    "  }\n"
    "  inf->tx_rl_nb_parents = created;\n"
    "  info(\"%s(%d), RL branched tree: %u QG parents, capacity %u leaves\\n\", __func__, port,\n"
    "       created, created * ST_TM_QG_FANOUT);\n"
    "\n"
    "  inf->tx_rl_root_active = true;\n"
)
if OLD_INIT not in c:
    print("ERREUR: ancre 'dev_rl_init_nonleaf_nodes loop' introuvable dans %s" % FC, file=sys.stderr)
    sys.exit(1)
c = c.replace(OLD_INIT, NEW_INIT, 1)

# ---------------------------------------------------------------- 3) attaches feuille réparties
# Les DEUX sites (dev_init_ratelimit_all pour IAVF, dev_tx_queue_set_rl_rate pour le PF/à-la-demande)
# rattachent la feuille sous ST_TM_LAST_NONLEAF_NODE_ID_{PF,VF}. On remplace par le parent QG
# distribué : base (= LAST_NONLEAF_NODE_ID = premier id QG) + (file / ST_TM_QG_FANOUT).
#
# 3a) dev_init_ratelimit_all (variable de file : `q`)
OLD_LEAF_Q = (
    "    if (inf->drv_info.drv_type == MT_DRV_IAVF) {\n"
    "      ret = rte_tm_node_add(port_id, q, ST_TM_LAST_NONLEAF_NODE_ID_VF, 0, 1,\n"
    "                            ST_TM_NONLEAF_NODES_NUM_VF, &qp, &error);\n"
    "    } else {\n"
    "      ret = rte_tm_node_add(port_id, q, ST_TM_LAST_NONLEAF_NODE_ID_PF, 0, 1,\n"
    "                            ST_TM_NONLEAF_NODES_NUM_PF, &qp, &error);\n"
    "    }\n"
)
NEW_LEAF_Q = (
    "    " + MARK + " /* feuille répartie sur les nœuds QG (arbre branché) */\n"
    "    uint32_t qg_base = (inf->drv_info.drv_type == MT_DRV_IAVF)\n"
    "                           ? ST_TM_LAST_NONLEAF_NODE_ID_VF\n"
    "                           : ST_TM_LAST_NONLEAF_NODE_ID_PF;\n"
    "    uint32_t leaf_level = (inf->drv_info.drv_type == MT_DRV_IAVF)\n"
    "                              ? ST_TM_NONLEAF_NODES_NUM_VF\n"
    "                              : ST_TM_NONLEAF_NODES_NUM_PF;\n"
    "    uint32_t qg_slot = q / ST_TM_QG_FANOUT;\n"
    "    if (inf->tx_rl_nb_parents && qg_slot >= inf->tx_rl_nb_parents) {\n"
    "      err(\"%s(%d), q %u exceeds RL branched capacity (%u parents x %u)\\n\", __func__, port,\n"
    "          q, inf->tx_rl_nb_parents, ST_TM_QG_FANOUT);\n"
    "      return -EINVAL;\n"
    "    }\n"
    "    ret = rte_tm_node_add(port_id, q, qg_base + qg_slot, 0, 1, leaf_level, &qp, &error);\n"
)
if OLD_LEAF_Q not in c:
    print("ERREUR: ancre 'dev_init_ratelimit_all leaf add' introuvable dans %s" % FC, file=sys.stderr)
    sys.exit(1)
c = c.replace(OLD_LEAF_Q, NEW_LEAF_Q, 1)

# 3b) dev_tx_queue_set_rl_rate (variable de file : `queue`) — chemin PF/à-la-demande (le nôtre)
OLD_LEAF_QUEUE = (
    "    if (inf->drv_info.drv_type == MT_DRV_IAVF) {\n"
    "      ret = rte_tm_node_add(port_id, queue, ST_TM_LAST_NONLEAF_NODE_ID_VF, 0, 1,\n"
    "                            ST_TM_NONLEAF_NODES_NUM_VF, &qp, &error);\n"
    "    } else {\n"
    "      ret = rte_tm_node_add(port_id, queue, ST_TM_LAST_NONLEAF_NODE_ID_PF, 0, 1,\n"
    "                            ST_TM_NONLEAF_NODES_NUM_PF, &qp, &error);\n"
    "    }\n"
)
NEW_LEAF_QUEUE = (
    "    " + MARK + " /* feuille répartie sur les nœuds QG (arbre branché) */\n"
    "    uint32_t qg_base = (inf->drv_info.drv_type == MT_DRV_IAVF)\n"
    "                           ? ST_TM_LAST_NONLEAF_NODE_ID_VF\n"
    "                           : ST_TM_LAST_NONLEAF_NODE_ID_PF;\n"
    "    uint32_t leaf_level = (inf->drv_info.drv_type == MT_DRV_IAVF)\n"
    "                              ? ST_TM_NONLEAF_NODES_NUM_VF\n"
    "                              : ST_TM_NONLEAF_NODES_NUM_PF;\n"
    "    uint32_t qg_slot = queue / ST_TM_QG_FANOUT;\n"
    "    if (inf->tx_rl_nb_parents && qg_slot >= inf->tx_rl_nb_parents) {\n"
    "      err(\"%s(%d), q %u exceeds RL branched capacity (%u parents x %u)\\n\", __func__, port,\n"
    "          queue, inf->tx_rl_nb_parents, ST_TM_QG_FANOUT);\n"
    "      return -EINVAL;\n"
    "    }\n"
    "    ret = rte_tm_node_add(port_id, queue, qg_base + qg_slot, 0, 1, leaf_level, &qp, &error);\n"
)
if OLD_LEAF_QUEUE not in c:
    print("ERREUR: ancre 'dev_tx_queue_set_rl_rate leaf add' introuvable dans %s" % FC, file=sys.stderr)
    sys.exit(1)
c = c.replace(OLD_LEAF_QUEUE, NEW_LEAF_QUEUE, 1)

open(FC, "w").write(c)
print("patch TM hierarchy : appliqué")
