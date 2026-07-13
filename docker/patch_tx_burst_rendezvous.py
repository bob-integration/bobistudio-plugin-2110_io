#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
# 5ᵉ patch de la famille « commit RL tue des sessions vives ».
#
# ⚠⚠ VERDICT DE BANC (2026-07-13, moteur 140, cold-batch 6 TX, images instrumentées avant/après
# avec sonde rte_mempool_avail_count par session) : le rendez-vous NE SUPPRIME PAS l'épuisement
# des mempools. Avant : 154 × `build ret -207`, pools à 3/1792. Après : 173 × -207, pools à
# 0-3/1792, ZÉRO timeout de rendez-vous. ⇒ l'hypothèse « burst en vol écrasé par le memset du
# stop » est INFIRMÉE comme source dominante de la fuite. Le patch est CONSERVÉ (correct, coût
# 2 atomiques/burst, ferme une vraie fenêtre de course) mais ce n'est PAS le fix racine.
# La vraie piste, mise au jour par le 2ᵉ correctif de ce fichier : `ICE_DRIVER: ice_dev_start():
# fail to start Tx queue 15` — après le commit, le port NE REDÉMARRE PAS (ret -5, 6 tentatives).
# TX ne draine plus ⇒ les mbufs restent dans les rings MTL ⇒ pool vide ⇒ -207. Chantier ouvert.
#
# Ce fichier porte DEUX correctifs distincts sur la MÊME fonction (dev_tx_queue_set_rl_rate,
# mt_dev.c) et le même flag (inf->resetting), trouvés par deux investigations parallèles
# 2026-07-13 — testables et lisibles séparément dans le diff (MARK différent chacun) :
#   1. « TX burst rendezvous » (sections 1-3 ci-dessous) : empêche un burst DÉJÀ ENGAGÉ de
#      perdre ses mbufs quand le commit stoppe le port (fuite silencieuse de mempool).
#   2. « commit port-restart guard » (fin de la section 3) : empêche resetting de repasser à
#      false quand le restart de port INTERNE au commit driver échoue silencieusement sous
#      rafale (landmine « PTP not connected définitif » après une tempête de commits).
#
# ── Cause racine (diagnostiquée, cf. mémoire rl-commit-mempool-exhaustion-silent-tx-death) ────
# Sur le PMD ice en pacing RL, créer/modifier une session TX (dev_tx_queue_set_rl_rate, mt_dev.c)
# appelle rte_tm_hierarchy_commit → ice_hierarchy_commit (ice_tm.c) → rte_eth_dev_stop du PORT
# ENTIER → ice_tx_queue_stop de TOUTES les queues → ci_txq_release_mbufs (common/tx.h) libère le
# sw_ring PUIS fait un memset INCONDITIONNEL (ice_reset_tx_queue remet txe[i].mbuf=NULL partout).
#
# RACE TOCTOU : inf->resetting (mt_dev.c:745, cf. patch_rx_resetting_guard) est posé par le
# thread de CONFIG SANS rendez-vous avec les threads transmitter/receiver. La garde existante
# (patch_rx_resetting_guard) empêche seulement un NOUVEAU burst de démarrer une fois resetting
# vu à true — elle ne fait RIEN pour un burst DÉJÀ ENGAGÉ (rte_eth_tx_burst en cours d'écriture
# de pointeurs mbuf dans le sw_ring) au moment où le thread de config bascule resetting puis
# stoppe le port. Ces mbufs écrits tardivement sont écrasés par le memset SANS free = fuite pure
# (pool de 2047 mbufs vidé progressivement, ~O(512 desc)/commit, cumulatif sur les commits
# successifs d'un cold-batch) → rte_pktmbuf_alloc_bulk échoue pour toujours (build ret -207)
# → le hang detector natif (video_trs_burst_fail) ne vit que dans le chemin burst, jamais
# atteint puisqu'un builder en échec d'alloc ne construit aucun paquet → mort SILENCIEUSE,
# invisible aussi du filet builder (patch_tx_builder_famine_recovery, étage 0 TX_LAYOUTS.md)
# qui la guérit après coup mais ne l'empêche pas.
#
# ── Le patch : rendez-vous burst/config par compteur atomique per-port ─────────────────────────
# `bobi_burst_inflight` (rte_atomic32_t, ajouté à struct mt_interface juste après `resetting`,
# mt_main.h) compte les bursts EN COURS sur le port, RX et TX confondus (mt_rxq_burst ET
# mt_txq_burst sont chacun le point UNIQUE de choke de leur datapath — cf. patch_rx_resetting_guard
# — donc couvrent déjà vidéo/audio/ANC/CNI/PTP côté RX et vidéo/audio/ANC/sys côté TX sans autre
# modif). Protocole (mt_queue.c), par burst :
#   1. incrémenter bobi_burst_inflight (ANNONCER l'intention) ;
#   2. relire resetting ; si vrai, décrémenter et retourner 0 sans toucher le port (comme avant) ;
#   3. sinon appeler entry->burst(), puis décrémenter.
# Cet ordre (incrément AVANT la lecture de resetting) est le point clé : rte_atomic32_{inc,set,
# read} sont des RMW pleinement barrées (lock prefix x86 via les builtins __sync de DPDK), donc
# sequentially consistent entre elles. Si le burst a incrémenté avant que le thread de config ne
# lise le compteur, ce dernier le verra forcément (>=1) — qu'il ait lui-même déjà posé resetting
# ou pas encore : aucune fenêtre où le burst peut s'engager sans être vu.
# Ct̂é config (dev_tx_queue_set_rl_rate, mt_dev.c), APRÈS `resetting = true` et AVANT le commit :
# spin-wait borné (rte_pause(), même idiome que mt_handle_guard.h) sur bobi_burst_inflight == 0,
# timeout 80 ms (log warn() si dépassé — on part quand même au commit plutôt que de risquer un
# deadlock si un lcore de burst est lui-même bloqué ; 80 ms est très supérieur à la durée d'un
# burst normal, largement dans la marge mesurée du commit lui-même ~100 ms-1 s).
#
# Portée : RX ET TX (le port-stop du commit arrête les deux datapaths — ci_txq_release_mbufs
# est le mécanisme identifié pour TX, mais un burst RX en vol pendant le stop/reset des rx queues
# n'est pas davantage garanti sûr ; le rendez-vous protège les deux pour le coût de 2 atomiques
# supplémentaires par burst RX, chemin déjà dominé par des I/O DPDK bien plus coûteuses).
#
# Complète (ne remplace pas) : patch_rx_resetting_guard (portait le port-stop, segfault→0) et
# patch_tx_builder_famine_recovery (filet de 2ᵉ ligne pour les cas résiduels — avec ce patch, il
# ne devrait plus jamais se déclencher au cold-batch normal ; laissé en place par prudence).
#
# Idempotent + fail-fast : ancre introuvable (source MTL changée, OU patch_rx_resetting_guard pas
# appliqué avant celui-ci — ordre requis dans le Dockerfile) ⇒ échec du build.
import sys

MARK = "/* bobi.studio: TX burst rendezvous */"
RENDEZVOUS_TIMEOUT_NS = "80000000ULL"  # 80 ms (cf. en-tête : marge large sur un burst normal)

# ================================================================== 1) mt_main.h : compteur per-port
FH = "lib/src/mt_main.h"
h = open(FH).read()

if MARK in h:
    print("patch TX burst rendezvous : déjà appliqué (%s)" % FH)
else:
    OLD_H = (
        "  uint32_t status;                        /* MT_IF_STAT_* */\n"
        "  /* The port is temporarily off, e.g. during rte_tm_hierarchy_commit */\n"
        "  rte_atomic32_t resetting;\n"
    )
    NEW_H = (
        "  uint32_t status;                        /* MT_IF_STAT_* */\n"
        "  /* The port is temporarily off, e.g. during rte_tm_hierarchy_commit */\n"
        "  rte_atomic32_t resetting;\n"
        "  " + MARK + "\n"
        "  /* Rendez-vous burst/config : nb de bursts RX+TX en vol sur ce port (mt_queue.c).\n"
        "   * Le thread de config attend qu'il retombe à 0 après avoir posé resetting=true et\n"
        "   * AVANT rte_tm_hierarchy_commit, pour ne jamais stopper le port pendant qu'un burst\n"
        "   * écrit encore dans le sw_ring (sinon fuite mbufs silencieuse au memset du stop). */\n"
        "  rte_atomic32_t bobi_burst_inflight;\n"
    )
    if OLD_H not in h:
        print("ERREUR: ancre 'struct mt_interface (status/resetting)' introuvable dans %s "
              "(source MTL modifiée ?)" % FH, file=sys.stderr)
        sys.exit(1)
    h = h.replace(OLD_H, NEW_H, 1)
    open(FH, "w").write(h)
    print("patch TX burst rendezvous : appliqué (%s)" % FH)

# ======================================== 2) mt_queue.c : compteur autour des 2 points de choke
# Ancres = l'état APRÈS patch_rx_resetting_guard (doit s'exécuter AVANT celui-ci dans le
# Dockerfile) : mt_rxq_burst/mt_txq_burst y lisent déjà `resetting` via entry->port.
FC = "lib/src/datapath/mt_queue.c"
c = open(FC).read()

if MARK in c:
    print("patch TX burst rendezvous : déjà appliqué (%s)" % FC)
else:
    OLD_RXB = (
        "uint16_t mt_rxq_burst(struct mt_rxq_entry* entry, struct rte_mbuf** rx_pkts,\n"
        "                      const uint16_t nb_pkts) {\n"
        "  /* bobi.studio: RX/TX resetting guard */\n"
        "  /* Port temporairement stoppé pendant rte_tm_hierarchy_commit (attache shaper RL TX) :\n"
        "   * poller rte_eth_rx_burst sur un port not-ready segfault. On ne reçoit rien le temps du\n"
        "   * commit ; le burst reprend seul ensuite. */\n"
        "  if (rte_atomic32_read(&mt_if(entry->parent, entry->port)->resetting)) return 0;\n"
        "  return entry->burst(entry, rx_pkts, nb_pkts);\n"
        "}\n"
    )
    NEW_RXB = (
        "uint16_t mt_rxq_burst(struct mt_rxq_entry* entry, struct rte_mbuf** rx_pkts,\n"
        "                      const uint16_t nb_pkts) {\n"
        "  /* bobi.studio: RX/TX resetting guard */\n"
        "  " + MARK + "\n"
        "  /* Rendez-vous : annoncer l'intention AVANT de relire resetting (cf. en-tête du\n"
        "   * patch pour la preuve d'ordonnancement). */\n"
        "  struct mt_interface* bobi_inf = mt_if(entry->parent, entry->port);\n"
        "  rte_atomic32_inc(&bobi_inf->bobi_burst_inflight);\n"
        "  /* Port temporairement stoppé pendant rte_tm_hierarchy_commit (attache shaper RL TX) :\n"
        "   * poller rte_eth_rx_burst sur un port not-ready segfault. On ne reçoit rien le temps du\n"
        "   * commit ; le burst reprend seul ensuite. */\n"
        "  if (rte_atomic32_read(&bobi_inf->resetting)) {\n"
        "    rte_atomic32_dec(&bobi_inf->bobi_burst_inflight);\n"
        "    return 0;\n"
        "  }\n"
        "  uint16_t bobi_rx_ret = entry->burst(entry, rx_pkts, nb_pkts);\n"
        "  rte_atomic32_dec(&bobi_inf->bobi_burst_inflight);\n"
        "  return bobi_rx_ret;\n"
        "}\n"
    )
    OLD_TXB = (
        "uint16_t mt_txq_burst(struct mt_txq_entry* entry, struct rte_mbuf** tx_pkts,\n"
        "                      uint16_t nb_pkts) {\n"
        "  /* bobi.studio: RX/TX resetting guard */\n"
        "  /* Même flag PORT-WIDE que la RX : émettre via rte_eth_tx_burst sur un port stoppé (commit\n"
        "   * RL en cours) déclenche « tx_pkt_burst for not ready port » puis un fatal-error/segfault.\n"
        "   * On n'émet rien le temps du commit (les mbufs restent en file, drainés au redémarrage). */\n"
        "  if (rte_atomic32_read(&mt_if(entry->parent, entry->port)->resetting)) return 0;\n"
        "  return entry->burst(entry, tx_pkts, nb_pkts);\n"
        "}\n"
    )
    NEW_TXB = (
        "uint16_t mt_txq_burst(struct mt_txq_entry* entry, struct rte_mbuf** tx_pkts,\n"
        "                      uint16_t nb_pkts) {\n"
        "  /* bobi.studio: RX/TX resetting guard */\n"
        "  " + MARK + "\n"
        "  /* Rendez-vous : annoncer l'intention AVANT de relire resetting. Ce compteur est ce\n"
        "   * qu'attend dev_tx_queue_set_rl_rate (mt_dev.c) avant le commit — sans lui, un burst\n"
        "   * déjà engagé ICI peut encore écrire dans le sw_ring APRÈS que le commit ait fait son\n"
        "   * scan de libération : ces mbufs sont alors écrasés par le memset du stop SANS free,\n"
        "   * fuite silencieuse du mempool (cf. mémoire rl-commit-mempool-exhaustion-silent-tx-death). */\n"
        "  struct mt_interface* bobi_inf = mt_if(entry->parent, entry->port);\n"
        "  rte_atomic32_inc(&bobi_inf->bobi_burst_inflight);\n"
        "  /* Même flag PORT-WIDE que la RX : émettre via rte_eth_tx_burst sur un port stoppé (commit\n"
        "   * RL en cours) déclenche « tx_pkt_burst for not ready port » puis un fatal-error/segfault.\n"
        "   * On n'émet rien le temps du commit (les mbufs restent en file, drainés au redémarrage). */\n"
        "  if (rte_atomic32_read(&bobi_inf->resetting)) {\n"
        "    rte_atomic32_dec(&bobi_inf->bobi_burst_inflight);\n"
        "    return 0;\n"
        "  }\n"
        "  uint16_t bobi_tx_ret = entry->burst(entry, tx_pkts, nb_pkts);\n"
        "  rte_atomic32_dec(&bobi_inf->bobi_burst_inflight);\n"
        "  return bobi_tx_ret;\n"
        "}\n"
    )
    for name, old in (("mt_rxq_burst (post rx_resetting_guard)", OLD_RXB),
                      ("mt_txq_burst (post rx_resetting_guard)", OLD_TXB)):
        if old not in c:
            print("ERREUR: ancre '%s' introuvable dans %s — patch_rx_resetting_guard doit "
                  "s'appliquer AVANT celui-ci, ou source MTL modifiée ?" % (name, FC),
                  file=sys.stderr)
            sys.exit(1)
    c = c.replace(OLD_RXB, NEW_RXB, 1)
    c = c.replace(OLD_TXB, NEW_TXB, 1)
    open(FC, "w").write(c)
    print("patch TX burst rendezvous : appliqué (%s)" % FC)

# ============================================ 3) mt_dev.c : spin-wait avant le commit TM live
FD = "lib/src/dev/mt_dev.c"
d = open(FD).read()

if MARK in d:
    print("patch TX burst rendezvous : déjà appliqué (%s)" % FD)
else:
    # Assure rte_pause() disponible (idiome déjà utilisé dans mt_handle_guard.h du même arbre).
    if "#include <rte_pause.h>" not in d:
        OLD_INC = "#include \"mt_dev.h\"\n"
        if OLD_INC not in d:
            print("ERREUR: ancre include 'mt_dev.h' introuvable dans %s (source MTL modifiée ?)"
                  % FD, file=sys.stderr)
            sys.exit(1)
        d = d.replace(OLD_INC, OLD_INC + "#include <rte_pause.h>\n", 1)

    # Seul site où `resetting` est posé avant un commit TM live (dev_init_ratelimit_all,
    # ~l.679, tourne au dev_start AVANT toute session TX — vérifié, pas de rendez-vous requis
    # là ; c'est le seul autre appelant de rte_tm_hierarchy_commit dans ce fichier).
    OLD_D = (
        "  rte_atomic32_set(&inf->resetting, true);\n"
        "  mt_pthread_mutex_lock(&inf->vf_cmd_mutex);\n"
        "  ret = rte_tm_hierarchy_commit(port_id, 1, &error);\n"
        "  mt_pthread_mutex_unlock(&inf->vf_cmd_mutex);\n"
    )
    NEW_D = (
        "  rte_atomic32_set(&inf->resetting, true);\n"
        "  " + MARK + "\n"
        "  /* Rendez-vous : attendre que tout burst RX/TX déjà engagé sur ce port (annoncé via\n"
        "   * bobi_burst_inflight, mt_queue.c) se termine AVANT le commit — sinon ce commit peut\n"
        "   * stopper le port (ice_tx_queue_stop → memset du sw_ring sans free) pendant qu'un\n"
        "   * burst écrit encore des mbufs, fuite silencieuse du mempool d'une session TX déjà\n"
        "   * vivante (cf. mémoire rl-commit-mempool-exhaustion-silent-tx-death). Borné : un lcore\n"
        "   * de burst bloqué ne doit pas figer indéfiniment la création/modif d'une AUTRE session. */\n"
        "  {\n"
        "    uint64_t bobi_wait_start = mt_get_tsc(inf->parent);\n"
        "    while (rte_atomic32_read(&inf->bobi_burst_inflight) > 0) {\n"
        "      if ((mt_get_tsc(inf->parent) - bobi_wait_start) > " + RENDEZVOUS_TIMEOUT_NS + ") {\n"
        "        warn(\"%s(%d), bobi: burst rendezvous timeout (80 ms), committing anyway\\n\",\n"
        "             __func__, port);\n"
        "        break;\n"
        "      }\n"
        "      rte_pause();\n"
        "    }\n"
        "  }\n"
        "  mt_pthread_mutex_lock(&inf->vf_cmd_mutex);\n"
        "  ret = rte_tm_hierarchy_commit(port_id, 1, &error);\n"
        "  mt_pthread_mutex_unlock(&inf->vf_cmd_mutex);\n"
    )
    if d.count(OLD_D) != 1:
        print("ERREUR: ancre 'dev_tx_queue_set_rl_rate (resetting/commit)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FD, d.count(OLD_D)), file=sys.stderr)
        sys.exit(1)
    d = d.replace(OLD_D, NEW_D, 1)

    # 3b) Bug DISTINCT trouvé sur la même fonction (investigation croisée 2026-07-13) :
    # `resetting` était remis à false INCONDITIONNELLEMENT juste après le commit, que celui-ci ait
    # réussi ou non. Or ice_hierarchy_commit (driver ice, hors de cet arbre — script/build_dpdk.sh)
    # fait en interne un stop → commit TM → start ; sous rafale de commits (cold-batch), le start
    # interne peut échouer (aller-retour admin-queue firmware) SANS retry côté driver. Le port
    # reste alors réellement `dev_started=0` alors que resetting redevenait déjà false : les gardes
    # RX/TX (patch_rx_resetting_guard) ne lisent QUE resetting, donc cessent de protéger → burst
    # sur port non prêt → RX à 0 pour toujours → PTP not connected DÉFINITIF (mt_ptp.c) → moteur
    # muet, seul un restart complet du daemon en sort (landmine « perte du chemin RX PTP après
    # tempête », DISTINCTE de la fuite mbufs visée par le rendez-vous ci-dessus — même fonction,
    # même flag, deux bugs différents).
    #
    # Fix : ne lever resetting que si le port est RÉELLEMENT démarré. `rte_eth_devices[]` (accès
    # direct au champ interne dev_started) s'est révélé NON visible depuis du code applicatif sur
    # le DPDK vendoré ici (26.03 patché MTL — erreur de build réelle, `rte_eth_devices` undeclared
    # dans mt_dev.c ; ce tableau n'est plus exposé hors de l'arbre ethdev/PMD dans cette version).
    # On s'appuie donc UNIQUEMENT sur l'API publique : rte_eth_dev_start() est documenté idempotent
    # côté DPDK — si le port est déjà démarré, il logue et retourne 0 SANS req matérielle — l'appeler
    # inconditionnellement après le commit est donc un moyen sûr et public de faire foi de l'état
    # réel (ret 0 ⇒ port confirmé up, que ce soit parce qu'il l'était déjà ou parce que cet appel
    # vient de le (re)démarrer). Sinon (ret != 0), retenter borné (5 tentatives, 20 ms d'écart, cf.
    # mt_sleep_ms déjà utilisé dans cet arbre) — PAS dev_start_port/sa reconfig complète, qui suppose
    # un port proprement stoppé et rejouerait toute l'init rx/tx queue/mempool, risqué à chaud. Si
    # les 5 échouent : resetting RESTE à true — choix délibéré (option 3 de l'investigation
    # croisée) : un port qui reste "resetting" garde les gardes RX/TX actives (bursts skippés
    # proprement, moteur muet mais SANS toucher un port mort) plutôt que de laisser resetting=false
    # taper sur un port non prêt. Récupérable par un commit ultérieur (qui retente) ou un restart
    # contrôlé du daemon ; jamais par un accès direct qui referait le SIGSEGV que
    # patch_rx_resetting_guard visait à éliminer.
    MARK2 = "/* bobi.studio: commit port-restart guard */"
    OLD_D2 = (
        "  rte_atomic32_set(&inf->resetting, false);\n"
        "  if (ret < 0) {\n"
        "    err(\"%s(%d), commit error (%d)%s\\n\", __func__, port, ret,\n"
        "        mt_string_safe(error.message));\n"
        "    return ret;\n"
        "  }\n"
    )
    NEW_D2 = (
        "  " + MARK2 + "\n"
        "  /* rte_eth_dev_start() est idempotent (no-op, retourne 0) si le port est déjà démarré :\n"
        "   * l'appeler ici fait foi de l'état RÉEL du port (public API, cf. patch en-tête) plutôt\n"
        "   * que de croire aveuglément resetting=false comme le faisait le code d'origine. */\n"
        "  {\n"
        "    int bobi_restart_ret = -1;\n"
        "    for (int bobi_try = 0; bobi_try < 5; bobi_try++) {\n"
        "      bobi_restart_ret = rte_eth_dev_start(port_id);\n"
        "      if (!bobi_restart_ret) break;\n"
        "      mt_sleep_ms(20);\n"
        "    }\n"
        "    if (!bobi_restart_ret) {\n"
        "      if (ret < 0) /* commit TM lui-même en échec, port confirmé up quand même */\n"
        "        warn(\"%s(%d), bobi: commit error but port confirmed started\\n\", __func__, port);\n"
        "      rte_atomic32_set(&inf->resetting, false);\n"
        "    } else {\n"
        "      err(\"%s(%d), bobi: port did NOT restart after commit (ret %d) + 5 retries -- left \"\n"
        "          \"resetting=true (engine muted but safe, bursts skipped) until next commit or \"\n"
        "          \"full daemon restart\\n\", __func__, port, bobi_restart_ret);\n"
        "    }\n"
        "  }\n"
        "  if (ret < 0) {\n"
        "    err(\"%s(%d), commit error (%d)%s\\n\", __func__, port, ret,\n"
        "        mt_string_safe(error.message));\n"
        "    return ret;\n"
        "  }\n"
    )
    if d.count(OLD_D2) != 1:
        print("ERREUR: ancre 'dev_tx_queue_set_rl_rate (resetting=false/commit error)' non-unique "
              "ou introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FD, d.count(OLD_D2)), file=sys.stderr)
        sys.exit(1)
    d = d.replace(OLD_D2, NEW_D2, 1)

    open(FD, "w").write(d)
    print("patch TX burst rendezvous : appliqué (%s)" % FD)
