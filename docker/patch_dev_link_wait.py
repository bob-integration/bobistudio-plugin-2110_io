#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# PORTE À ~120s LE BUDGET D'ATTENTE DE LA DÉTECTION DE LIEN (dev_detect_link) SUR E810 100G DPDK.
#
# ── Pourquoi (banc DPDK 2026-07, dl360-1) ───────────────────────────────────────────────────────
# Sur E810 en 100G DPDK, dès que le port passe sous vfio-pci le lien doit ré-entraîner autoneg + FEC,
# ce qui prend ~60-90s côté PHY (le switch, lui, voit bien le 100G). libmtl, dans dev_detect_link
# (lib/src/dev/mt_dev.c), scrute rte_eth_link_get_nowait() en boucle puis abandonne : le budget
# STRICT vaut MT_DEV_LINK_RETRY_COUNT (3) × MT_DEV_LINK_POLL_COUNT (300) × MT_DEV_LINK_POLL_INTERVAL_MS
# (100ms) = 90s. Le lien montant souvent JUSTE après cette limite, la détection échoue :
#   dev_detect_link fail -5  →  mt_dev_create fail -5  →  mtl_init fail
# Le wrapper relance alors mtl_rx en boucle (crash-loop bruyant ~90s) jusqu'à ce qu'une relance
# tombe sur le lien enfin monté. Le lien EST bon — il faut juste ATTENDRE plus longtemps.
#
# ── Le fix ─────────────────────────────────────────────────────────────────────────────────────
# Approche la moins invasive : bumper la SEULE constante MT_DEV_LINK_POLL_COUNT de 300 à 400 dans
# lib/src/dev/mt_dev.h. Le budget strict passe à 3 × 400 × 100ms = 120s (marge au-delà des ~90s
# d'entraînement observés) SANS toucher la boucle C, l'intervalle de poll, ni le nombre de retries.
# Le mode « relaxed » (allow_down_init) utilise le même POLL_COUNT avec un intervalle de 10ms → il
# passe de 3s à 4s, impact négligeable. On met aussi à jour le commentaire de tête pour rester juste.

import sys

F = "lib/src/dev/mt_dev.h"
OLD = "#define MT_DEV_LINK_POLL_COUNT 300"
NEW = "#define MT_DEV_LINK_POLL_COUNT 400"

c = open(F).read()
if NEW in c:
    print("patch dev link wait : déjà appliqué"); sys.exit(0)

# Garde-fous : on vérifie qu'on cible bien le bloc de détection de lien attendu (valeurs upstream).
for needle in (OLD,
               "#define MT_DEV_LINK_RETRY_COUNT 3",
               "#define MT_DEV_LINK_POLL_INTERVAL_MS 100",
               "= 3 × 300 × 100ms = 90 seconds"):
    if needle not in c:
        print("patch dev link wait : ERREUR — ancre '%s' introuvable" % needle)
        sys.exit(1)

c = c.replace(OLD, NEW)
# Le commentaire de tête chiffre le budget : le garder synchrone (sinon il ment).
c = c.replace("= 3 × 300 × 100ms = 90 seconds",
              "= 3 × 400 × 100ms = 120 seconds (bobi.studio : E810 100G DPDK ~60-90s d'entraînement autoneg+FEC)")

open(F, "w").write(c)
print("patch dev link wait : OK (MT_DEV_LINK_POLL_COUNT 300->400, budget strict 90s->120s)")
