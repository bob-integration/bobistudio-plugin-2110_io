#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# EXPORTE LE GRANDMASTER PTP interne de libmtl (mt_bobi_ptp_gm) POUR LE SDP TX.
#
# ── Pourquoi (banc G1 2026-07-11) ──────────────────────────────────────────────────────────────
# Le SDP émis d'un sender 2110 doit annoncer l'horloge de référence :
#   a=ts-refclk:ptp=IEEE1588-2008:<gmIdentity>:<domain>
# L'orchestrateur la construit normalement en lisant le grandmaster via `pmc` sur le ptp4l KERNEL
# (app/ptp.refclk_for_host). Or sur le socle full-PF DPDK, le port est en vfio-pci : plus de ptp4l
# kernel, le PTP est fait DANS libmtl (ENGINE_PTP=libmtl). `pmc` ne voit rien → ts-refclk OMIS du
# SDP (non conforme ; pénalise l'interop multi-éditeurs). Or libmtl EST esclave PTPv2 et connaît le
# grandmaster (Announce) : `struct mt_ptp_impl.master_port_id.clock_identity` (8 octets = gmIdentity),
# `t1_domain_number` (domaine reçu), `master_utc_offset`.
#
# ── Le fix ─────────────────────────────────────────────────────────────────────────────────────
# Ajouter à mt_ptp.c une fonction NON statique (liée dans libmtl.so, appelée par mtl_rx via extern) :
# `mt_bobi_ptp_gm(impl, port, out_id8, out_domain, out_utc)` → remplit l'identité GM (8 octets), le
# domaine et l'offset UTC, et renvoie true SSI le PTP interne est actif + master initialisé + lockÉ
# (sinon le SDP ne doit pas annoncer un GM non verrouillé). Ajout pur en fin de fichier.

import sys

F = "lib/src/mt_ptp.c"
MARK = "bobi.studio: export grandmaster PTP"

c = open(F).read()
if MARK in c:
    print("patch PTP GM export : déjà appliqué"); sys.exit(0)

for needle in ("struct mt_ptp_impl* ptp = mt_get_ptp(impl, port);",
               "master_port_id", "master_initialized", "t1_domain_number"):
    if needle not in c:
        print("patch PTP GM export : ERREUR — ancre '%s' introuvable" % needle)
        sys.exit(1)

c += """
/* %s : identité du grandmaster auquel le PTP interne est asservi (pour a=ts-refclk:ptp du SDP TX
 * quand le ptp4l kernel est absent — socle DPDK). Remplit out_id8[8] (clock identity), out_domain,
 * out_utc. true SSI actif + master initialisé (Announce reçu → GM connu). ⚠ NE PAS gater sur
 * ptp->locked : c'est le lock SERVO (delta < 100 ns continu), distinct de la précision du system
 * clock ; l'identité du GM est valide dès qu'on a reçu son Announce, indépendamment de la précision
 * instantanée — le SDP doit annoncer le GM de référence (pratique standard, cf. ptp4l/pmc PARENT).
 * Appelé depuis mtl_rx via extern (symbole exporté). */
bool mt_bobi_ptp_gm(struct mtl_main_impl* impl, enum mtl_port port,
                    uint8_t* out_id8, int* out_domain, int* out_utc) {
  struct mt_ptp_impl* ptp = mt_get_ptp(impl, port);
  if (!ptp || !ptp->active || !ptp->master_initialized) return false;
  if (out_id8)
    for (int i = 0; i < 8; i++) out_id8[i] = ptp->master_port_id.clock_identity.id[i];
  if (out_domain) *out_domain = (int)ptp->t1_domain_number;
  if (out_utc)    *out_utc    = (int)ptp->master_utc_offset;
  return true;
}
""" % MARK

open(F, "w").write(c)
print("patch PTP GM export : OK (mt_bobi_ptp_gm ajouté à mt_ptp.c)")
