#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# Bug : en AF-XDP NATIF, les mbufs ANC (st40) arrivent avec rte_mbuf->port = UINT16_MAX (non
# renseigné par le chemin XSK), alors que st40_pipeline_rx exige pkt->port == port_id physique
# enregistré → CHAQUE paquet ANC est jeté (« rx_st40p_rtp_ready: drop pkt: unmapped port_id 65535 »)
# → 0 trame ANC, timecode RP188 jamais reçu. st20/st30 ne font pas ce contrôle (mode trame) → seul
# l'ANC est touché. En MONO-PORT (num_port==1) le paquet vient forcément du seul port → on retombe
# sur le port 0 au lieu de jeter (le contrôle ne sert qu'au dé-doublonnage 2022-7 multi-port).
#
# Idempotent + fail-fast : si l'ancre n'est pas trouvée (source MTL changée), on ÉCHOUE le build.
import sys

F = "lib/src/st2110/pipeline/st40_pipeline_rx.c"
src = open(F).read()

NEW_GUARD = "/* bobi.studio: AF-XDP mono-port fallback */"
if NEW_GUARD in src:
    print("patch st40 AF-XDP port : déjà appliqué"); sys.exit(0)

OLD = (
    "  if (s_port < 0 || phy_port >= MTL_PORT_MAX) {\n"
    "    warn(\"%s(%d), drop pkt: unmapped port_id %u\\n\", __func__, ctx->idx, pkt_port_id);\n"
    "    st40_rx_put_mbuf(ctx->transport, mbuf);\n"
    "    return -EIO;\n"
    "  }"
)
NEW = (
    "  if (s_port < 0 || phy_port >= MTL_PORT_MAX) {\n"
    "    " + NEW_GUARD + "\n"
    "    if (ctx->ops.port.num_port <= 1 && ctx->port_map[0] < MTL_PORT_MAX) {\n"
    "      s_port = 0;\n"
    "      phy_port = ctx->port_map[0];\n"
    "    } else {\n"
    "      warn(\"%s(%d), drop pkt: unmapped port_id %u\\n\", __func__, ctx->idx, pkt_port_id);\n"
    "      st40_rx_put_mbuf(ctx->transport, mbuf);\n"
    "      return -EIO;\n"
    "    }\n"
    "  }"
)

if OLD not in src:
    print("ERREUR: ancre st40_pipeline_rx introuvable (source MTL modifiée ?)", file=sys.stderr)
    sys.exit(1)
open(F, "w").write(src.replace(OLD, NEW, 1))
print("patch st40 AF-XDP port : appliqué")
