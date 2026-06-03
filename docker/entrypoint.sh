#!/bin/sh
# SPDX-License-Identifier: GPL-3.0-or-later
# Entrypoint du conteneur MTL. Passe les arguments à mtl_rx, mais prépare d'abord
# l'environnement selon le PMD demandé :
#   - af_xdp / kernel : monte bpffs (si absent) ; af_xdp démarre en plus MtlManager
#     (requis par le backend AF_XDP de MTL pour charger le prog XDP + coordonner les lcores).
#   - dpdk (défaut) : rien de spécial (hugepages + /dev/vfio fournis par `docker run`).
# Usage : docker run ... <image> --pmd af_xdp --iface ens1f0np0 --mcast ... [args mtl_rx]
set -e

ARGS="$*"
case "$ARGS" in
  *af_xdp*|*kernel*)
    mountpoint -q /sys/fs/bpf 2>/dev/null || mount -t bpf bpf /sys/fs/bpf 2>/dev/null || true
    ;;
esac
case "$ARGS" in
  *af_xdp*)
    # MtlManager doit tourner pendant toute la vie du conteneur ; en fond.
    MtlManager >/var/log/mtl_manager.log 2>&1 &
    # petite attente que le socket /var/run/imtl soit prêt
    i=0; while [ ! -S /var/run/imtl/mtl_manager.sock ] && [ $i -lt 20 ]; do sleep 0.2; i=$((i+1)); done
    ;;
esac

exec mtl_rx "$@"
