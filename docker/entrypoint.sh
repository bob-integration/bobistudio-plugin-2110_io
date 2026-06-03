#!/bin/sh
# SPDX-License-Identifier: GPL-3.0-or-later
# Entrypoint du conteneur MTL. Deux modes :
#   - SANS argument (déploiement orchestré) : prépare l'env AF_XDP (bpffs + MtlManager) puis
#     exec le CONTRÔLEUR Python (:8080 métriques, :8081 nmos/subscribe, simu, lance mtl_rx à
#     l'activation NMOS). C'est le mode utilisé par le driver Docker de Bobi.Studio.
#   - AVEC arguments (test manuel) : prépare l'env selon le PMD demandé puis exec mtl_rx nu.
#     Ex : docker run ... <image> --pmd af_xdp --iface ens1f0np0 --mcast ... --shm ...
set -e

_prep_afxdp() {
  mountpoint -q /sys/fs/bpf 2>/dev/null || mount -t bpf bpf /sys/fs/bpf 2>/dev/null || true
  # MtlManager doit tourner pendant toute la vie du conteneur ; en fond.
  MtlManager >/var/log/mtl_manager.log 2>&1 &
  i=0; while [ ! -S /var/run/imtl/mtl_manager.sock ] && [ $i -lt 20 ]; do sleep 0.2; i=$((i+1)); done
}

if [ "$#" -eq 0 ]; then
  # Mode contrôleur (AF_XDP par défaut).
  _prep_afxdp
  exec python3 /usr/local/bin/controller.py
fi

# Mode mtl_rx nu (test manuel).
case "$*" in
  *af_xdp*|*kernel*) _prep_afxdp ;;
esac
exec mtl_rx "$@"
