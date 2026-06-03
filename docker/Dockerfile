# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Image runtime MTL (Media Transport Library) pour Bobi.Studio — réception/émission ST 2110.
#
# Codifie la recette validée manuellement sur pvemxl (cf. mémoires mtl-e810-vfio-path /
# mtl-pivot-docker-pf-dpdk). Déploiement cible : E810, PF en DPDK (ou AF_XDP en repli IGMP),
# sous Docker, sur un hôte noyau LTS. Le NIC est préparé SUR L'HÔTE (vfio + DDP) ; ce conteneur
# ne fait que tourner l'app MTL avec /dev/vfio + hugepages passés par `docker run`.
#
# ⚠️ NON BUILD/TESTÉ off-site — à builder sur le nœud cible (accès Internet pour le clone MTL).
# Build : docker build -t bobi-mtl:VER -f Dockerfile <contexte>
#   contexte doit contenir : mtl_rx.c, entrypoint.sh, et (optionnel) ice_comms-*.pkg (DDP, pour
#   le mode PF-DPDK-in-container ; en mode AF_XDP le DDP est chargé par le noyau hôte).
#
# Base Debian 13 (trixie) = ce sur quoi DPDK/libmtl 26.01 ont été validés.
FROM debian:trixie

ARG MTL_REF=main
ENV DEBIAN_FRONTEND=noninteractive
ENV PKG_CONFIG_PATH=/usr/local/lib/x86_64-linux-gnu/pkgconfig:/usr/local/lib/pkgconfig

# 1) Toolchain + deps build (DPDK/libmtl) + AF_XDP (libxdp/libbpf) + BPF (clang/llvm/gcc-multilib).
RUN apt-get update && apt-get install -y --no-install-recommends \
      git build-essential meson ninja-build pkg-config python3-pyelftools \
      libnuma-dev libssl-dev libjson-c-dev libpcap-dev rdma-core libibverbs-dev \
      libcap-ng-dev m4 sudo unzip cmake systemtap-sdt-dev libgtest-dev libgmock-dev \
      libxdp-dev libbpf-dev libelf-dev clang llvm gcc-multilib \
      ca-certificates iproute2 ethtool wget curl \
 && rm -rf /var/lib/apt/lists/*

# Fix BPF/clang sur Debian multiarch : asm/ accessible (sinon mtl.xdp.c ne compile pas).
RUN ln -sf /usr/include/x86_64-linux-gnu/asm /usr/include/asm

# 2) Build DPDK patché MTL + libmtl. tests DPDK désactivés (inline, sinon fuit dans build.sh).
#    Le composant ld_preload/udp casse (signature ioctl) → ignoré ; gate = pkg-config mtl.
RUN git clone --depth 1 -b ${MTL_REF} https://github.com/OpenVisualCloud/Media-Transport-Library /src/MTL \
 && cd /src/MTL \
 && MTL_PREFIX_ARGS="-Dtests=false" ./script/build_dpdk.sh -f \
 && ./build.sh || true \
 && ldconfig \
 && pkg-config --exists mtl \
 && test -e /usr/local/lib/x86_64-linux-gnu/libmtl.so

# 3) MtlManager (requis pour AF_XDP) : meson clang BPF a besoin de l'include multiarch.
RUN cd /src/MTL/manager \
 && sed -i "s|'-target', 'bpf', '-c'|'-target', 'bpf', '-I/usr/include/x86_64-linux-gnu', '-c'|" meson.build \
 && meson setup build && ninja -C build && ninja -C build install \
 && ldconfig \
 # le prog XDP doit être à côté de xdp-dispatcher.o (chemin libxdp standard)
 && cp -f /usr/local/lib/bpf/mtl.xdp.o /usr/lib/x86_64-linux-gnu/bpf/ 2>/dev/null || true

# 4) Notre binaire de réception.
COPY mtl_rx.c /src/mtl_rx.c
RUN cc -O2 -o /usr/local/bin/mtl_rx /src/mtl_rx.c $(pkg-config --cflags --libs mtl) -lpthread -lm

# 5) DDP comms : NON copié ici. En AF_XDP/kernel le DDP est chargé par le driver ice de l'HÔTE.
#    Pour le mode PF-DPDK-in-container (plus tard), monter le .pkg via `docker run -v` à
#    /usr/local/lib/bpf/... ou /lib/firmware/intel/ice/ddp/ice.pkg (évite de versionner un binaire Intel).

# 6) Nettoyage de l'arbre source (les artefacts sont dans /usr/local).
RUN rm -rf /src/MTL

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
