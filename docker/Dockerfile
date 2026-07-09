# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Image runtime MTL (Media Transport Library) pour Bobi.Studio — réception/émission ST 2110.
#
# Codifie la recette validée manuellement sur node-1 (cf. mémoires mtl-e810-vfio-path /
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
#
# CHANTIER MXL — Phase 2 : cette image embarque désormais libmxl (SDK Linux Foundation, patché
# planar) ET bobimxl.py. mtl_rx.c écrit/lit des FLOWS MXL (plus de rings shm maison) ; controller.py
# importe bobimxl pour la simu/txgen. L'étage builder libmxl ci-dessous est IDENTIQUE à celui de
# plugins/_compute_runtime/Dockerfile → cache BuildKit partagé (pas de recompil vcpkg sur un nœud
# qui a déjà buildé bobi-compute).

# ---------------------------------------------------------------- étage builder : compiler libmxl
FROM debian:trixie-slim AS mxl-builder
ARG MXL_REF=v1.1.0-beta-1
ENV DEBIAN_FRONTEND=noninteractive
ENV VCPKG_ROOT=/opt/vcpkg
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates build-essential cmake ninja-build pkg-config \
      autoconf automake autoconf-archive libtool nasm bison flex libpcap-dev \
      libfabric-dev \
      curl zip unzip tar \
 && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/microsoft/vcpkg.git "$VCPKG_ROOT" \
 && "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics
RUN git clone --depth 1 --branch "${MXL_REF}" https://github.com/dmf-mxl/mxl.git /src/mxl
WORKDIR /src/mxl
RUN cmake -S . -B build -G Ninja \
      -DCMAKE_MAKE_PROGRAM=/usr/bin/ninja \
      -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ \
      -DBUILD_SHARED_LIBS=ON -DBUILD_TOOLS=ON \
      -DBUILD_DOCS=OFF -DBUILD_TESTS=OFF -DBUILD_UTILS=OFF \
      -DMXL_ENABLE_FABRICS_OFI=ON \
      -DCMAKE_INSTALL_PREFIX=/opt/mxl-install \
      || { echo "===== vcpkg-manifest-install.log ====="; \
           tail -n 120 /src/mxl/build/vcpkg-manifest-install.log 2>/dev/null; \
           echo "===== derniers logs de ports ====="; \
           for f in $(ls -t /opt/vcpkg/buildtrees/*/*-err.log 2>/dev/null | head -3); do \
             echo "--- $f ---"; tail -n 60 "$f"; done; false; }
COPY plugins/_compute_runtime/patches/mxl-planar-type.patch /tmp/mxl-planar-type.patch
RUN git apply --verbose /tmp/mxl-planar-type.patch \
 && cmake --build build -j"$(nproc)" \
 && cmake --install build

# ---------------------------------------------------------------- image finale : runtime MTL
FROM debian:trixie

# MTL_REF ÉPINGLÉ (chantier DPDK, 2026-07-06) : SHA de `main` au moment des builds verts
# 0.36→0.38 (tag amont le plus proche : v26.01). Les patchs bobi.studio ci-dessous
# (st40 AF-XDP, 2022-7 tx link drop) sont fail-fast contre CE SHA — toute montée de
# version = décision explicite + re-validation des patchs. Ne PAS remettre `main`.
ARG MTL_REF=32b1b4e937c834353536a9e84e04f2988754951a
ENV DEBIAN_FRONTEND=noninteractive
ENV PKG_CONFIG_PATH=/usr/local/lib/x86_64-linux-gnu/pkgconfig:/usr/local/lib/pkgconfig

# 1) Toolchain + deps build (DPDK/libmtl) + AF_XDP (libxdp/libbpf) + BPF (clang/llvm/gcc-multilib).
RUN apt-get update && apt-get install -y --no-install-recommends \
      git build-essential meson ninja-build pkg-config python3-pyelftools \
      libnuma-dev libssl-dev libjson-c-dev libpcap-dev rdma-core libibverbs-dev \
      libfabric1 \
      libcap-ng-dev m4 sudo unzip cmake systemtap-sdt-dev libgtest-dev libgmock-dev \
      libxdp-dev libbpf-dev libelf-dev clang llvm gcc-multilib \
      ca-certificates iproute2 ethtool wget curl \
      python3 python3-numpy python3-pil fonts-dejavu-core \
 && rm -rf /var/lib/apt/lists/*

# Fix BPF/clang sur Debian multiarch : asm/ accessible (sinon mtl.xdp.c ne compile pas).
RUN ln -sf /usr/include/x86_64-linux-gnu/asm /usr/include/asm

# 2) Build DPDK patché MTL + libmtl. tests DPDK désactivés (inline, sinon fuit dans build.sh).
#    Le composant ld_preload/udp casse (signature ioctl) → ignoré ; gate = pkg-config mtl.
#    Patch bobi.studio st40/AF-XDP (RX ANC : mbuf port=UINT16_MAX en AF-XDP natif → fallback mono-port)
#    appliqué APRÈS le clone, AVANT build.sh (qui compile libmtl depuis lib/src).
#    Patch bobi.studio 2022-7 hitless TX (un port AF_XDP au lien mort jette ses paquets comme
#    émis au lieu de figer la session entière — sinon un câble débranché tue tout le TX).
#    Patch bobi.studio RX resetting guard (RX+TX pacing RL même port : le commit TM du shaper RL
#    stoppe brièvement le port ; le burst RX doit respecter inf->resetting sinon SIGSEGV « rx_pkt_burst
#    for not ready port » — narrow matériel mono-port réparable).
#    Patch bobi.studio TM hierarchy (lève le mur des 8 files TX RL : arbre traffic-manager RAMIFIÉ
#    — plusieurs nœuds queue-group au lieu d'un seul — pour >8 senders RL/port ET création en rafale ;
#    nb_qps = P_parents × fan-out 8 côté PMD ice, jusqu'à MT_MAX_RL_ITEMS=128).
COPY patch_st40_afxdp_port.py /tmp/patch_st40_afxdp_port.py
COPY patch_afxdp_tx_link_drop.py /tmp/patch_afxdp_tx_link_drop.py
COPY patch_rx_resetting_guard.py /tmp/patch_rx_resetting_guard.py
COPY patch_tm_hierarchy.py /tmp/patch_tm_hierarchy.py
COPY patch_igmp_router_alert.py /tmp/patch_igmp_router_alert.py
COPY patch_ptp_mcast_flow.py /tmp/patch_ptp_mcast_flow.py
# MTL_REF est un SHA (pas une branche) → clone + fetch ciblé (GitHub sert les SHA joignables).
RUN mkdir -p /src/MTL && cd /src/MTL \
 && git init -q && git remote add origin https://github.com/OpenVisualCloud/Media-Transport-Library \
 && git fetch --depth 1 origin ${MTL_REF} \
 && git checkout -q FETCH_HEAD \
 && python3 /tmp/patch_st40_afxdp_port.py \
 && python3 /tmp/patch_afxdp_tx_link_drop.py \
 && python3 /tmp/patch_rx_resetting_guard.py \
 && python3 /tmp/patch_tm_hierarchy.py \
 && python3 /tmp/patch_igmp_router_alert.py \
 && python3 /tmp/patch_ptp_mcast_flow.py \
 && MTL_PREFIX_ARGS="-Dtests=false" ./script/build_dpdk.sh -f \
 && ./build.sh || true \
 && ldconfig \
 && pkg-config --exists mtl \
 && test -e /usr/local/lib/x86_64-linux-gnu/libmtl.so \
 # Assertions chemin DPDK/vfio (chantier narrow) : le build E810-only doit fournir le PMD
 # ice DPDK et le bus PCI (BDF/vfio). Échec de build immédiat si l'un manque.
 && test -n "$(find /usr/local/lib -name 'librte_net_ice*' | head -1)" \
 && test -n "$(find /usr/local/lib -name 'librte_bus_pci*' | head -1)"

# 3) MtlManager (requis pour AF_XDP) : meson clang BPF a besoin de l'include multiarch.
RUN cd /src/MTL/manager \
 && sed -i "s|'-target', 'bpf', '-c'|'-target', 'bpf', '-I/usr/include/x86_64-linux-gnu', '-c'|" meson.build \
 && meson setup build && ninja -C build && ninja -C build install \
 && ldconfig \
 # le prog XDP doit être à côté de xdp-dispatcher.o (chemin libxdp standard)
 && cp -f /usr/local/lib/bpf/mtl.xdp.o /usr/lib/x86_64-linux-gnu/bpf/ 2>/dev/null || true

# 3b) libmxl (depuis l'étage builder) : libs + headers (pour compiler mtl_rx) + outil mxl-info.
COPY --from=mxl-builder /opt/mxl-install/lib/     /usr/local/lib/
COPY --from=mxl-builder /opt/mxl-install/bin/     /usr/local/bin/
COPY --from=mxl-builder /opt/mxl-install/include/ /usr/local/include/
RUN ldconfig

# 4) Notre binaire de réception/émission ST 2110 → FLOWS MXL. Lie libmxl (grains/samples) et
#    libcrypto (SHA-1 pour l'UUIDv5 déterministe des flux, cohérent avec bobimxl.flow_id).
COPY mtl_rx.c /src/mtl_rx.c
RUN cc -O2 -o /usr/local/bin/mtl_rx /src/mtl_rx.c \
      $(pkg-config --cflags --libs mtl) -I/usr/local/include \
      -lmxl -ljson-c -lcrypto -lpthread -lm

# 5) DDP comms : NON copié ici. En AF_XDP/kernel le DDP est chargé par le driver ice de l'HÔTE.
#    En mode PF-DPDK (port en vfio-pci) : le PMD ice DPDK charge le DDP DEPUIS LE CONTENEUR →
#    docker_driver monte `-v /lib/firmware/intel/ice/ddp/ice.pkg:/lib/firmware/intel/ice/ddp/ice.pkg:ro`
#    dès qu'un port du nœud est `pmd=dpdk` (le .pkg vit sur l'hôte, cf. node_agent/firmware ;
#    on ne versionne pas un binaire Intel dans l'image).

# 6) Nettoyage de l'arbre source (les artefacts sont dans /usr/local).
RUN rm -rf /src/MTL

# 7) Contrôleur Python (rôle agent+contrôleur : :8080 métriques + :8081 nmos/subscribe + simu).
#    bobimxl : binding ctypes libmxl (même exemplaire que compute/media) → la simu/txgen écrit des
#    flows MXL. Sur le PYTHONPATH du process controller.py.
COPY controller.py /usr/local/bin/controller.py
COPY script_templates/bobimxl.py /usr/local/lib/mxl-python/bobimxl.py
ENV PYTHONPATH=/usr/local/lib/mxl-python

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
