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
#
# CHANTIER MXL — Phase 2 : cette image embarque désormais libmxl (SDK Linux Foundation, patché
# planar) ET bobimxl.py. mtl_rx.c écrit/lit des FLOWS MXL (plus de rings shm maison) ; controller.py
# importe bobimxl pour la simu/txgen. L'étage builder libmxl ci-dessous est IDENTIQUE à celui de
# plugins/_compute_runtime/Dockerfile → cache BuildKit partagé (pas de recompil vcpkg sur un nœud
# qui a déjà buildé bobi-compute).

# ---------------------------------------------------------------- étage builder : compiler libmxl
FROM debian:trixie-slim AS mxl-builder
ARG MXL_REF=v1.0.1
ENV DEBIAN_FRONTEND=noninteractive
ENV VCPKG_ROOT=/opt/vcpkg
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates build-essential cmake ninja-build pkg-config \
      autoconf automake autoconf-archive libtool nasm bison flex libpcap-dev \
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
      -DMXL_ENABLE_FABRICS_OFI=OFF \
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
      python3 python3-numpy python3-pil fonts-dejavu-core \
 && rm -rf /var/lib/apt/lists/*

# Fix BPF/clang sur Debian multiarch : asm/ accessible (sinon mtl.xdp.c ne compile pas).
RUN ln -sf /usr/include/x86_64-linux-gnu/asm /usr/include/asm

# 2) Build DPDK patché MTL + libmtl. tests DPDK désactivés (inline, sinon fuit dans build.sh).
#    Le composant ld_preload/udp casse (signature ioctl) → ignoré ; gate = pkg-config mtl.
#    Patch bobi.studio st40/AF-XDP (RX ANC : mbuf port=UINT16_MAX en AF-XDP natif → fallback mono-port)
#    appliqué APRÈS le clone, AVANT build.sh (qui compile libmtl depuis lib/src).
COPY patch_st40_afxdp_port.py /tmp/patch_st40_afxdp_port.py
RUN git clone --depth 1 -b ${MTL_REF} https://github.com/OpenVisualCloud/Media-Transport-Library /src/MTL \
 && cd /src/MTL \
 && python3 /tmp/patch_st40_afxdp_port.py \
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
#    Pour le mode PF-DPDK-in-container (plus tard), monter le .pkg via `docker run -v` à
#    /usr/local/lib/bpf/... ou /lib/firmware/intel/ice/ddp/ice.pkg (évite de versionner un binaire Intel).

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
