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
ARG MXL_REF=v1.1.0-rc1
ENV DEBIAN_FRONTEND=noninteractive
ENV VCPKG_ROOT=/opt/vcpkg
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates build-essential cmake ninja-build pkg-config \
      autoconf automake autoconf-archive libtool nasm bison flex libpcap-dev \
      libibverbs-dev librdmacm-dev \
      curl zip unzip tar \
 && rm -rf /var/lib/apt/lists/*

# ─── libfabric COMPILÉE DEPUIS LES SOURCES ───────────────────────────────────────────────────
# MXL rc1 exige `FI_SOCKADDR_IP`, absent de la libfabric 2.1.0 de Debian trixie → la compilation
# de libmxl échoue net. L'amont compile lui aussi v2.5.1 depuis git, y compris dans son propre
# Dockerfile.debiantrixie. Détail complet dans plugins/_compute_runtime/Dockerfile.
ARG LIBFABRIC_REF=v2.5.1
RUN git clone --depth 1 --branch "${LIBFABRIC_REF}" https://github.com/ofiwg/libfabric.git /src/libfabric \
 && cd /src/libfabric \
 && ./autogen.sh \
 && ./configure --prefix=/opt/libfabric \
      --disable-kdreg2 --disable-memhooks-monitor --disable-uffd-monitor \
 && make -j"$(nproc)" install \
 && rm -rf /src/libfabric

# Garde-fou : le provider VERBS (RoCEv2) doit être RÉELLEMENT embarqué. On interroge la lib
# construite, on n'infère pas.
RUN LD_LIBRARY_PATH=/opt/libfabric/lib /opt/libfabric/bin/fi_info -l > /tmp/fi_providers.txt 2>&1 \
 && grep -qw verbs /tmp/fi_providers.txt \
 || { echo "ERREUR: provider VERBS absent de libfabric — RoCEv2/RDMA serait mort en silence."; \
      echo "--- providers réellement embarqués ---"; cat /tmp/fi_providers.txt; false; }
ENV PKG_CONFIG_PATH=/opt/libfabric/lib/pkgconfig
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
COPY plugins/_compute_runtime/patches/mxl-planar-slices.patch /tmp/mxl-planar-slices.patch
# Curseur de tranche non réinitialisé au changement de grain dans l'initiateur fabrics : c'est LA
# cause des bandes venant d'une autre trame sur une réplique de flux TRANCHÉ (cf. le patch, et
# docs/chantiers/MXL_FABRICS_SLICE_CORRUPTION.md). À remonter en amont ; en attendant, on corrige
# ici — on construit MXL depuis les sources, autant que ce soit avec le correctif.
COPY plugins/_compute_runtime/patches/mxl-fabrics-slice-reset.patch /tmp/mxl-fabrics-slice-reset.patch
# Notification (donnée immédiate) clôturée derrière la charge utile qu'elle annonce :
# sans FI_FENCE rien n'ordonne les écritures RMA d'un lot multi-plans, la cible publie des
# tranches dont les octets sont encore en vol. SECONDE cause des bandes déchirées.
COPY plugins/_compute_runtime/patches/mxl-fabrics-fence-notification.patch /tmp/mxl-fabrics-fence-notification.patch
RUN git apply --verbose /tmp/mxl-planar-type.patch \
 && git apply --verbose /tmp/mxl-planar-slices.patch \
 && git apply --verbose /tmp/mxl-fabrics-slice-reset.patch \
 && git apply --verbose /tmp/mxl-fabrics-fence-notification.patch \
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
      librdmacm1 \
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
#    Patch bobi.studio TX burst rendezvous (0.45.0, fix RACINE des morts silencieuses de sessions
#    TX au commit RL) : la garde resetting ci-dessus empêche un NOUVEAU burst de démarrer une fois
#    resetting vu à true, mais rien n'empêchait un burst DÉJÀ ENGAGÉ d'écrire encore dans le
#    sw_ring pendant que rte_tm_hierarchy_commit stoppe le port (ice_tx_queue_stop → memset SANS
#    free) — ces mbufs tardifs étaient perdus en silence (mempool vidé, build ret -207 permanent,
#    hang detector natif jamais atteint puisqu'il ne vit que dans le chemin burst). Compteur
#    atomique per-port bobi_burst_inflight (mt_main.h) autour des 2 points de choke burst
#    (mt_rxq_burst/mt_txq_burst, mt_queue.c) + spin-wait borné (80 ms) dans
#    dev_tx_queue_set_rl_rate (mt_dev.c) APRÈS resetting=true et AVANT le commit. DOIT s'appliquer
#    après patch_rx_resetting_guard (ancres = son état de sortie).
#    Patch bobi.studio TM hierarchy (lève le mur des 8 files TX RL : arbre traffic-manager RAMIFIÉ
#    — plusieurs nœuds queue-group au lieu d'un seul — pour >8 senders RL/port ET création en rafale ;
#    nb_qps = P_parents × fan-out 8 côté PMD ice, jusqu'à MT_MAX_RL_ITEMS=128).
#    Patch bobi.studio builder famine recovery (vidéo+audio) : une session TX déjà vivante qui perd
#    les mbufs de son mempool hdr au commit RL d'UNE AUTRE session TX (cf. patch RX/TX hang resetting
#    guard ci-dessus) échoue à l'ALLOC dans le builder, AVANT le chemin burst où vit le hang detector
#    natif — celui-ci ne se déclenche donc jamais (ring vide, plus aucun paquet construit). Ce patch
#    track la durée de famine (hors fenêtre resetting) et déclenche st20_tx_queue_fatal_error /
#    st_audio_queue_fatal_error (récupération EXISTANTE : purge rings + nouvelle queue + reset
#    mempool + WAIT_FRAME) après 2 s de famine confirmée.
COPY patch_st40_afxdp_port.py /tmp/patch_st40_afxdp_port.py
COPY patch_afxdp_tx_link_drop.py /tmp/patch_afxdp_tx_link_drop.py
COPY patch_rx_resetting_guard.py /tmp/patch_rx_resetting_guard.py
COPY patch_tx_burst_rendezvous.py /tmp/patch_tx_burst_rendezvous.py
COPY patch_tx_hang_resetting_guard.py /tmp/patch_tx_hang_resetting_guard.py
COPY patch_tx_builder_famine_recovery.py /tmp/patch_tx_builder_famine_recovery.py
#    Patch bobi.studio TX inflight frame reclaim (0.49.0) : 2ᵉ mode de mort collatérale du commit RL,
#    DISTINCT de la famine mempool ci-dessus. Les mbufs perdus sans free au stop de port (memset de
#    ice_reset_tx_queue) tiennent encore une ref extbuf sur la TRAME en vol → tv_frame_free_cb n'est
#    jamais appelé → la trame n'est JAMAIS rendue à l'app → l'app n'a plus de slot libre → plus rien
#    à donner → get_next_frame rend -EBUSY à chaque passe = `build ret -203`
#    (STI_FRAME_APP_GET_FRAME_BUSY) PERMANENT, sur une session qui n'échoue JAMAIS à allouer (donc
#    invisible du filet famine, dont les points d'appel sont les branches d'échec d'ALLOC). Le patch
#    détecte -203 continu > 2 s hors resetting et RÉCUPÈRE les trames à refcnt non nul (impossible à
#    50 fps) exactement comme st20_tx_queue_fatal_error le fait déjà, mais SANS récupération de queue
#    → ZÉRO commit TM, zéro nouvelle victime. DOIT s'appliquer APRÈS patch_tx_builder_famine_recovery
#    (ancres = son état de sortie).
COPY patch_tx_frame_inflight_reclaim.py /tmp/patch_tx_frame_inflight_reclaim.py
#    Patch bobi.studio TX reset no-drop (0.50.0) : LA FUITE DE MBUFS DU COMMIT TM, À LA SOURCE.
#    L'hypothèse « ice_reset_tx_queue memset le sw_ring sans free » est FAUSSE sur DPDK 26.03
#    (ice_rxtx.c:1196 ci_txq_release_all_mbufs AVANT ice_reset_tx_queue:1197 ; common/tx.h:360
#    libère TOUT le sw_ring ; ice_ethdev.c:2886 stoppe chaque queue) — le PMD libère bien. La
#    fuite venait de NOTRE patch_tx_hang_resetting_guard : pendant la fenêtre de commit il
#    retournait nb_pkts (« émis ») aux transmetteurs vidéo/audio, qui lâchaient donc les mbufs
#    SANS free (video_burst_packet ne ré-empile que si tx < bulk ; l'audio fait
#    trs->inflight = NULL). D'où les DEUX morts collatérales : mempool vidé (-207) et ref extbuf
#    jamais rendue sur la trame en vol (-203 permanent). Fix : retourner 0 (= « queue pleine »,
#    sémantique déjà gérée par tous les appelants, cf. le transmetteur ANC qui n'a jamais fui) —
#    les paquets restent en inflight et sont ré-émis au redémarrage du port. Zéro mbuf perdu.
#    DOIT s'appliquer APRÈS patch_tx_hang_resetting_guard / famine / inflight_reclaim.
COPY patch_tx_reset_no_drop.py /tmp/patch_tx_reset_no_drop.py
COPY patch_tm_hierarchy.py /tmp/patch_tm_hierarchy.py
COPY patch_igmp_router_alert.py /tmp/patch_igmp_router_alert.py
# Patch bobi.studio ICMP ECHO : un port en vfio-pci n'a plus de netdev noyau, donc plus
# personne pour répondre au ping — et libmtl ne connaît que l'ARP. L'intégration sur un
# plant ST 2110 se valide interface par interface AU PING depuis le routeur d'accès.
COPY patch_icmp_echo.py /tmp/patch_icmp_echo.py
COPY patch_ptp_mcast_flow.py /tmp/patch_ptp_mcast_flow.py
COPY patch_ptp_stable_getter.py /tmp/patch_ptp_stable_getter.py
COPY patch_ptp_gm_export.py /tmp/patch_ptp_gm_export.py
COPY patch_ptp_offset_getter.py /tmp/patch_ptp_offset_getter.py
# Patch bobi.studio DEV LINK WAIT : sur E810 100G DPDK le lien met ~60-90s à ré-entraîner autoneg+FEC
# après passage sous vfio-pci ; dev_detect_link abandonnait à 90s (fail -5 → mtl_init fail → crash-loop
# bruyant du wrapper). Bumpe MT_DEV_LINK_POLL_COUNT 300->400 (budget strict 90s->120s). Indépendant.
COPY patch_dev_link_wait.py /tmp/patch_dev_link_wait.py
# Patch bobi.studio EPOCH-SHIFT TX (chantier latence) : ops.rtp_timestamp_delta_us NÉGATIF ⇒ la
# fenêtre d'émission de la session est décalée de −delta APRÈS l'epoch nominal, le timestamp RTP
# restant sur l'epoch NOMINAL (TROFF ST 2110-21, FPT ≈ shift côté récepteur). Delta ≥ 0 inchangé.
COPY patch_epoch_shift.py /tmp/patch_epoch_shift.py
# Patch bobi.studio ICE TM MOVE RETRY (0.46.0, fix RACINE des sessions TX mortes en cold-batch,
# cf. patch_tm_hierarchy.py + patch_tx_burst_rendezvous.py) : ce patch cible le driver DPDK ice
# (ice_tm.c), PAS libmtl — il dépose un .patch dans patches/dpdk/<ver>/ du clone MTL, appliqué
# par le mécanisme natif de ./script/build_dpdk.sh (comme les 8 patches Intel/MTL officiels de ce
# dossier). Cause racine : à chaque restart de port déclenché par un commit RL, ice_tm_setup_txq_node()
# reparente CHAQUE queue déjà en hiérarchie vers notre arbre RL via un aller-retour admin-queue
# firmware (ice_aq_move_recfg_lan_txq) — structurel, a lieu à chaque restart, pas évitable en
# ajustant le fan-out côté libmtl. Cet aller-retour peut échouer transitoirement sous rafale de
# commits ; non retenté, l'échec faisait échouer LE PORT ENTIER (pas juste la queue en cause) ⇒
# plus aucune queue TX ne draine ⇒ mempools vidés ⇒ build ret -207 permanent. Fix : retry borné
# (5×20ms) puis, si toujours en échec, dégrader (queue laissée sur son nœud actuel, port maintenu
# en vie) au lieu de tuer le port — auto-guérison au commit suivant. DOIT s'appliquer avant
# build_dpdk.sh (dépose le .patch dans le clone, pas de modif du dossier patches/dpdk en place).
COPY patch_ice_tm_move_retry.py /tmp/patch_ice_tm_move_retry.py
# MTL_REF est un SHA (pas une branche) → clone + fetch ciblé (GitHub sert les SHA joignables).
RUN mkdir -p /src/MTL && cd /src/MTL \
 && git init -q && git remote add origin https://github.com/OpenVisualCloud/Media-Transport-Library \
 && git fetch --depth 1 origin ${MTL_REF} \
 && git checkout -q FETCH_HEAD \
 && python3 /tmp/patch_st40_afxdp_port.py \
 && python3 /tmp/patch_afxdp_tx_link_drop.py \
 && python3 /tmp/patch_rx_resetting_guard.py \
 && python3 /tmp/patch_tx_burst_rendezvous.py \
 && python3 /tmp/patch_tx_hang_resetting_guard.py \
 && python3 /tmp/patch_tx_builder_famine_recovery.py \
 && python3 /tmp/patch_tx_frame_inflight_reclaim.py \
 && python3 /tmp/patch_tx_reset_no_drop.py \
 && python3 /tmp/patch_tm_hierarchy.py \
 && python3 /tmp/patch_igmp_router_alert.py \
 && python3 /tmp/patch_icmp_echo.py \
 && python3 /tmp/patch_ptp_mcast_flow.py \
 && python3 /tmp/patch_ptp_stable_getter.py \
 && python3 /tmp/patch_ptp_gm_export.py \
 && python3 /tmp/patch_ptp_offset_getter.py \
 && python3 /tmp/patch_dev_link_wait.py \
 && python3 /tmp/patch_epoch_shift.py \
 && python3 /tmp/patch_ice_tm_move_retry.py \
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
# libfabric = celle qu'on a COMPILÉE (v2.5.1), surtout pas `libfabric1` d'apt (2.1.0, même soname
# `libfabric.so.1` → chargerait l'ancienne au runtime). `librdmacm1` est explicite plus haut :
# c'est `libfabric1` qui le tirait transitivement (cf. plugins/_compute_runtime/Dockerfile).
COPY --from=mxl-builder /opt/mxl-install/lib/     /usr/local/lib/
COPY --from=mxl-builder /opt/mxl-install/bin/     /usr/local/bin/
COPY --from=mxl-builder /opt/mxl-install/include/ /usr/local/include/
COPY --from=mxl-builder /opt/libfabric/lib/libfabric.so* /usr/local/lib/
RUN ldconfig

# Garde-fou de l'étage RUNTIME : toutes les dépendances dynamiques du chemin RDMA doivent être
# résolues DANS L'IMAGE (l'étage builder ne prouve rien sur ce qui est livré).
RUN if ldd /usr/local/lib/libmxl-fabrics.so 2>&1 | grep -q "not found"; then \
      echo "ERREUR: dépendance dynamique NON RÉSOLUE dans l'image runtime (chemin RDMA) :"; \
      ldd /usr/local/lib/libmxl-fabrics.so 2>&1 | grep "not found"; \
      false; \
    fi

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
