// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.
//
// mtl_rx — receiver ST 2110-20 via Media Transport Library (libmtl / DPDK), écriture
// ZÉRO-COPIE dans le ring shared memory de Bobi.Studio (mêmes offsets/headers que
// receiver_2110 : header 64 o = [uint64 frame_index][uint64 time_ns], puis ring de frames
// YUV planar).
//
// ÉTAT : SQUELETTE (Phase C). La logique st20p ci-dessous est l'ossature cible ; elle sera
// complétée et validée une fois le runtime MTL/DPDK opérationnel dans le container (template
// LXC MTL + accès VF mlx5 via rdma-core + hugepages). Tant que ce n'est pas en place, le
// plugin tourne en simulation (script.py) et ce binaire n'est pas lancé.
//
// Principe cible (st20p, external frames) :
//   - mtl_init() avec le port = PCI de la VF (ex. 0000:11:00.2), IP source, hugepages.
//   - st20p_rx_create() en MODE EXTERNAL FRAMES : on fournit nous-mêmes les buffers =
//     les slots du ring /dev/shm → MTL y dépacketise/écrit directement (aucune copie).
//   - boucle : st20p_rx_get_frame() → la frame est déjà dans le slot shm ; on met à jour
//     le header (frame_index, time_ns) + le compteur fps ; st20p_rx_put_frame().
//
// Args (à finaliser) : --pci <bdf> --sip <ip> --mcast <ip> --port <udp> --w --h --fps
//                      --fmt <yuv422p10le|...> --shm </dev/shm/host_i> --ring <n> --hdr 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* En Phase C :
 * #include <mtl/mtl_api.h>
 * #include <mtl/st_pipeline_api.h>
 */

int main(int argc, char **argv) {
    /* TODO Phase C :
     *  1. parser les args (pci, mcast, port, w, h, fps, fmt, shm, ring, hdr)
     *  2. ouvrir+mmap le shm (taille = hdr + ring * frame_size)
     *  3. mtl_init() + st20p_rx_create() en external frames pointant sur les slots du ring
     *  4. boucle get_frame/put_frame : MAJ header (frame_index,time_ns) + fps
     */
    fprintf(stderr,
        "mtl_rx: squelette (Phase C) — RX MTL non encore implémentée. "
        "argc=%d\n", argc);
    (void)argv;
    return 0;
}
