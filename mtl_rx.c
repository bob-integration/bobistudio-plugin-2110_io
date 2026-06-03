// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.
//
// mtl_rx — receiver ST 2110-20 via Media Transport Library (libmtl/DPDK), écriture
// ZÉRO-COPIE dans le ring shared memory de Bobi.Studio.
//
// Modèle : app/sample/ext_frame/rx_st20_pipeline_dyn_ext_frame_sample.c de MTL, mais les
// buffers d'external frames pointent directement sur les slots du ring /dev/shm (DMA mlx5
// → shm, aucune copie). À chaque frame complète : on écrit le header (frame_index, time_ns)
// au début du shm — MÊME layout que receiver_2110 (header 64o : [u64 frame_index][u64 ns],
// puis ring de `ring` frames de `framesize`, à partir de l'offset `hdr`).
//
// Le scoping NIC (VF mlx5 dans le netns + RDMA exclusive), les hugepages, le cpuset/lcores
// et le sizing sont fournis par l'orchestrateur (cf. plugin script.py / deploy). Ici on ne
// fait que la RX MTL → shm.

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <math.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <mtl/st_pipeline_api.h>

#define MAX_FB 64

static volatile int g_stop = 0;
static void on_signal(int s) { (void)s; g_stop = 1; }

struct rx_ctx {
  mtl_handle st;
  st20p_rx_handle handle;
  /* shm */
  uint8_t* shm_base;      /* début du mapping (header à l'offset 0) */
  size_t   shm_size;
  uint8_t* frames_base;   /* shm_base + hdr (début du 1er slot) */
  size_t   framesize;     /* taille d'un slot = st20p_rx_frame_size(...) */
  int      ring;
  int      hdr;
  /* external frames pré-calculées (un par slot du ring) */
  struct {
    void*       addr;
    mtl_iova_t  iova;
    size_t      len;
  } fb[MAX_FB];
  int ext_idx;            /* prochain slot fourni à MTL (round-robin) */
  int plane_h;            /* hauteur des plans dans le buffer (champ si entrelacé) */
  enum st_frame_fmt out_fmt; /* format de la frame externe (sortie planar) */
  mtl_iova_t frames_iova; /* iova du début de la zone frames (dma_map) */
  /* compteurs */
  uint64_t frame_index;
  uint64_t frames_recv;
  pthread_t thread;
};

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* écrit l'en-tête du shm : [u64 frame_index][u64 time_ns] (little-endian natif x86) */
static void write_shm_header(struct rx_ctx* c, uint64_t fi) {
  uint64_t* h = (uint64_t*)c->shm_base;
  h[0] = fi;
  h[1] = now_ns();
}

/* MTL nous demande où écrire la prochaine frame entrante → on lui donne un slot du ring */
static int query_ext_frame(void* priv, struct st_ext_frame* ext_frame,
                           struct st20_rx_frame_meta* meta) {
  struct rx_ctx* c = priv;
  int i = c->ext_idx;
  /* La frame externe est le buffer de SORTIE (planar), pas le transport packé →
   * calculer les plans selon le format de sortie. */
  enum st_frame_fmt fmt = c->out_fmt;
  uint8_t planes = st_frame_fmt_planes(fmt);
  uint8_t* addr = (uint8_t*)c->fb[i].addr;
  /* Hauteur de plan = hauteur de CHAMP si entrelacé (le buffer ext = 1 champ, cf.
   * st_frame_size(interlaced) = taille d'un champ). meta->width donne la largeur. */
  uint32_t ph = c->plane_h ? (uint32_t)c->plane_h : meta->height;

  ext_frame->size = c->fb[i].len;
  for (int pl = 0; pl < planes; pl++) {
    ext_frame->linesize[pl] = st_frame_least_linesize(fmt, meta->width, pl);
    ext_frame->addr[pl] = addr;
    if (pl == 0)
      ext_frame->iova[0] = c->fb[i].iova;
    else
      ext_frame->iova[pl] = ext_frame->iova[pl - 1] + ext_frame->linesize[pl - 1] * ph;
    addr += ext_frame->linesize[pl] * ph;
  }
  if (++c->ext_idx >= c->ring) c->ext_idx = 0;
  return 0;
}

static int frame_available(void* priv) {
  (void)priv;
  return 0;
}

/* thread RX : récupère les frames complètes (déjà dans le shm) et publie l'en-tête */
static void* rx_thread(void* arg) {
  struct rx_ctx* c = arg;
  while (!g_stop) {
    struct st_frame* frame = st20p_rx_get_frame(c->handle);
    if (!frame) {
      usleep(1000);
      continue;
    }
    if (!frame->addr[0]) { st20p_rx_put_frame(c->handle, frame); continue; }
    /* la frame est DÉJÀ dans un slot du ring (zéro-copie). On calcule le slot réel à
     * partir de l'adresse et on aligne frame_index dessus pour que les consommateurs
     * lisent le bon slot (frame_index % ring). */
    uint8_t* a = (uint8_t*)frame->addr[0];
    long slot = (a >= c->frames_base && a < c->frames_base + (size_t)c->ring * c->framesize)
                ? (a - c->frames_base) / (long)c->framesize
                : (long)(c->frame_index % c->ring); /* garde-fou */
    if (slot < 0 || slot >= c->ring) slot = c->frame_index % c->ring;
    uint64_t fi = c->frame_index;
    if ((fi % c->ring) != (uint64_t)slot)
      fi += ((uint64_t)slot - (fi % c->ring) + c->ring) % c->ring;
    write_shm_header(c, fi);
    c->frame_index = fi + 1;
    c->frames_recv++;
    st20p_rx_put_frame(c->handle, frame);
  }
  return NULL;
}

static enum st_fps to_st_fps(double f) {
  if (fabs(f - 23.98) < 0.05) return ST_FPS_P23_98;
  if (fabs(f - 24.0) < 0.05) return ST_FPS_P24;
  if (fabs(f - 25.0) < 0.05) return ST_FPS_P25;
  if (fabs(f - 29.97) < 0.05) return ST_FPS_P29_97;
  if (fabs(f - 30.0) < 0.05) return ST_FPS_P30;
  if (fabs(f - 50.0) < 0.05) return ST_FPS_P50;
  if (fabs(f - 59.94) < 0.1) return ST_FPS_P59_94;
  if (fabs(f - 60.0) < 0.05) return ST_FPS_P60;
  if (fabs(f - 100.0) < 0.05) return ST_FPS_P100;
  if (fabs(f - 120.0) < 0.05) return ST_FPS_P120;
  return ST_FPS_P25;
}

static void usage(const char* p) {
  fprintf(stderr,
    "usage: %s --pci <BDF> --sip <ip> --mcast <ip> --udp_port <p> --payload_type <pt>\n"
    "          --width W --height H --fps F [--interlaced] --shm </dev/shm/x>\n"
    "          --ring N --hdr 64 --lcores a,b,c [--stats_file /path]\n", p);
}

int main(int argc, char** argv) {
  const char *pci = NULL, *sip = NULL, *mcast = NULL, *shm_path = NULL,
             *lcores = NULL, *stats_file = NULL;
  int udp_port = 0, payload_type = 96, width = 1920, height = 1080, ring = 10, hdr = 64;
  int interlaced = 0;
  double fps = 25.0;

  static struct option opts[] = {
    {"pci", 1, 0, 'p'}, {"sip", 1, 0, 's'}, {"mcast", 1, 0, 'm'},
    {"udp_port", 1, 0, 'u'}, {"payload_type", 1, 0, 't'}, {"width", 1, 0, 'W'},
    {"height", 1, 0, 'H'}, {"fps", 1, 0, 'F'}, {"interlaced", 0, 0, 'i'},
    {"shm", 1, 0, 'S'}, {"ring", 1, 0, 'R'}, {"hdr", 1, 0, 'D'},
    {"lcores", 1, 0, 'l'}, {"stats_file", 1, 0, 'f'}, {0, 0, 0, 0}};
  int o;
  while ((o = getopt_long(argc, argv, "", opts, NULL)) != -1) {
    switch (o) {
      case 'p': pci = optarg; break;
      case 's': sip = optarg; break;
      case 'm': mcast = optarg; break;
      case 'u': udp_port = atoi(optarg); break;
      case 't': payload_type = atoi(optarg); break;
      case 'W': width = atoi(optarg); break;
      case 'H': height = atoi(optarg); break;
      case 'F': fps = atof(optarg); break;
      case 'i': interlaced = 1; break;
      case 'S': shm_path = optarg; break;
      case 'R': ring = atoi(optarg); break;
      case 'D': hdr = atoi(optarg); break;
      case 'l': lcores = optarg; break;
      case 'f': stats_file = optarg; break;
      default: usage(argv[0]); return 1;
    }
  }
  if (!pci || !sip || !mcast || !udp_port || !shm_path) { usage(argv[0]); return 1; }
  /* MTL st20 RX transport limite framebuff_cnt à [2:8] → on cale le ring du shm dessus.
   * (le ring « logique » du pipeline aval peut être plus grand côté consommateurs ; ici
   * c'est le nombre de slots que MTL remplit en zéro-copie). */
  if (ring > 8) ring = 8;
  if (ring < 2) ring = 2;
  if (ring > MAX_FB) ring = MAX_FB;

  signal(SIGINT, on_signal);
  signal(SIGTERM, on_signal);

  /* ── init MTL ── */
  struct mtl_init_params p;
  memset(&p, 0, sizeof(p));
  p.num_ports = 1;
  snprintf(p.port[MTL_PORT_P], MTL_PORT_MAX_LEN, "%s", pci);
  inet_pton(AF_INET, sip, p.sip_addr[MTL_PORT_P]);
  p.pmd[MTL_PORT_P] = MTL_PMD_DPDK_USER;
  p.flags |= MTL_FLAG_DEV_AUTO_START_STOP;
  p.log_level = MTL_LOG_LEVEL_INFO;
  p.lcores = (char*)lcores;
  /* Mandatory : nb de queues NIC que la lib doit supporter. 1 session RX → 1 queue RX ;
   * 1 queue TX pour le contrôle (IGMP join / ARP). Sans ça : « fail to find free rx queue ». */
  p.rx_queues_cnt[MTL_PORT_P] = 1;
  p.tx_queues_cnt[MTL_PORT_P] = 1;

  struct rx_ctx c;
  memset(&c, 0, sizeof(c));
  c.ring = ring;
  c.hdr = hdr;

  c.out_fmt = ST_FRAME_FMT_YUV422PLANAR10LE;
  c.plane_h = interlaced ? height / 2 : height;  /* buffer ext = 1 champ si entrelacé */
  c.st = mtl_init(&p);
  if (!c.st) { fprintf(stderr, "mtl_rx: mtl_init fail\n"); return 1; }

  /* ── ops st20p RX en external frames ── */
  struct st20p_rx_ops ops;
  memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_rx";
  ops.priv = &c;
  ops.port.num_port = 1;
  inet_pton(AF_INET, mcast, ops.port.ip_addr[MTL_SESSION_PORT_P]);
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", pci);
  ops.port.udp_port[MTL_SESSION_PORT_P] = udp_port;
  ops.port.payload_type = payload_type;
  ops.width = width;
  ops.height = height;
  ops.fps = to_st_fps(fps);
  ops.interlaced = interlaced ? true : false;
  ops.transport_fmt = ST20_FMT_YUV_422_10BIT;
  ops.output_fmt = ST_FRAME_FMT_YUV422PLANAR10LE;
  ops.device = ST_PLUGIN_DEVICE_AUTO;
  ops.framebuff_cnt = ring;
  ops.notify_frame_available = frame_available;
  ops.query_ext_frame = query_ext_frame;
  ops.flags |= ST20P_RX_FLAG_EXT_FRAME;
  /* pas de RECEIVE_INCOMPLETE : on ne veut que des frames complètes dans le shm */

  c.handle = st20p_rx_create(c.st, &ops);
  if (!c.handle) { fprintf(stderr, "mtl_rx: st20p_rx_create fail\n"); mtl_uninit(c.st); return 1; }
  c.framesize = st20p_rx_frame_size(c.handle);

  /* ── shm : mmap (header + ring*framesize), arrondi page ── */
  size_t raw = (size_t)hdr + (size_t)ring * c.framesize;
  size_t pg = (size_t)sysconf(_SC_PAGESIZE);
  c.shm_size = (raw + pg - 1) & ~(pg - 1);   /* aligné page pour le DMA-map */
  int fd = open(shm_path, O_CREAT | O_RDWR, 0666);
  if (fd < 0) { perror("open shm"); return 1; }
  if (ftruncate(fd, c.shm_size) < 0) { perror("ftruncate"); return 1; }
  c.shm_base = mmap(NULL, c.shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (c.shm_base == MAP_FAILED) { perror("mmap"); return 1; }
  c.frames_base = c.shm_base + hdr;

  /* DMA-map du mapping ENTIER (base alignée page) ; l'iova des frames = base + hdr */
  c.frames_iova = mtl_dma_map(c.st, c.shm_base, c.shm_size);
  if (c.frames_iova == MTL_BAD_IOVA) {
    fprintf(stderr, "mtl_rx: mtl_dma_map fail (shm non DMA-mappable)\n");
    return 1;
  }
  for (int i = 0; i < ring; i++) {
    c.fb[i].addr = c.frames_base + (size_t)i * c.framesize;
    c.fb[i].iova = c.frames_iova + (mtl_iova_t)hdr + (mtl_iova_t)i * c.framesize;
    c.fb[i].len = c.framesize;
  }

  fprintf(stderr,
          "mtl_rx: started %dx%d%s fps=%.2f pt=%d mcast=%s:%d framesize=%zu ring=%d shm=%s\n",
          width, height, interlaced ? "i" : "p", fps, payload_type, mcast, udp_port,
          c.framesize, ring, shm_path);

  pthread_create(&c.thread, NULL, rx_thread, &c);

  /* boucle de stats (fps écrit dans stats_file pour le wrapper :8080) */
  uint64_t last = 0;
  time_t last_t = time(NULL);
  while (!g_stop) {
    sleep(2);
    time_t now = time(NULL);
    double dt = difftime(now, last_t);
    double f = dt > 0 ? (double)(c.frames_recv - last) / dt : 0.0;
    last = c.frames_recv;
    last_t = now;
    if (stats_file) {
      FILE* sf = fopen(stats_file, "w");
      if (sf) {
        fprintf(sf, "{\"fps\": %.1f, \"frame_index\": %llu}\n", f,
                (unsigned long long)c.frame_index);
        fclose(sf);
      }
    }
  }

  g_stop = 1;
  pthread_join(c.thread, NULL);
  st20p_rx_free(c.handle);
  mtl_dma_unmap(c.st, c.shm_base, c.frames_iova, c.shm_size);
  munmap(c.shm_base, c.shm_size);
  mtl_uninit(c.st);
  return 0;
}
