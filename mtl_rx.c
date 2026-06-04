// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.
//
// mtl_rx — receiver ST 2110 MULTI-SESSION via Media Transport Library (libmtl/DPDK).
//
// UN SEUL mtl_init (un PF = un MtlManager/XDP) héberge N sessions de tout type sur la même carte :
//   - VIDEO (st20p, ST 2110-20) → ring /dev/shm/{hn}_{idx} (YUV422 planar, conv 10→8, overlay IDENT)
//   - AUDIO (st30p, ST 2110-30) → ring /dev/shm/{hn}_audio_{idx} (chunks 1ms, L24 8ch, BIG-ENDIAN
//     wire-native : on écrit le payload TEL QUEL, zéro conversion — le pipeline MXL audio est en BE)
//   - DATA  (st40, ST 2110-40 ANC) → point d'extension prêt (non implémenté : pas de consommateur).
//
// Les sessions sont décrites par un fichier de config JSON (--config), écrit par le contrôleur.
// Compat : sans --config, les args legacy construisent 1 session vidéo (tests manuels).
//
// Layout shm identique à receiver_2110 : header 64o [u64 index][u64 ns] puis ring de slots.

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
#include <mtl/st30_pipeline_api.h>
#include <json-c/json.h>

#define MAX_FB    64
#define MAX_SESS  16

enum sess_kind { K_VIDEO, K_AUDIO, K_DATA };   /* DATA = ST 2110-40, prêt mais non implémenté */

static volatile int g_stop = 0;
static void on_signal(int s) { (void)s; g_stop = 1; }

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* Une session = une essence sur le mtl_handle partagé. Champs communs + spécifiques par type. */
struct sess {
  enum sess_kind kind;
  mtl_handle st;            /* partagé (mtl_init unique) */
  char portname[MTL_PORT_MAX_LEN];
  /* réseau */
  char mcast[64];
  int  udp_port, payload_type;
  /* shm */
  uint8_t* shm_base; size_t shm_size; uint8_t* frames_base;
  size_t   slotsize;       /* vidéo: framesize ; audio: 1152 (1 chunk 1ms) */
  int      ring, hdr;
  char     shm_path[300], stats_path[300];
  uint64_t index;          /* frame_index (vidéo) / chunk_index (audio) */
  uint64_t recv;           /* compteur reçu (pour le débit) */
  pthread_t thread; int started;
  int      copy_mode;      /* vidéo: 1=memcpy (af_xdp) ; audio: toujours 1 */
  /* ── vidéo ── */
  st20p_rx_handle vh;
  int      width, height, bit_depth, interlaced; double fps;
  size_t   src_framesize; int conv8;
  enum st_frame_fmt out_fmt; int plane_h;
  struct { void* addr; mtl_iova_t iova; size_t len; } fb[MAX_FB];
  int      ext_idx; mtl_iova_t frames_iova;
  char     ident_file[300]; int has_ident;
  uint8_t* ident_patch; int id_bw, id_bh; long id_mtime;
  /* ── audio ── */
  st30p_rx_handle ah;
  int      channels;
};

static void write_shm_header(struct sess* s, uint64_t i) {
  uint64_t* h = (uint64_t*)s->shm_base;
  h[0] = i; h[1] = now_ns();
}

/* ═══ VIDÉO ═══════════════════════════════════════════════════════════════════ */

static int query_ext_frame(void* priv, struct st_ext_frame* ext_frame,
                           struct st20_rx_frame_meta* meta) {
  struct sess* s = priv;
  int i = s->ext_idx;
  enum st_frame_fmt fmt = s->out_fmt;
  uint8_t planes = st_frame_fmt_planes(fmt);
  uint8_t* addr = (uint8_t*)s->fb[i].addr;
  uint32_t ph = s->plane_h ? (uint32_t)s->plane_h : meta->height;
  ext_frame->size = s->fb[i].len;
  for (int pl = 0; pl < planes; pl++) {
    ext_frame->linesize[pl] = st_frame_least_linesize(fmt, meta->width, pl);
    ext_frame->addr[pl] = addr;
    if (pl == 0) ext_frame->iova[0] = s->fb[i].iova;
    else ext_frame->iova[pl] = ext_frame->iova[pl - 1] + ext_frame->linesize[pl - 1] * ph;
    addr += ext_frame->linesize[pl] * ph;
  }
  if (++s->ext_idx >= s->ring) s->ext_idx = 0;
  return 0;
}

static int frame_available(void* priv) { (void)priv; return 0; }

/* IDENT : recharge le patch (fichier écrit par le contrôleur) si mtime change. Coût ~nul si off. */
static void load_ident_patch(struct sess* s) {
  if (!s->has_ident) return;
  struct stat st;
  if (stat(s->ident_file, &st) != 0) {
    if (s->ident_patch) { free(s->ident_patch); s->ident_patch = NULL; }
    s->id_bw = s->id_bh = 0; s->id_mtime = 0; return;
  }
  if ((long)st.st_mtime == s->id_mtime && s->ident_patch) return;
  FILE* f = fopen(s->ident_file, "rb");
  if (!f) return;
  uint32_t hdr[2];
  if (fread(hdr, sizeof(uint32_t), 2, f) != 2) { fclose(f); return; }
  int bw = (int)hdr[0], bh = (int)hdr[1];
  if (bw <= 0 || bh <= 0 || bw > 8192 || bh > 8192) { fclose(f); return; }
  size_t n = (size_t)bw * bh;
  uint8_t* p = malloc(n);
  if (!p) { fclose(f); return; }
  if (fread(p, 1, n, f) != n) { free(p); fclose(f); return; }
  fclose(f);
  if (s->ident_patch) free(s->ident_patch);
  s->ident_patch = p; s->id_bw = bw; s->id_bh = bh; s->id_mtime = (long)st.st_mtime;
}

static void overlay_ident(struct sess* s, uint8_t* dst) {
  if (!s->ident_patch || s->id_bw <= 0) return;
  int W = s->width, H = s->height, bw = s->id_bw, bh = s->id_bh;
  if (bw > W || bh > H) return;
  int x0 = W - bw - 8, y0 = 8;
  x0 -= x0 & 1; y0 -= y0 & 1; if (x0 < 0) x0 = 0;
  int deep = (s->bit_depth >= 10), shift = s->bit_depth - 8;
  size_t ysz = (size_t)W * H, uvsz = (size_t)(W / 2) * H;
  int bps = deep ? 2 : 1;
  for (int r = 0; r < bh; r++) {
    const uint8_t* p = s->ident_patch + (size_t)r * bw;
    if (deep) { uint16_t* y = (uint16_t*)dst + (size_t)(y0 + r) * W + x0;
                for (int x = 0; x < bw; x++) y[x] = (uint16_t)p[x] << shift; }
    else      { uint8_t*  y = dst + (size_t)(y0 + r) * W + x0;
                for (int x = 0; x < bw; x++) y[x] = p[x]; }
  }
  int neutral = 1 << (s->bit_depth - 1);
  int ux0 = x0 / 2, ubw = bw / 2;
  uint8_t* uplane = dst + ysz * bps;
  uint8_t* vplane = uplane + uvsz * bps;
  for (int pl = 0; pl < 2; pl++) {
    uint8_t* plane = pl ? vplane : uplane;
    for (int r = 0; r < bh; r++) {
      if (deep) { uint16_t* row = (uint16_t*)plane + (size_t)(y0 + r) * (W / 2) + ux0;
                  for (int x = 0; x < ubw; x++) row[x] = (uint16_t)neutral; }
      else      { uint8_t*  row = plane + (size_t)(y0 + r) * (W / 2) + ux0;
                  for (int x = 0; x < ubw; x++) row[x] = (uint8_t)neutral; }
    }
  }
}

static void* video_rx_thread(void* arg) {
  struct sess* s = arg;
  while (!g_stop) {
    struct st_frame* frame = st20p_rx_get_frame(s->vh);
    if (!frame) { usleep(1000); continue; }
    if (!frame->addr[0]) { st20p_rx_put_frame(s->vh, frame); continue; }
    uint64_t fi = s->index;
    if (s->copy_mode) {
      long slot = fi % s->ring;
      uint8_t* dst = s->frames_base + (size_t)slot * s->slotsize;
      if (s->conv8) {
        const uint16_t* src = (const uint16_t*)frame->addr[0];
        size_t n = s->slotsize;
        for (size_t k = 0; k < n; k++) dst[k] = (uint8_t)(src[k] >> 2);
      } else {
        memcpy(dst, frame->addr[0], s->slotsize);
      }
    } else {
      uint8_t* a = (uint8_t*)frame->addr[0];
      long slot = (a >= s->frames_base && a < s->frames_base + (size_t)s->ring * s->slotsize)
                  ? (a - s->frames_base) / (long)s->slotsize : (long)(fi % s->ring);
      if (slot < 0 || slot >= s->ring) slot = fi % s->ring;
      if ((fi % s->ring) != (uint64_t)slot)
        fi += ((uint64_t)slot - (fi % s->ring) + s->ring) % s->ring;
    }
    if (s->has_ident) {
      load_ident_patch(s);
      overlay_ident(s, s->frames_base + (size_t)(fi % s->ring) * s->slotsize);
    }
    write_shm_header(s, fi);
    s->index = fi + 1; s->recv++;
    st20p_rx_put_frame(s->vh, frame);
  }
  return NULL;
}

/* ═══ AUDIO ═══════════════════════════════════════════════════════════════════ */
/* st30p délivre le payload L24 du fil = BIG-ENDIAN. Le pipeline MXL audio est désormais en BE
 * (wire-native) → on écrit le chunk TEL QUEL dans le ring, ZÉRO conversion (passthrough). */
static void* audio_rx_thread(void* arg) {
  struct sess* s = arg;
  while (!g_stop) {
    struct st30_frame* frame = st30p_rx_get_frame(s->ah);
    if (!frame) { usleep(500); continue; }
    uint64_t ci = s->index;
    long slot = ci % s->ring;
    uint8_t* dst = s->frames_base + (size_t)slot * s->slotsize;   /* slotsize = 1152 */
    size_t n = frame->data_size < s->slotsize ? frame->data_size : s->slotsize;
    memcpy(dst, frame->addr, n);
    if (n < s->slotsize) memset(dst + n, 0, s->slotsize - n);
    write_shm_header(s, ci);
    s->index = ci + 1; s->recv++;
    st30p_rx_put_frame(s->ah, frame);
  }
  return NULL;
}

/* ═══ communs ═════════════════════════════════════════════════════════════════ */

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

/* mmap (création + ftruncate) du shm de la session, header à l'offset 0. */
static int open_shm(struct sess* s) {
  size_t raw = (size_t)s->hdr + (size_t)s->ring * s->slotsize;
  size_t pg = (size_t)sysconf(_SC_PAGESIZE);
  s->shm_size = (raw + pg - 1) & ~(pg - 1);
  int fd = open(s->shm_path, O_CREAT | O_RDWR, 0666);
  if (fd < 0) { perror("open shm"); return -1; }
  if (ftruncate(fd, s->shm_size) < 0) { perror("ftruncate"); close(fd); return -1; }
  s->shm_base = mmap(NULL, s->shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (s->shm_base == MAP_FAILED) { perror("mmap"); return -1; }
  s->frames_base = s->shm_base + s->hdr;
  return 0;
}

static int setup_video(struct sess* s) {
  s->copy_mode = 1;   /* af_xdp/kernel uniquement dans ce contexte */
  s->out_fmt = ST_FRAME_FMT_YUV422PLANAR10LE;   /* converter présent ; conv 10→8 nous-mêmes */
  s->conv8 = (s->bit_depth == 8);
  s->plane_h = s->interlaced ? s->height / 2 : s->height;
  if (s->ring > 8) s->ring = 8; if (s->ring < 2) s->ring = 2; if (s->ring > MAX_FB) s->ring = MAX_FB;

  struct st20p_rx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_v";
  ops.priv = s;
  ops.port.num_port = 1;
  inet_pton(AF_INET, s->mcast, ops.port.ip_addr[MTL_SESSION_PORT_P]);
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  ops.port.payload_type = s->payload_type;
  ops.width = s->width; ops.height = s->height; ops.fps = to_st_fps(s->fps);
  ops.interlaced = s->interlaced ? true : false;
  ops.transport_fmt = ST20_FMT_YUV_422_10BIT;
  ops.output_fmt = ST_FRAME_FMT_YUV422PLANAR10LE;
  ops.device = ST_PLUGIN_DEVICE_AUTO;
  ops.framebuff_cnt = s->ring;
  ops.notify_frame_available = frame_available;
  /* af_xdp → copy_mode : frames internes MTL, on memcpy (pas d'ext-frame DMA). */

  s->vh = st20p_rx_create(s->st, &ops);
  if (!s->vh) { fprintf(stderr, "mtl_rx: st20p_rx_create fail (video %s:%d)\n", s->mcast, s->udp_port); return -1; }
  s->src_framesize = st20p_rx_frame_size(s->vh);
  s->slotsize = s->conv8 ? s->src_framesize / 2 : s->src_framesize;
  if (open_shm(s) != 0) return -1;
  fprintf(stderr, "mtl_rx[video] %dx%d%s fps=%.2f pt=%d mc=%s:%d slot=%zu ring=%d shm=%s\n",
          s->width, s->height, s->interlaced ? "i" : "p", s->fps, s->payload_type,
          s->mcast, s->udp_port, s->slotsize, s->ring, s->shm_path);
  return pthread_create(&s->thread, NULL, video_rx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

static int setup_audio(struct sess* s) {
  if (s->channels <= 0) s->channels = 8;
  if (s->ring < 2) s->ring = 2;
  s->slotsize = (size_t)(48000 / 1000) * s->channels * 3;   /* 1 ms L24 = 1152 si 8ch */
  s->copy_mode = 1;

  struct st30p_rx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_a";
  ops.priv = s;
  ops.port.num_port = 1;
  inet_pton(AF_INET, s->mcast, ops.port.ip_addr[MTL_SESSION_PORT_P]);
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  ops.port.payload_type = s->payload_type;
  ops.fmt = ST30_FMT_PCM24;
  ops.channel = (uint16_t)s->channels;
  ops.sampling = ST30_SAMPLING_48K;
  ops.ptime = ST30_PTIME_1MS;
  ops.framebuff_size = (uint32_t)s->slotsize;   /* 1 chunk = 1 ms */
  ops.framebuff_cnt = 4;

  s->ah = st30p_rx_create(s->st, &ops);
  if (!s->ah) { fprintf(stderr, "mtl_rx: st30p_rx_create fail (audio %s:%d)\n", s->mcast, s->udp_port); return -1; }
  if (open_shm(s) != 0) return -1;
  fprintf(stderr, "mtl_rx[audio] %dch L24/48k pt=%d mc=%s:%d slot=%zu ring=%d shm=%s\n",
          s->channels, s->payload_type, s->mcast, s->udp_port, s->slotsize, s->ring, s->shm_path);
  return pthread_create(&s->thread, NULL, audio_rx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

/* ── parse config JSON ── */
static const char* jstr(struct json_object* o, const char* k, const char* def) {
  struct json_object* v;
  return (json_object_object_get_ex(o, k, &v)) ? json_object_get_string(v) : def;
}
static int jint(struct json_object* o, const char* k, int def) {
  struct json_object* v;
  return (json_object_object_get_ex(o, k, &v)) ? json_object_get_int(v) : def;
}
static double jdbl(struct json_object* o, const char* k, double def) {
  struct json_object* v;
  return (json_object_object_get_ex(o, k, &v)) ? json_object_get_double(v) : def;
}

static void usage(const char* p) {
  fprintf(stderr,
    "usage: %s --config <json>   (sessions multiples : video st20 / audio st30)\n"
    "   ou : %s [args legacy 1 session video] (--pmd af_xdp --iface .. --mcast .. --shm .. ...)\n", p, p);
}

int main(int argc, char** argv) {
  /* globaux partagés */
  char pmd[32] = "af_xdp", iface[64] = "", sip[64] = "", lcores[128] = "";
  const char* config = NULL;
  struct sess S[MAX_SESS]; memset(S, 0, sizeof(S));
  int ns = 0;

  /* args legacy d'une session vidéo (compat tests manuels) */
  const char *l_mcast=NULL,*l_shm=NULL,*l_stats=NULL,*l_ident=NULL,*l_pci=NULL;
  int l_port=0,l_pt=96,l_w=1920,l_h=1080,l_ring=8,l_hdr=64,l_inter=0,l_bd=10; double l_fps=25.0;

  static struct option opts[] = {
    {"config",1,0,'c'},{"pmd",1,0,'M'},{"iface",1,0,'N'},{"sip",1,0,'s'},{"lcores",1,0,'l'},
    {"mcast",1,0,'m'},{"udp_port",1,0,'u'},{"payload_type",1,0,'t'},{"width",1,0,'W'},
    {"height",1,0,'H'},{"fps",1,0,'F'},{"interlaced",0,0,'i'},{"shm",1,0,'S'},{"ring",1,0,'R'},
    {"hdr",1,0,'D'},{"stats_file",1,0,'f'},{"bit_depth",1,0,'B'},{"ident_file",1,0,'G'},
    {"pci",1,0,'p'},{0,0,0,0}};
  int o;
  while ((o = getopt_long(argc, argv, "", opts, NULL)) != -1) {
    switch (o) {
      case 'c': config = optarg; break;
      case 'M': snprintf(pmd,sizeof(pmd),"%s",optarg); break;
      case 'N': snprintf(iface,sizeof(iface),"%s",optarg); break;
      case 's': snprintf(sip,sizeof(sip),"%s",optarg); break;
      case 'l': snprintf(lcores,sizeof(lcores),"%s",optarg); break;
      case 'm': l_mcast=optarg; break;   case 'u': l_port=atoi(optarg); break;
      case 't': l_pt=atoi(optarg); break; case 'W': l_w=atoi(optarg); break;
      case 'H': l_h=atoi(optarg); break;  case 'F': l_fps=atof(optarg); break;
      case 'i': l_inter=1; break;         case 'S': l_shm=optarg; break;
      case 'R': l_ring=atoi(optarg); break; case 'D': l_hdr=atoi(optarg); break;
      case 'f': l_stats=optarg; break;    case 'B': l_bd=atoi(optarg); break;
      case 'G': l_ident=optarg; break;    case 'p': l_pci=optarg; break;
      default: usage(argv[0]); return 1;
    }
  }
  (void)l_pci;

  if (config) {
    struct json_object* root = json_object_from_file(config);
    if (!root) { fprintf(stderr, "mtl_rx: config illisible: %s\n", config); return 1; }
    snprintf(pmd,sizeof(pmd),"%s",jstr(root,"pmd","af_xdp"));
    snprintf(iface,sizeof(iface),"%s",jstr(root,"iface",""));
    snprintf(sip,sizeof(sip),"%s",jstr(root,"sip",""));
    snprintf(lcores,sizeof(lcores),"%s",jstr(root,"lcores",""));
    struct json_object* arr;
    if (!json_object_object_get_ex(root,"sessions",&arr) || !json_object_is_type(arr,json_type_array)) {
      fprintf(stderr,"mtl_rx: config sans 'sessions'\n"); return 1; }
    int n = json_object_array_length(arr);
    for (int k = 0; k < n && ns < MAX_SESS; k++) {
      struct json_object* j = json_object_array_get_idx(arr, k);
      struct sess* s = &S[ns];
      const char* kind = jstr(j,"kind","video");
      s->kind = !strcmp(kind,"audio") ? K_AUDIO : !strcmp(kind,"data") ? K_DATA : K_VIDEO;
      snprintf(s->mcast,sizeof(s->mcast),"%s",jstr(j,"mcast",""));
      s->udp_port=jint(j,"udp_port",0); s->payload_type=jint(j,"payload_type",96);
      s->ring=jint(j,"ring", s->kind==K_AUDIO?100:8); s->hdr=jint(j,"hdr",64);
      snprintf(s->shm_path,sizeof(s->shm_path),"%s",jstr(j,"shm",""));
      snprintf(s->stats_path,sizeof(s->stats_path),"%s",jstr(j,"stats",""));
      if (s->kind == K_VIDEO) {
        s->width=jint(j,"width",1920); s->height=jint(j,"height",1080);
        s->fps=jdbl(j,"fps",25.0); s->interlaced=jint(j,"interlaced",0);
        s->bit_depth=jint(j,"bit_depth",10);
        const char* idf=jstr(j,"ident_file",""); if (idf && *idf) { snprintf(s->ident_file,sizeof(s->ident_file),"%s",idf); s->has_ident=1; }
      } else if (s->kind == K_AUDIO) {
        s->channels=jint(j,"channels",8);
      } else {
        fprintf(stderr,"mtl_rx: session data (2110-40) pas encore implémentée, ignorée\n");
        continue;   /* point d'extension : setup_data() à venir */
      }
      if (!s->mcast[0] || !s->udp_port || !s->shm_path[0]) {
        fprintf(stderr,"mtl_rx: session %d incomplète, ignorée\n",k); continue; }
      ns++;
    }
    json_object_put(root);
  } else {
    /* legacy : 1 session vidéo depuis les args */
    if (!l_mcast || !l_port || !l_shm || !iface[0]) { usage(argv[0]); return 1; }
    struct sess* s = &S[0]; s->kind=K_VIDEO;
    snprintf(s->mcast,sizeof(s->mcast),"%s",l_mcast);
    s->udp_port=l_port; s->payload_type=l_pt; s->ring=l_ring; s->hdr=l_hdr;
    s->width=l_w; s->height=l_h; s->fps=l_fps; s->interlaced=l_inter; s->bit_depth=l_bd;
    snprintf(s->shm_path,sizeof(s->shm_path),"%s",l_shm);
    if (l_stats) snprintf(s->stats_path,sizeof(s->stats_path),"%s",l_stats);
    if (l_ident) { snprintf(s->ident_file,sizeof(s->ident_file),"%s",l_ident); s->has_ident=1; }
    ns = 1;
  }
  if (ns <= 0) { fprintf(stderr,"mtl_rx: aucune session valide\n"); return 1; }

  /* nom de port MTL selon le PMD */
  char portname[MTL_PORT_MAX_LEN];
  if (!strcmp(pmd,"af_xdp"))      snprintf(portname,sizeof(portname),"native_af_xdp:%s",iface);
  else if (!strcmp(pmd,"kernel")) snprintf(portname,sizeof(portname),"kernel:%s",iface);
  else                            snprintf(portname,sizeof(portname),"%s",iface);

  signal(SIGINT, on_signal); signal(SIGTERM, on_signal);

  /* ── mtl_init UNIQUE (PF/MtlManager/XDP partagés) ── */
  struct mtl_init_params p; memset(&p, 0, sizeof(p));
  p.num_ports = 1;
  snprintf(p.port[MTL_PORT_P], MTL_PORT_MAX_LEN, "%s", portname);
  if (sip[0]) inet_pton(AF_INET, sip, p.sip_addr[MTL_PORT_P]);
  p.pmd[MTL_PORT_P] = mtl_pmd_by_port_name(portname);
  p.flags |= MTL_FLAG_DEV_AUTO_START_STOP;
  p.log_level = MTL_LOG_LEVEL_INFO;
  p.lcores = lcores[0] ? lcores : NULL;
  p.rx_queues_cnt[MTL_PORT_P] = ns;     /* une file RX par session */
  p.tx_queues_cnt[MTL_PORT_P] = 1;      /* contrôle (IGMP/ARP) */

  mtl_handle st = mtl_init(&p);
  if (!st) { fprintf(stderr, "mtl_rx: mtl_init fail\n"); return 1; }

  int up = 0;
  for (int k = 0; k < ns; k++) {
    S[k].st = st;
    snprintf(S[k].portname, sizeof(S[k].portname), "%s", portname);
    int r = (S[k].kind == K_AUDIO) ? setup_audio(&S[k]) : setup_video(&S[k]);
    if (r == 0) up++;
    else fprintf(stderr, "mtl_rx: session %d échouée\n", k);
  }
  if (!up) { fprintf(stderr, "mtl_rx: aucune session démarrée\n"); mtl_uninit(st); return 1; }

  /* ── boucle de stats par session ── */
  uint64_t last[MAX_SESS]; memset(last, 0, sizeof(last));
  time_t last_t = time(NULL);
  while (!g_stop) {
    sleep(2);
    time_t now = time(NULL); double dt = difftime(now, last_t); last_t = now;
    for (int k = 0; k < ns; k++) {
      struct sess* s = &S[k];
      if (!s->started || !s->stats_path[0]) continue;
      double rate = dt > 0 ? (double)(s->recv - last[k]) / dt : 0.0;
      last[k] = s->recv;
      FILE* sf = fopen(s->stats_path, "w");
      if (sf) { fprintf(sf, "{\"fps\": %.1f, \"frame_index\": %llu}\n", rate,
                        (unsigned long long)s->index); fclose(sf); }
    }
  }

  g_stop = 1;
  for (int k = 0; k < ns; k++) {
    if (!S[k].started) continue;
    pthread_join(S[k].thread, NULL);
    if (S[k].kind == K_AUDIO) { if (S[k].ah) st30p_rx_free(S[k].ah); }
    else                      { if (S[k].vh) st20p_rx_free(S[k].vh); }
    if (S[k].shm_base && S[k].shm_base != MAP_FAILED) munmap(S[k].shm_base, S[k].shm_size);
    if (S[k].ident_patch) free(S[k].ident_patch);
  }
  mtl_uninit(st);
  return 0;
}
