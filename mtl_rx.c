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
#include <mtl/st40_pipeline_api.h>
#include <json-c/json.h>

#define MAX_FB    64
#define MAX_SESS  16
#define MAX_TG    16   /* cibles (shm de sortie) par session : fan-out même-source → N slots */

/* ── ANC / ST 2110-40 (data) ── un slot shm ANC sérialise un frame st40 (meta + udw). */
#define ANC_SLOT     8192u    /* taille d'un slot ANC (sérialisation bornée) */
#define ANC_MAX_UDW  4000u    /* buffer UDW max par frame (octets, 1 o = 1 UDW low8) */
/* En-tête de slot : [u32 meta_num][u32 udw_fill], puis meta_num × anc_meta_rec, puis udw_fill octets. */
struct anc_meta_rec { uint16_t did, sdid, line, hori, udw_size, udw_offset, c, s; };  /* 16 o */

enum sess_kind { K_VIDEO, K_AUDIO, K_DATA };   /* DATA = ST 2110-40 ANC (passthrough + timecode) */
enum sess_role { ROLE_RX, ROLE_TX };           /* RX = wire→shm (receiver) ; TX = shm→wire (sender) */

static volatile int g_stop = 0;
static void on_signal(int s) { (void)s; g_stop = 1; }

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* Une cible = un shm de sortie. Une session en porte 1..N (fan-out même-source → N slots).
 * Chaque cible a son propre état d'écriture (index/recv) et son propre IDENT (par slot). */
struct target {
  int      idx;            /* slot d'origine (pour le log) */
  char     shm_path[300], stats_path[300];
  uint8_t* shm_base; size_t shm_size; uint8_t* frames_base;
  uint64_t index;          /* frame_index (vidéo) / chunk_index (audio) */
  uint64_t recv;           /* compteur reçu (pour le débit) */
  char     ident_file[300]; int has_ident;
  uint8_t* ident_patch; int id_bw, id_bh; long id_mtime;
  /* timecode ATC (data/ANC) — dernier TC décodé, publié dans les stats */
  char     tc[16]; int tc_df; int tc_valid;
};

/* Une session = un flux réseau décodé UNE fois sur le mtl_handle partagé, fan-out vers ses cibles. */
struct sess {
  enum sess_kind kind;
  enum sess_role role;      /* RX (wire→shm) ou TX (shm→wire) */
  mtl_handle st;            /* partagé (mtl_init unique) */
  char portname[MTL_PORT_MAX_LEN];
  /* réseau */
  char mcast[64];
  int  udp_port, payload_type;
  /* décodage (partagé par toutes les cibles) */
  size_t   slotsize;       /* vidéo: framesize ; audio: 1152 (1 chunk 1ms) */
  int      ring, hdr;
  /* cibles (shm de sortie) */
  struct target tg[MAX_TG]; int ntg;
  pthread_t thread; int started;
  int      copy_mode;      /* vidéo: 1=memcpy (af_xdp) ; audio: toujours 1 */
  /* ── cycle de vie daemon (réconciliation à chaud) ── */
  volatile int stop;       /* arrêt PROPRE à cette session (thread boucle sur !stop) */
  int      used;           /* slot du registre occupé par une session vivante */
  int      seen;           /* marquage transitoire pendant un passage de réconciliation */
  char     sig[1024];      /* signature = identité+contenu ; un sig différent ⇒ recréer */
  /* ── vidéo ── */
  st20p_rx_handle vh;      /* RX */
  st20p_tx_handle vth;     /* TX */
  int      width, height, bit_depth, interlaced; double fps;
  int      tff;            /* entrelacé : 1=TFF (1080i), 0=BFF (576i) — parité des champs */
  size_t   src_framesize; int conv8;
  size_t   shm_slotsize;   /* taille d'un slot shm = TRAME PLEINE (≠ slotsize = taille CHAMP en
                              entrelacé, côté libmtl). 0 ⇒ open_shm retombe sur slotsize (audio/data). */
  enum st_frame_fmt out_fmt; int plane_h;   /* plane_h = hauteur de CHAMP (H/2) en entrelacé */
  /* ── audio ── */
  st30p_rx_handle ah;      /* RX */
  st30p_tx_handle a_tx;    /* TX */
  int      channels;
  double   a_ptime;        /* ptime audio (ms) du SDP/réglage : 1.0, 0.125, 0.25… → ST30_PTIME_* */
  /* ── data / ANC (2110-40) ── */
  st40p_rx_handle d_rx;    /* RX */
  st40p_tx_handle d_tx;    /* TX */
  uint32_t max_udw;        /* taille du buffer UDW (octets) */
};

static void write_shm_header(struct target* t, uint64_t i) {
  uint64_t* h = (uint64_t*)t->shm_base;
  h[0] = i; h[1] = now_ns();
}

/* ═══ VIDÉO ═══════════════════════════════════════════════════════════════════ */

static int frame_available(void* priv) { (void)priv; return 0; }

/* IDENT : recharge le patch (fichier écrit par le contrôleur) si mtime change. Coût ~nul si off.
 * Le patch est PAR CIBLE (chaque slot a son propre IDENT, même quand la source est partagée). */
static void load_ident_patch(struct target* t) {
  if (!t->has_ident) return;
  struct stat st;
  if (stat(t->ident_file, &st) != 0) {
    if (t->ident_patch) { free(t->ident_patch); t->ident_patch = NULL; }
    t->id_bw = t->id_bh = 0; t->id_mtime = 0; return;
  }
  if ((long)st.st_mtime == t->id_mtime && t->ident_patch) return;
  FILE* f = fopen(t->ident_file, "rb");
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
  if (t->ident_patch) free(t->ident_patch);
  t->ident_patch = p; t->id_bw = bw; t->id_bh = bh; t->id_mtime = (long)st.st_mtime;
}

static void overlay_ident(struct sess* s, struct target* t, uint8_t* dst) {
  if (!t->ident_patch || t->id_bw <= 0) return;
  int W = s->width, H = s->height, bw = t->id_bw, bh = t->id_bh;
  if (bw > W || bh > H) return;
  int x0 = W - bw - 8, y0 = 8;
  x0 -= x0 & 1; y0 -= y0 & 1; if (x0 < 0) x0 = 0;
  int deep = (s->bit_depth >= 10), shift = s->bit_depth - 8;
  size_t ysz = (size_t)W * H, uvsz = (size_t)(W / 2) * H;
  int bps = deep ? 2 : 1;
  for (int r = 0; r < bh; r++) {
    const uint8_t* p = t->ident_patch + (size_t)r * bw;
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

/* Entrelacé : pont entre un CHAMP libmtl (planar 422, 10-bit, plane_h = H/2 lignes, addr/linesize
 * par plan) et un slot shm TRAME PLEINE (planar 422 contigu Y|Cb|Cr, H lignes). Une ligne de champ
 * = une ligne sur deux de la trame, départ `parity` (0|1), stride 2.
 *   dir=0 : champ → trame (RX merge) ; dir=1 : trame → champ (TX split).
 *   shm8=1 : le shm est en 8 bits (conv8 RX / bit_depth 8 TX) → conversion 8↔10 par échantillon ;
 *            le buffer libmtl est TOUJOURS 10-bit (out/input_fmt YUV422PLANAR10LE).
 * 422 : plan 0 (Y) = W échantillons/ligne ; plans 1,2 (Cb,Cr) = W/2 ; tous H lignes en trame. */
static void field_weave(int W, int H, int shm8, struct st_frame* frame,
                        uint8_t* slot, int parity, int dir) {
  int fh = H / 2;
  int bps_shm = shm8 ? 1 : 2;
  size_t plane_off = 0;
  for (int p = 0; p < 3; p++) {
    int pw = p == 0 ? W : W / 2;                  /* échantillons/ligne du plan (422) */
    size_t shm_ls = (size_t)pw * bps_shm;          /* linesize shm (sans padding) */
    uint8_t* shm_p = slot + plane_off;
    uint8_t* fld_p = (uint8_t*)frame->addr[p];
    size_t fld_ls = frame->linesize[p];            /* linesize libmtl (peut avoir du padding) */
    for (int j = 0; j < fh; j++) {
      uint8_t* shm_l = shm_p + (size_t)(2 * j + parity) * shm_ls;
      uint8_t* fld_l = fld_p + (size_t)j * fld_ls;
      if (dir == 0) {                              /* RX : champ(10) → trame(shm) */
        if (shm8) { const uint16_t* sf = (const uint16_t*)fld_l;
                    for (int x = 0; x < pw; x++) shm_l[x] = (uint8_t)(sf[x] >> 2); }
        else memcpy(shm_l, fld_l, (size_t)pw * 2);
      } else {                                     /* TX : trame(shm) → champ(10) */
        if (shm8) { uint16_t* df = (uint16_t*)fld_l;
                    for (int x = 0; x < pw; x++) df[x] = (uint16_t)shm_l[x] << 2; }
        else memcpy(fld_l, shm_l, (size_t)pw * 2);
      }
    }
    plane_off += (size_t)pw * H * bps_shm;          /* plan suivant (trame pleine : H lignes) */
  }
}

/* parité (ligne de départ 0|1) d'un champ : second_field (lib) + ordre de champ (tff).
 * TFF : 1er champ→paires(0), 2e→impaires(1) ; BFF : inversé. DOIT être identique RX↔TX. */
static inline int field_parity(int second_field, int tff) {
  return (second_field ? 1 : 0) ^ (tff ? 0 : 1);
}

static void* video_rx_thread(void* arg) {
  struct sess* s = arg;
  while (!s->stop) {
    struct st_frame* frame = st20p_rx_get_frame(s->vh);
    if (!frame) { usleep(1000); continue; }
    if (!frame->addr[0]) { st20p_rx_put_frame(s->vh, frame); continue; }
    /* af_xdp/copy_mode : décodage unique → fan-out vers chaque cible (slot shm).
     * Chaque cible a son propre ring/index + son propre IDENT. */
    if (s->interlaced) {
      /* On reçoit UN CHAMP par appel ; libmtl pose frame->second_field. On weave les 2 champs dans
       * le MÊME slot (trame pleine) et on ne PUBLIE (header + index) qu'au 2e champ → les
       * consommateurs lisent toujours une trame complète, jamais une demi-trame. */
      int sf = frame->second_field ? 1 : 0;
      int parity = field_parity(sf, s->tff);
      for (int ti = 0; ti < s->ntg; ti++) {
        struct target* t = &s->tg[ti];
        uint64_t fi = t->index;                    /* index NON bumpé tant que la trame est partielle */
        uint8_t* dst = t->frames_base + (size_t)(fi % s->ring) * s->shm_slotsize;
        field_weave(s->width, s->height, s->conv8, frame, dst, parity, 0);
        if (sf) {                                  /* 2e champ : trame complète */
          if (t->has_ident) { load_ident_patch(t); overlay_ident(s, t, dst); }
          write_shm_header(t, fi);
          t->index = fi + 1; t->recv++;
        }
      }
    } else {
      for (int ti = 0; ti < s->ntg; ti++) {
        struct target* t = &s->tg[ti];
        uint64_t fi = t->index;
        uint8_t* dst = t->frames_base + (size_t)(fi % s->ring) * s->slotsize;
        if (s->conv8) {
          const uint16_t* src = (const uint16_t*)frame->addr[0];
          size_t n = s->slotsize;
          for (size_t k = 0; k < n; k++) dst[k] = (uint8_t)(src[k] >> 2);
        } else {
          memcpy(dst, frame->addr[0], s->slotsize);
        }
        if (t->has_ident) { load_ident_patch(t); overlay_ident(s, t, dst); }
        write_shm_header(t, fi);
        t->index = fi + 1; t->recv++;
      }
    }
    st20p_rx_put_frame(s->vh, frame);
  }
  return NULL;
}

/* ═══ AUDIO ═══════════════════════════════════════════════════════════════════ */
/* st30p délivre le payload L24 du fil = BIG-ENDIAN. Le pipeline MXL audio est désormais en BE
 * (wire-native) → on écrit le chunk TEL QUEL dans le ring, ZÉRO conversion (passthrough). */
static void* audio_rx_thread(void* arg) {
  struct sess* s = arg;
  while (!s->stop) {
    struct st30_frame* frame = st30p_rx_get_frame(s->ah);
    if (!frame) { usleep(500); continue; }
    size_t n = frame->data_size < s->slotsize ? frame->data_size : s->slotsize;
    for (int ti = 0; ti < s->ntg; ti++) {        /* fan-out (même source audio → N slots) */
      struct target* t = &s->tg[ti];
      uint64_t ci = t->index;
      uint8_t* dst = t->frames_base + (size_t)(ci % s->ring) * s->slotsize;   /* slotsize = 1152 */
      memcpy(dst, frame->addr, n);
      if (n < s->slotsize) memset(dst + n, 0, s->slotsize - n);
      write_shm_header(t, ci);
      t->index = ci + 1; t->recv++;
    }
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

/* ptime audio (ms) → enum ST30_PTIME. DOIT matcher le flux (a=ptime du SDP) sinon « pkt len
 * mismatch » → tous les paquets droppés. Défaut 1 ms. framebuff_size reste 1 ms (multiple du
 * paquet pour 1/0.25/0.125 ms → chunk shm inchangé). */
static enum st30_ptime to_st30_ptime(double ms) {
  if (fabs(ms - 0.125) < 0.01) return ST30_PTIME_125US;
  if (fabs(ms - 0.25)  < 0.01) return ST30_PTIME_250US;
  if (fabs(ms - 0.333) < 0.02) return ST30_PTIME_333US;
  if (fabs(ms - 4.0)   < 0.05) return ST30_PTIME_4MS;
  return ST30_PTIME_1MS;
}

/* mmap (création + ftruncate) du shm d'une cible, header à l'offset 0. Taille = hdr + ring*slot
 * (dimensions de la session, communes à toutes ses cibles). */
static int open_shm(struct sess* s, struct target* t) {
  /* Slot = TRAME PLEINE (shm_slotsize) en vidéo entrelacée ; sinon (progressif/audio/data)
   * shm_slotsize vaut 0 → on retombe sur slotsize (comportement historique). */
  size_t slot = s->shm_slotsize ? s->shm_slotsize : s->slotsize;
  size_t raw = (size_t)s->hdr + (size_t)s->ring * slot;
  size_t pg = (size_t)sysconf(_SC_PAGESIZE);
  t->shm_size = (raw + pg - 1) & ~(pg - 1);
  int fd = open(t->shm_path, O_CREAT | O_RDWR, 0666);
  if (fd < 0) { perror("open shm"); return -1; }
  if (ftruncate(fd, t->shm_size) < 0) { perror("ftruncate"); close(fd); return -1; }
  t->shm_base = mmap(NULL, t->shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (t->shm_base == MAP_FAILED) { perror("mmap"); return -1; }
  t->frames_base = t->shm_base + s->hdr;
  return 0;
}

/* Ouvre toutes les cibles de la session (slotsize/ring/hdr déjà calculés). */
static int open_targets(struct sess* s) {
  for (int ti = 0; ti < s->ntg; ti++)
    if (open_shm(s, &s->tg[ti]) != 0) return -1;
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
  s->src_framesize = st20p_rx_frame_size(s->vh);   /* = taille CHAMP si entrelacé (st_frame_size/2) */
  s->slotsize = s->conv8 ? s->src_framesize / 2 : s->src_framesize;
  /* Slot shm = TRAME PLEINE : en entrelacé on weave 2 champs (×2) ; en progressif = slotsize. */
  s->shm_slotsize = s->interlaced ? s->slotsize * 2 : s->slotsize;
  if (open_targets(s) != 0) return -1;
  fprintf(stderr, "mtl_rx[video] %dx%d%s fps=%.2f pt=%d mc=%s:%d slot=%zu ring=%d → %d cible(s):",
          s->width, s->height, s->interlaced ? "i" : "p", s->fps, s->payload_type,
          s->mcast, s->udp_port, s->slotsize, s->ring, s->ntg);
  for (int ti = 0; ti < s->ntg; ti++) fprintf(stderr, " %s", s->tg[ti].shm_path);
  fprintf(stderr, "\n");
  return pthread_create(&s->thread, NULL, video_rx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

/* ═══ VIDÉO TX (shm→wire, st20p_tx) ═══════════════════════════════════════════ */
/* Ouvre un shm EXISTANT en lecture (le shm d'entrée du TX, écrit par un producteur). Ne crée ni ne
 * tronque (ne pas resizer le shm du producteur). Renvoie 0 si mappé et assez grand. */
static int open_shm_in(struct sess* s, struct target* t, size_t want) {
  char path[320];   /* défensif : un nom relatif (ex. "mtl_0") → /dev/shm/mtl_0 */
  if (t->shm_path[0] == '/') snprintf(path, sizeof(path), "%s", t->shm_path);
  else snprintf(path, sizeof(path), "/dev/shm/%s", t->shm_path);
  int fd = open(path, O_RDWR);
  if (fd < 0) return -1;
  struct stat stt;
  if (fstat(fd, &stt) != 0 || (size_t)stt.st_size < want) { close(fd); return -1; }
  t->shm_size = (size_t)stt.st_size;
  t->shm_base = mmap(NULL, t->shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (t->shm_base == MAP_FAILED) { t->shm_base = NULL; return -1; }
  t->frames_base = t->shm_base + s->hdr;
  return 0;
}

/* Feeder TX : lit la frame courante du shm d'entrée (header [index,ts], ring) et l'émet. Up-shift
 * 8→10 si le pipeline est en 8 bits (transport 2110-20 = 422-10). Le shm d'entrée est mappé
 * PARESSEUSEMENT (le producteur peut démarrer après nous). Pacing assuré par ST20P_TX_FLAG_BLOCK_GET. */
static void* video_tx_thread(void* arg) {
  struct sess* s = arg;
  struct target* t = &s->tg[0];                 /* la cible TX = l'unique shm d'entrée */
  size_t out_size = st20p_tx_frame_size(s->vth);   /* = taille CHAMP si entrelacé */
  uint64_t latched_fi = 0;                      /* entrelacé : index latché sur le 1er champ */
  while (!s->stop) {
    if (!t->shm_base) {
      if (open_shm_in(s, t, (size_t)s->hdr + s->slotsize) != 0) { usleep(20000); continue; }
    }
    struct st_frame* frame = st20p_tx_get_frame(s->vth);   /* bloque → pacing à fps */
    if (!frame) { usleep(1000); continue; }
    if (s->interlaced) {
      /* Un CHAMP par appel (libmtl pose/alterne second_field). Les 2 champs d'une trame viennent du
       * MÊME slot shm → on latche l'index sur le 1er champ, réutilisé pour le 2e (évite le combing si
       * le producteur avance entre les deux get_frame). On dé-weave la trame pleine en lignes. */
      int sf = frame->second_field ? 1 : 0;
      int parity = field_parity(sf, s->tff);
      uint64_t fi = sf ? latched_fi : (latched_fi = ((volatile uint64_t*)t->shm_base)[0]);
      long slot = (long)(fi % s->ring);
      uint8_t* src = t->frames_base + (size_t)slot * s->slotsize;   /* slot = TRAME PLEINE */
      field_weave(s->width, s->height, (s->bit_depth == 8), frame, src, parity, 1);
      st20p_tx_put_frame(s->vth, frame);
      t->index = fi; t->recv++;
    } else {
      uint64_t fi = ((volatile uint64_t*)t->shm_base)[0];
      long slot = (long)(fi % s->ring);
      const uint8_t* src = t->frames_base + (size_t)slot * s->slotsize;
      uint8_t* dst = (uint8_t*)frame->addr[0];
      if (s->bit_depth == 8) {
        uint16_t* d16 = (uint16_t*)dst;
        size_t n = s->slotsize;                 /* 8 bits : slotsize octets = n échantillons */
        for (size_t k = 0; k < n; k++) d16[k] = (uint16_t)src[k] << 2;   /* 8→10 */
      } else {
        memcpy(dst, src, out_size < s->slotsize ? out_size : s->slotsize);
      }
      st20p_tx_put_frame(s->vth, frame);
      t->index = fi; t->recv++;
    }
  }
  return NULL;
}

static int setup_video_tx(struct sess* s) {
  if (s->ring < 2) s->ring = 2;                 /* ring du shm d'ENTRÉE (réglage du producteur) */
  /* taille d'un slot du shm d'entrée (422 planar : 8b = 2·w·h octets, 10b = 4·w·h octets) */
  s->slotsize = (size_t)(s->bit_depth == 8 ? 2 : 4) * (size_t)s->width * (size_t)s->height;

  struct st20p_tx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_vtx";
  ops.priv = s;
  ops.port.num_port = 1;
  inet_pton(AF_INET, s->mcast, ops.port.dip_addr[MTL_SESSION_PORT_P]);   /* destination */
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  ops.port.payload_type = s->payload_type;
  ops.width = s->width; ops.height = s->height; ops.fps = to_st_fps(s->fps);
  ops.interlaced = s->interlaced ? true : false;
  ops.input_fmt = ST_FRAME_FMT_YUV422PLANAR10LE;   /* on fournit du 10-bit (up-shift 8→10 nous-mêmes) */
  ops.transport_fmt = ST20_FMT_YUV_422_10BIT;
  ops.device = ST_PLUGIN_DEVICE_AUTO;
  ops.framebuff_cnt = 3;
  ops.flags = ST20P_TX_FLAG_BLOCK_GET;             /* get_frame bloque → pacing à fps */

  s->vth = st20p_tx_create(s->st, &ops);
  if (!s->vth) { fprintf(stderr, "mtl_rx: st20p_tx_create fail (video %s:%d)\n", s->mcast, s->udp_port); return -1; }
  fprintf(stderr, "mtl_rx[video TX] %dx%d%s fps=%.2f pt=%d → %s:%d (in shm=%s bd%d ring%d)\n",
          s->width, s->height, s->interlaced ? "i" : "p", s->fps, s->payload_type,
          s->mcast, s->udp_port, s->tg[0].shm_path, s->bit_depth, s->ring);
  return pthread_create(&s->thread, NULL, video_tx_thread, s) == 0 ? (s->started = 1, 0) : -1;
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
  ops.ptime = to_st30_ptime(s->a_ptime);   /* AUTO depuis le SDP (a=ptime) / défaut réglage */
  ops.framebuff_size = (uint32_t)s->slotsize;   /* 1 chunk = 1 ms */
  ops.framebuff_cnt = 4;

  s->ah = st30p_rx_create(s->st, &ops);
  if (!s->ah) { fprintf(stderr, "mtl_rx: st30p_rx_create fail (audio %s:%d)\n", s->mcast, s->udp_port); return -1; }
  if (open_targets(s) != 0) return -1;
  fprintf(stderr, "mtl_rx[audio] %dch L24/48k pt=%d mc=%s:%d slot=%zu ring=%d → %d cible(s):",
          s->channels, s->payload_type, s->mcast, s->udp_port, s->slotsize, s->ring, s->ntg);
  for (int ti = 0; ti < s->ntg; ti++) fprintf(stderr, " %s", s->tg[ti].shm_path);
  fprintf(stderr, "\n");
  return pthread_create(&s->thread, NULL, audio_rx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

/* ═══ AUDIO TX (shm→wire, st30p_tx) ═══════════════════════════════════════════ */
/* Feeder audio TX : lit le chunk courant du shm d'entrée (L24 BE = wire-native) et l'émet TEL QUEL
 * (passthrough, zéro conversion — le shm MXL audio est déjà en BE). shm mappé paresseusement. */
static void* audio_tx_thread(void* arg) {
  struct sess* s = arg;
  struct target* t = &s->tg[0];
  while (!s->stop) {
    if (!t->shm_base) {
      if (open_shm_in(s, t, (size_t)s->hdr + s->slotsize) != 0) { usleep(20000); continue; }
    }
    struct st30_frame* frame = st30p_tx_get_frame(s->a_tx);   /* bloque (BLOCK_GET) → pacing 1ms */
    if (!frame) { usleep(500); continue; }
    uint64_t ci = ((volatile uint64_t*)t->shm_base)[0];
    long slot = (long)(ci % s->ring);
    const uint8_t* src = t->frames_base + (size_t)slot * s->slotsize;
    size_t n = frame->data_size < s->slotsize ? frame->data_size : s->slotsize;
    memcpy(frame->addr, src, n);                              /* BE shm → BE fil = passthrough */
    st30p_tx_put_frame(s->a_tx, frame);
    t->index = ci; t->recv++;
  }
  return NULL;
}

static int setup_audio_tx(struct sess* s) {
  if (s->channels <= 0) s->channels = 8;
  if (s->ring < 2) s->ring = 2;
  s->slotsize = (size_t)(48000 / 1000) * s->channels * 3;   /* 1 ms L24 = 1152 si 8ch */

  struct st30p_tx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_atx";
  ops.priv = s;
  ops.port.num_port = 1;
  inet_pton(AF_INET, s->mcast, ops.port.dip_addr[MTL_SESSION_PORT_P]);   /* destination */
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  ops.port.payload_type = s->payload_type;
  ops.fmt = ST30_FMT_PCM24;
  ops.channel = (uint16_t)s->channels;
  ops.sampling = ST30_SAMPLING_48K;
  ops.ptime = to_st30_ptime(s->a_ptime);   /* AUTO depuis le SDP (a=ptime) / défaut réglage */
  ops.framebuff_size = (uint32_t)s->slotsize;
  ops.framebuff_cnt = 4;
  ops.flags = ST30P_TX_FLAG_BLOCK_GET;

  s->a_tx = st30p_tx_create(s->st, &ops);
  if (!s->a_tx) { fprintf(stderr, "mtl_rx: st30p_tx_create fail (audio %s:%d)\n", s->mcast, s->udp_port); return -1; }
  fprintf(stderr, "mtl_rx[audio TX] %dch L24/48k pt=%d → %s:%d (in shm=%s)\n",
          s->channels, s->payload_type, s->mcast, s->udp_port, s->tg[0].shm_path);
  return pthread_create(&s->thread, NULL, audio_tx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

/* ═══ DATA / ANC — ST 2110-40 (st40p) ═════════════════════════════════════════ */
/* Le pipeline st40p présente udw_buff_addr comme un tableau d'OCTETS (1 o = 1 UDW, low 8 bits,
 * parité déjà vérifiée à la réception) ; meta[k].udw_offset = offset OCTET dans ce tableau,
 * meta[k].udw_size = nombre de UDW. RX et TX utilisent ce MÊME format → passthrough = copie. */

/* Décode l'ATC (SMPTE ST 12-1 / RP 188, DID 0x60 SDID 0x60). 16 UDW → 8 octets de timecode :
 * octet[i] = low-nibble(UDW[2i]) | low-nibble(UDW[2i+1])<<4. Octets 0/2/4/6 = frames/sec/min/h
 * (BCD : unités b0-3, dizaines b4+). DF = bit 6 de l'octet 0. Renvoie 1 si un ATC est trouvé. */
static int decode_atc(struct st40_frame_info* f, char* out, int* df) {
  for (uint32_t m = 0; m < f->meta_num; m++) {
    struct st40_meta* md = &f->meta[m];
    if (md->did != 0x60 || md->sdid != 0x60) continue;
    if (md->udw_size < 16) continue;
    const uint8_t* w = f->udw_buff_addr + md->udw_offset;   /* 1 octet = 1 UDW (low 8 bits) */
    uint8_t b[8];
    for (int i = 0; i < 8; i++) b[i] = (uint8_t)((w[i*2] & 0x0f) | ((w[i*2+1] & 0x0f) << 4));
    int frames  = (b[0] & 0x0f) + ((b[0] >> 4) & 0x03) * 10;
    int seconds = (b[2] & 0x0f) + ((b[2] >> 4) & 0x07) * 10;
    int minutes = (b[4] & 0x0f) + ((b[4] >> 4) & 0x07) * 10;
    int hours   = (b[6] & 0x0f) + ((b[6] >> 4) & 0x03) * 10;
    *df = (b[0] >> 6) & 0x01;
    snprintf(out, 16, "%02d:%02d:%02d%c%02d", hours, minutes, seconds, *df ? ';' : ':', frames);
    return 1;
  }
  return 0;
}

/* Feeder RX ANC : st40p_rx_get_frame → sérialise meta[] + udw dans le slot courant (fan-out),
 * + extraction du timecode ATC publié dans les stats. */
static void* data_rx_thread(void* arg) {
  struct sess* s = arg;
  while (!s->stop) {
    struct st40_frame_info* frame = st40p_rx_get_frame(s->d_rx);
    if (!frame) { usleep(1000); continue; }
    uint32_t mn = frame->meta_num; if (mn > ST40_MAX_META) mn = ST40_MAX_META;
    uint32_t fill = frame->udw_buffer_fill; if (fill > s->max_udw) fill = s->max_udw;
    size_t need = 8 + (size_t)mn * sizeof(struct anc_meta_rec) + fill;
    char tc[16]; int df = 0; int got_tc = decode_atc(frame, tc, &df);
    for (int ti = 0; ti < s->ntg; ti++) {
      struct target* t = &s->tg[ti];
      uint64_t ci = t->index;
      uint8_t* dst = t->frames_base + (size_t)(ci % s->ring) * s->slotsize;
      if (need <= s->slotsize) {
        ((uint32_t*)dst)[0] = mn; ((uint32_t*)dst)[1] = fill;
        struct anc_meta_rec* mr = (struct anc_meta_rec*)(dst + 8);
        for (uint32_t m = 0; m < mn; m++) {
          struct st40_meta* md = &frame->meta[m];
          mr[m].did = md->did; mr[m].sdid = md->sdid;
          mr[m].line = md->line_number; mr[m].hori = md->hori_offset;
          mr[m].udw_size = md->udw_size; mr[m].udw_offset = md->udw_offset;
          mr[m].c = md->c; mr[m].s = md->s;
        }
        if (fill) memcpy(dst + 8 + (size_t)mn * sizeof(struct anc_meta_rec), frame->udw_buff_addr, fill);
      } else {   /* frame anormalement gros → slot vide (on ne déborde jamais) */
        ((uint32_t*)dst)[0] = 0; ((uint32_t*)dst)[1] = 0;
      }
      if (got_tc) { memcpy(t->tc, tc, sizeof(t->tc)); t->tc_df = df; t->tc_valid = 1; }
      write_shm_header(t, ci);
      t->index = ci + 1; t->recv++;
    }
    st40p_rx_put_frame(s->d_rx, frame);
  }
  return NULL;
}

/* Feeder TX ANC : lit le slot courant du shm d'entrée → reconstruit meta[]+udw → st40p_tx_put_frame.
 * Passthrough intégral (les udw_offset restent valides : on recopie tout le buffer udw verbatim). */
static void* data_tx_thread(void* arg) {
  struct sess* s = arg;
  struct target* t = &s->tg[0];
  while (!s->stop) {
    if (!t->shm_base) {
      if (open_shm_in(s, t, (size_t)s->hdr + s->slotsize) != 0) { usleep(20000); continue; }
    }
    struct st40_frame_info* frame = st40p_tx_get_frame(s->d_tx);   /* bloque (BLOCK_GET) → pacing fps */
    if (!frame) { usleep(1000); continue; }
    uint64_t ci = ((volatile uint64_t*)t->shm_base)[0];
    const uint8_t* src = t->frames_base + (size_t)(ci % s->ring) * s->slotsize;
    uint32_t mn = ((const uint32_t*)src)[0]; if (mn > ST40_MAX_META) mn = 0;
    uint32_t fill = ((const uint32_t*)src)[1]; if (fill > s->max_udw) fill = s->max_udw;
    const struct anc_meta_rec* mr = (const struct anc_meta_rec*)(src + 8);
    frame->meta_num = mn;
    for (uint32_t m = 0; m < mn; m++) {
      struct st40_meta* md = &frame->meta[m];
      md->did = mr[m].did; md->sdid = mr[m].sdid;
      md->line_number = mr[m].line; md->hori_offset = mr[m].hori;
      md->udw_size = mr[m].udw_size; md->udw_offset = mr[m].udw_offset;
      md->c = mr[m].c; md->s = mr[m].s; md->stream_num = 0;
    }
    if (fill && frame->udw_buff_addr)
      memcpy(frame->udw_buff_addr, src + 8 + (size_t)mn * sizeof(struct anc_meta_rec), fill);
    frame->udw_buffer_fill = fill;
    st40p_tx_put_frame(s->d_tx, frame);
    t->index = ci; t->recv++;
  }
  return NULL;
}

static int setup_data(struct sess* s) {
  if (s->ring > 8) s->ring = 8; if (s->ring < 2) s->ring = 2;
  s->slotsize = ANC_SLOT; s->max_udw = ANC_MAX_UDW; s->copy_mode = 1;

  struct st40p_rx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_d";
  ops.priv = s;
  ops.port.num_port = 1;
  inet_pton(AF_INET, s->mcast, ops.port.ip_addr[MTL_SESSION_PORT_P]);
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  ops.port.payload_type = s->payload_type;
  ops.framebuff_cnt = 4;
  ops.max_udw_buff_size = s->max_udw;
  ops.rtp_ring_size = 1024;   /* requis (>0, puissance de 2 : ring DPDK des paquets RTP ANC) */
  ops.flags = ST40P_RX_FLAG_BLOCK_GET;

  s->d_rx = st40p_rx_create(s->st, &ops);
  if (!s->d_rx) { fprintf(stderr, "mtl_rx: st40p_rx_create fail (data %s:%d)\n", s->mcast, s->udp_port); return -1; }
  if (open_targets(s) != 0) return -1;
  fprintf(stderr, "mtl_rx[data] ANC pt=%d mc=%s:%d slot=%zu ring=%d → %d cible(s):",
          s->payload_type, s->mcast, s->udp_port, s->slotsize, s->ring, s->ntg);
  for (int ti = 0; ti < s->ntg; ti++) fprintf(stderr, " %s", s->tg[ti].shm_path);
  fprintf(stderr, "\n");
  return pthread_create(&s->thread, NULL, data_rx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

static int setup_data_tx(struct sess* s) {
  if (s->ring < 2) s->ring = 2;
  s->slotsize = ANC_SLOT; s->max_udw = ANC_MAX_UDW;

  struct st40p_tx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_dtx";
  ops.priv = s;
  ops.port.num_port = 1;
  inet_pton(AF_INET, s->mcast, ops.port.dip_addr[MTL_SESSION_PORT_P]);   /* destination */
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  ops.port.payload_type = s->payload_type;
  ops.fps = to_st_fps(s->fps);
  ops.interlaced = false;
  ops.framebuff_cnt = 4;
  ops.max_udw_buff_size = s->max_udw;
  ops.flags = ST40P_TX_FLAG_BLOCK_GET;

  s->d_tx = st40p_tx_create(s->st, &ops);
  if (!s->d_tx) { fprintf(stderr, "mtl_rx: st40p_tx_create fail (data %s:%d)\n", s->mcast, s->udp_port); return -1; }
  fprintf(stderr, "mtl_rx[data TX] ANC fps=%.2f pt=%d → %s:%d (in shm=%s)\n",
          s->fps, s->payload_type, s->mcast, s->udp_port, s->tg[0].shm_path);
  return pthread_create(&s->thread, NULL, data_tx_thread, s) == 0 ? (s->started = 1, 0) : -1;
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

/* Remplit une cible depuis un objet JSON {idx, shm, stats, ident_file}. Renvoie 0 si shm valide. */
static int parse_target(struct json_object* j, struct target* t) {
  t->idx = jint(j, "idx", 0);
  snprintf(t->shm_path, sizeof(t->shm_path), "%s", jstr(j, "shm", ""));
  snprintf(t->stats_path, sizeof(t->stats_path), "%s", jstr(j, "stats", ""));
  const char* idf = jstr(j, "ident_file", "");
  if (idf && *idf) { snprintf(t->ident_file, sizeof(t->ident_file), "%s", idf); t->has_ident = 1; }
  return t->shm_path[0] ? 0 : -1;
}

static void usage(const char* p) {
  fprintf(stderr,
    "usage: %s --config <json>   (DAEMON : sessions réconciliées à chaud, video st20 / audio st30)\n"
    "   ou : %s [args legacy 1 session video] (--pmd af_xdp --iface .. --mcast .. --shm .. ...)\n", p, p);
}

/* Remplit une session depuis un objet JSON. 0 si valide (mcast + port + ≥1 cible). */
static int parse_session_into(struct json_object* j, struct sess* s) {
  memset(s, 0, sizeof(*s));
  const char* kind = jstr(j,"kind","video");
  s->kind = !strcmp(kind,"audio") ? K_AUDIO : !strcmp(kind,"data") ? K_DATA : K_VIDEO;
  s->role = !strcmp(jstr(j,"role","rx"), "tx") ? ROLE_TX : ROLE_RX;
  snprintf(s->mcast,sizeof(s->mcast),"%s",jstr(j,"mcast",""));
  s->udp_port=jint(j,"udp_port",0); s->payload_type=jint(j,"payload_type",96);
  s->ring=jint(j,"ring", s->kind==K_AUDIO?100:8); s->hdr=jint(j,"hdr",64);
  if (s->kind == K_VIDEO) {
    s->width=jint(j,"width",1920); s->height=jint(j,"height",1080);
    s->fps=jdbl(j,"fps",25.0); s->interlaced=jint(j,"interlaced",0);
    s->tff = strcmp(jstr(j,"field_order","tff"), "bff") != 0;   /* défaut TFF ; "bff" → 0 */
    s->bit_depth=jint(j,"bit_depth",10);
  } else if (s->kind == K_AUDIO) {
    s->channels=jint(j,"channels",8);
    s->a_ptime=jdbl(j,"ptime",1.0);   /* ms ; doit matcher le flux (a=ptime du SDP) */
  } else {   /* K_DATA / ANC : seul fps compte (pacing TX) */
    s->fps=jdbl(j,"fps",25.0);
  }
  struct json_object* tgs;
  if (json_object_object_get_ex(j,"targets",&tgs) && json_object_is_type(tgs,json_type_array)) {
    int nt = json_object_array_length(tgs);
    for (int ti = 0; ti < nt && s->ntg < MAX_TG; ti++)
      if (parse_target(json_object_array_get_idx(tgs, ti), &s->tg[s->ntg]) == 0) s->ntg++;
  } else {
    if (parse_target(j, &s->tg[0]) == 0) s->ntg = 1;   /* compat : shm/stats/ident_file inline */
  }
  return (s->mcast[0] && s->udp_port && s->ntg > 0) ? 0 : -1;
}

/* Signature = identité réseau + format + cibles. Un sig différent ⇒ on libère l'ancienne session et
 * on en recrée une (flow RX recyclé, device/XDP intacts ⇒ pas de faute PTP). */
static void compute_sig(struct sess* s) {
  int n = snprintf(s->sig, sizeof(s->sig), "%d|%d|%s|%d|%d|%dx%d|%.2f|i%d|f%d|bd%d|r%d|ch%d|ap%.3f|",
                   s->role, s->kind, s->mcast, s->udp_port, s->payload_type,
                   s->width, s->height, s->fps, s->interlaced, s->tff, s->bit_depth, s->ring,
                   s->channels, s->a_ptime);
  for (int ti = 0; ti < s->ntg && n > 0 && n < (int)sizeof(s->sig); ti++)
    n += snprintf(s->sig + n, sizeof(s->sig) - n, "%s>%s,",
                  s->tg[ti].shm_path, s->tg[ti].has_ident ? s->tg[ti].ident_file : "-");
}

/* Libère une session vivante : arrêt PROPRE du thread, free du handle MTL (le flow RX), munmap des
 * cibles. Le device (mtl_init/XDP) n'est PAS touché ⇒ aucune faute PTP. */
static void free_session(struct sess* s) {
  if (!s->used) return;
  if (s->started) { s->stop = 1; pthread_join(s->thread, NULL); }
  if (s->role == ROLE_TX) {
    if (s->kind == K_AUDIO)     { if (s->a_tx) st30p_tx_free(s->a_tx); }
    else if (s->kind == K_DATA) { if (s->d_tx) st40p_tx_free(s->d_tx); }
    else                        { if (s->vth)  st20p_tx_free(s->vth); }
  } else if (s->kind == K_AUDIO) { if (s->ah)  st30p_rx_free(s->ah); }
  else if (s->kind == K_DATA)    { if (s->d_rx) st40p_rx_free(s->d_rx); }
  else                           { if (s->vh)  st20p_rx_free(s->vh); }
  for (int ti = 0; ti < s->ntg; ti++) {
    struct target* t = &s->tg[ti];
    if (t->shm_base && t->shm_base != MAP_FAILED) munmap(t->shm_base, t->shm_size);
    if (t->ident_patch) free(t->ident_patch);
  }
  memset(s, 0, sizeof(*s));
}

/* Réconcilie le registre des sessions vivantes avec le config (sessions désirées), À CHAUD sur le
 * mtl_handle vivant : libère les disparues, crée les nouvelles. JAMAIS de mtl_uninit. */
static void reconcile(struct sess* reg, const char* path, mtl_handle st, const char* portname) {
  struct json_object* root = json_object_from_file(path);
  if (!root) return;
  struct json_object* arr;
  if (!json_object_object_get_ex(root,"sessions",&arr) || !json_object_is_type(arr,json_type_array)) {
    json_object_put(root); return;
  }
  for (int i = 0; i < MAX_SESS; i++) reg[i].seen = 0;
  int n = json_object_array_length(arr);
  for (int k = 0; k < n; k++) {
    struct sess want;
    if (parse_session_into(json_object_array_get_idx(arr, k), &want) != 0) continue;
    compute_sig(&want);
    int found = -1;
    for (int i = 0; i < MAX_SESS; i++)
      if (reg[i].used && !strcmp(reg[i].sig, want.sig)) { found = i; break; }
    if (found >= 0) { reg[found].seen = 1; continue; }      /* inchangée → on garde telle quelle */
    int slot = -1;
    for (int i = 0; i < MAX_SESS; i++) if (!reg[i].used) { slot = i; break; }
    if (slot < 0) { fprintf(stderr,"mtl_rx: registre plein, session ignorée\n"); continue; }
    struct sess* s = &reg[slot];
    *s = want;
    s->st = st; snprintf(s->portname, sizeof(s->portname), "%s", portname); s->stop = 0;
    int r = (s->role == ROLE_TX)
            ? ((s->kind == K_AUDIO) ? setup_audio_tx(s) : (s->kind == K_DATA) ? setup_data_tx(s) : setup_video_tx(s))
            : ((s->kind == K_AUDIO) ? setup_audio(s)    : (s->kind == K_DATA) ? setup_data(s)    : setup_video(s));
    if (r == 0) { s->used = 1; s->seen = 1; }
    else { fprintf(stderr,"mtl_rx: création session %s:%d échouée\n", s->mcast, s->udp_port); memset(s,0,sizeof(*s)); }
  }
  for (int i = 0; i < MAX_SESS; i++)
    if (reg[i].used && !reg[i].seen) {
      fprintf(stderr,"mtl_rx: retrait session %s:%d\n", reg[i].mcast, reg[i].udp_port);
      free_session(&reg[i]);
    }
  json_object_put(root);
}

/* Écrit le fichier de stats {fps, frame_index} de chaque cible vivante. */
static void write_stats(struct sess* reg, uint64_t last[][MAX_TG], double dt) {
  for (int i = 0; i < MAX_SESS; i++) {
    struct sess* s = &reg[i];
    if (!s->used || !s->started) continue;
    for (int ti = 0; ti < s->ntg; ti++) {
      struct target* t = &s->tg[ti];
      if (!t->stats_path[0]) continue;
      double rate = dt > 0 ? (double)(t->recv - last[i][ti]) / dt : 0.0;
      last[i][ti] = t->recv;
      FILE* sf = fopen(t->stats_path, "w");
      if (sf) {
        if (s->kind == K_DATA && t->tc_valid)
          fprintf(sf, "{\"fps\": %.1f, \"frame_index\": %llu, \"timecode\": \"%s\", \"df\": %s}\n",
                  rate, (unsigned long long)t->index, t->tc, t->tc_df ? "true" : "false");
        else
          fprintf(sf, "{\"fps\": %.1f, \"frame_index\": %llu}\n", rate, (unsigned long long)t->index);
        fclose(sf);
      }
    }
  }
}

int main(int argc, char** argv) {
  /* globaux partagés */
  char pmd[32] = "af_xdp", iface[64] = "", sip[64] = "", lcores[128] = "";
  const char* config = NULL;

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

  /* nom de port MTL selon le PMD (rempli après lecture iface/pmd) */
  char portname[MTL_PORT_MAX_LEN];
  signal(SIGINT, on_signal); signal(SIGTERM, on_signal);
  static struct sess reg[MAX_SESS]; memset(reg, 0, sizeof(reg));
  static uint64_t last[MAX_SESS][MAX_TG]; memset(last, 0, sizeof(last));

  if (config) {
    /* ═══ DAEMON ═══ mtl_init UNE fois (à vie), puis réconciliation des sessions à chaud.
     * Le device reste démarré (XDP attaché) tant que le process vit → ptp4l ne faute qu'au boot. */
    struct json_object* root = json_object_from_file(config);
    if (!root) { fprintf(stderr, "mtl_rx: config illisible: %s\n", config); return 1; }
    snprintf(pmd,sizeof(pmd),"%s",jstr(root,"pmd","af_xdp"));
    snprintf(iface,sizeof(iface),"%s",jstr(root,"iface",""));
    snprintf(sip,sizeof(sip),"%s",jstr(root,"sip",""));
    snprintf(lcores,sizeof(lcores),"%s",jstr(root,"lcores",""));
    int rx_q = jint(root,"rx_queues", 8);   /* plafond de sessions RX (= rx_count du moteur) */
    int tx_q = jint(root,"tx_queues", 1);   /* TX : Phase 2 (1 = file de contrôle IGMP/ARP) */
    json_object_put(root);
    if (!iface[0]) { fprintf(stderr,"mtl_rx: config sans iface\n"); return 1; }

    if (!strcmp(pmd,"af_xdp"))      snprintf(portname,sizeof(portname),"native_af_xdp:%s",iface);
    else if (!strcmp(pmd,"kernel")) snprintf(portname,sizeof(portname),"kernel:%s",iface);
    else                            snprintf(portname,sizeof(portname),"%s",iface);

    struct mtl_init_params p; memset(&p, 0, sizeof(p));
    p.num_ports = 1;
    snprintf(p.port[MTL_PORT_P], MTL_PORT_MAX_LEN, "%s", portname);
    if (sip[0]) inet_pton(AF_INET, sip, p.sip_addr[MTL_PORT_P]);
    p.pmd[MTL_PORT_P] = mtl_pmd_by_port_name(portname);
    /* AUTO_START_STOP : en AF_XDP le flow/XSK se crée AVEC le démarrage du device, déclenché par le
     * 1ᵉʳ st20p_rx_create (impossible de pré-démarrer un device vide : « add flow fail »). Le device
     * se start/stop au gré du nombre de sessions, mais le XDP reste attaché tant que mtl_init vit
     * (le détache n'a lieu qu'à mtl_uninit) → ptp4l ne faute qu'au boot, pas sur les changements. */
    p.flags |= MTL_FLAG_DEV_AUTO_START_STOP;
    p.log_level = MTL_LOG_LEVEL_INFO;
    p.lcores = lcores[0] ? lcores : NULL;
    p.rx_queues_cnt[MTL_PORT_P] = rx_q > 0 ? rx_q : 1;
    p.tx_queues_cnt[MTL_PORT_P] = tx_q > 0 ? tx_q : 1;

    mtl_handle st = mtl_init(&p);
    if (!st) { fprintf(stderr, "mtl_rx: mtl_init fail\n"); return 1; }
    fprintf(stderr, "mtl_rx: daemon up (iface=%s rx_q=%d tx_q=%d) — réconciliation à chaud\n",
            iface, rx_q, tx_q);

    long cfg_mtime = 0; struct stat cst;
    reconcile(reg, config, st, portname);                 /* état initial */
    if (stat(config, &cst) == 0) cfg_mtime = (long)cst.st_mtime;
    time_t last_t = time(NULL);
    while (!g_stop) {
      for (int z = 0; z < 5 && !g_stop; z++) usleep(100000);   /* ~0.5s, réactif au SIGTERM */
      if (g_stop) break;
      struct stat cs;
      if (stat(config, &cs) == 0 && (long)cs.st_mtime != cfg_mtime) {
        cfg_mtime = (long)cs.st_mtime;
        reconcile(reg, config, st, portname);             /* config changé → converge à chaud */
      }
      time_t now = time(NULL); double dt = difftime(now, last_t);
      if (dt >= 2.0) { write_stats(reg, last, dt); last_t = now; }
    }
    for (int i = 0; i < MAX_SESS; i++) if (reg[i].used) free_session(&reg[i]);
    mtl_uninit(st);
    return 0;
  }

  /* ═══ LEGACY one-shot ═══ (tests manuels : 1 session vidéo depuis les args, pas de réconciliation) */
  if (!l_mcast || !l_port || !l_shm || !iface[0]) { usage(argv[0]); return 1; }
  if (!strcmp(pmd,"af_xdp"))      snprintf(portname,sizeof(portname),"native_af_xdp:%s",iface);
  else if (!strcmp(pmd,"kernel")) snprintf(portname,sizeof(portname),"kernel:%s",iface);
  else                            snprintf(portname,sizeof(portname),"%s",iface);

  struct mtl_init_params p; memset(&p, 0, sizeof(p));
  p.num_ports = 1;
  snprintf(p.port[MTL_PORT_P], MTL_PORT_MAX_LEN, "%s", portname);
  if (sip[0]) inet_pton(AF_INET, sip, p.sip_addr[MTL_PORT_P]);
  p.pmd[MTL_PORT_P] = mtl_pmd_by_port_name(portname);
  p.flags |= MTL_FLAG_DEV_AUTO_START_STOP;
  p.log_level = MTL_LOG_LEVEL_INFO;
  p.lcores = lcores[0] ? lcores : NULL;
  p.rx_queues_cnt[MTL_PORT_P] = 1;
  p.tx_queues_cnt[MTL_PORT_P] = 1;

  mtl_handle st = mtl_init(&p);
  if (!st) { fprintf(stderr, "mtl_rx: mtl_init fail\n"); return 1; }

  struct sess* s = &reg[0]; s->kind = K_VIDEO;
  snprintf(s->mcast,sizeof(s->mcast),"%s",l_mcast);
  s->udp_port=l_port; s->payload_type=l_pt; s->ring=l_ring; s->hdr=l_hdr;
  s->width=l_w; s->height=l_h; s->fps=l_fps; s->interlaced=l_inter; s->bit_depth=l_bd;
  s->ntg = 1;
  snprintf(s->tg[0].shm_path,sizeof(s->tg[0].shm_path),"%s",l_shm);
  if (l_stats) snprintf(s->tg[0].stats_path,sizeof(s->tg[0].stats_path),"%s",l_stats);
  if (l_ident) { snprintf(s->tg[0].ident_file,sizeof(s->tg[0].ident_file),"%s",l_ident); s->tg[0].has_ident=1; }
  s->st = st; snprintf(s->portname, sizeof(s->portname), "%s", portname); s->stop = 0;
  if (setup_video(s) != 0) { fprintf(stderr,"mtl_rx: setup échoué\n"); mtl_uninit(st); return 1; }
  s->used = 1;

  time_t last_t = time(NULL);
  while (!g_stop) {
    for (int z = 0; z < 20 && !g_stop; z++) usleep(100000);
    if (g_stop) break;
    time_t now = time(NULL); double dt = difftime(now, last_t); last_t = now;
    write_stats(reg, last, dt);
  }
  free_session(s);
  mtl_uninit(st);
  return 0;
}
