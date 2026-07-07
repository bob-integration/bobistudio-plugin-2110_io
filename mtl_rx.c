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

/* ── MXL (Media eXchange Layer) ── le moteur produit/consomme des FLOWS MXL (grains vidéo/data,
 * samples audio) au lieu des rings shm maison. Voir bobimxl.py côté Python (mêmes conventions). */
#include <mxl/mxl.h>
#include <mxl/flow.h>
#include <mxl/time.h>
#include <mxl/rational.h>
#define OPENSSL_SUPPRESS_DEPRECATED   /* SHA1_* sont « deprecated » en OpenSSL 3 mais valides */
#include <openssl/sha.h>

#define MAX_FB    64
/* Registre de sessions libmtl (RX + TX, toutes essences). Une session = un (mcast,port) distinct
 * (les RX même-source sont fan-outés en UNE session à N cibles, cf. MAX_TG). DOIT couvrir le pire
 * cas ACTIF : N RX vidéo distincts + audio/ANC RX + slots TX. À 16 il était PILE rempli par 16 RX
 * vidéo → les sessions TX (traitées après) étaient rejetées (« registre plein ») → sortie TX muette.
 * `reg` est statique (BSS) ⇒ ~18 Kio/sess, l'agrandir est quasi gratuit. 128 couvre 40+ entrées
 * distinctes + audio + TX avec marge. */
#define MAX_SESS  128
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

/* Horloge monotone (mesure d'écarts insensible aux sauts NTP/PTP de l'horloge système). */
static uint64_t mono_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* Timestamp média de la frame → TAI ns ABSOLU (instant de capture, horloge PTP commune A/V).
 * Le RX libmtl fournit en pratique tfmt=MEDIA_CLK : un compteur 32 bits qui WRAPPE (~47721 s à
 * 90 kHz, ~89478 s à 48 kHz). st10_media_clk_to_ns ne donne que la position DANS la fenêtre →
 * vidéo (90k) et audio (48k) ne sont alors PAS comparables. On reconstruit l'absolu en recollant
 * la fenêtre courante à partir de l'horloge PTP (mtl_ptp_read_time_raw = TAI ns), la même qui
 * cadence les media-clocks ST 2110-10. tfmt=TAI → renvoyé tel quel. */
static uint64_t media_ts_to_tai(mtl_handle st, enum st10_timestamp_fmt tfmt,
                                uint64_t ts, uint32_t rate) {
  if (tfmt == ST10_TIMESTAMP_FMT_TAI) return ts;
  uint64_t within = st10_media_clk_to_ns((uint32_t)ts, rate);     /* ns dans [0, window) */
  uint64_t window = (uint64_t)(((double)(1ULL << 32) * 1e9) / (double)rate);
  uint64_t ptp = mtl_ptp_read_time_raw(st);                       /* TAI ns courant */
  if (!window) return ptp;
  uint64_t tai = (ptp / window) * window + within;
  if      ((int64_t)(tai - ptp) >  (int64_t)(window / 2)) tai -= window;  /* fenêtre précédente */
  else if ((int64_t)(ptp - tai) >  (int64_t)(window / 2)) tai += window;  /* fenêtre suivante */
  return tai;
}

/* ═══ MXL ═══ instance globale (un domaine = un sous-rép. tmpfs partagé avec les consommateurs).
 * Domaine surchargeable par MXL_DOMAIN (isole un banc). Créée dans main(), à vie. */
static mxlInstance g_mxl = NULL;

/* ── Ports MTL (NIC) du device ── un mtl_init unique peut déclarer plusieurs NIC média
 * (MTL_PORT_MAX). Remplis à l'init, puis résolus PAR SESSION via le nom d'iface (multi-NIC :
 * chaque session vise la NIC qui porte physiquement son mcast — AF-XDP, pas d'auto-sélection). */
struct mtl_port_ent {
  char iface[64]; char portname[MTL_PORT_MAX_LEN];
  char pmd[16];   /* étiquette PMD du port ("af_xdp"|"dpdk"|"kernel") — publiée dans mtl_ports.json */
};
static struct mtl_port_ent g_ports[MTL_PORT_MAX];
static int g_nports = 0;

/* Nom de port MTL selon le PMD : native_af_xdp:<if> / kernel:<if> / <if> (PCI/dpdk). */
static void build_portname(const char* pmd, const char* iface, char out[MTL_PORT_MAX_LEN]) {
  if (!strcmp(pmd, "af_xdp"))      snprintf(out, MTL_PORT_MAX_LEN, "native_af_xdp:%s", iface);
  else if (!strcmp(pmd, "kernel")) snprintf(out, MTL_PORT_MAX_LEN, "kernel:%s", iface);
  else                             snprintf(out, MTL_PORT_MAX_LEN, "%s", iface);
}

/* Résout un nom d'iface vers le portname MTL initialisé. iface vide → port 0 (mono-NIC).
 * Renvoie l'index de port (≥0) ; -1 si l'iface est demandée mais inconnue du device → `out` reste
 * vide et l'appelant (reconcile) remonte `unknown_iface`. */
static int resolve_port(const char* iface, char out[MTL_PORT_MAX_LEN]) {
  out[0] = '\0';
  if (g_nports <= 0) return -1;
  if (!iface || !iface[0]) { snprintf(out, MTL_PORT_MAX_LEN, "%s", g_ports[0].portname); return 0; }
  for (int k = 0; k < g_nports; k++)
    if (!strcmp(g_ports[k].iface, iface)) { snprintf(out, MTL_PORT_MAX_LEN, "%s", g_ports[k].portname); return k; }
  return -1;
}

/* ── 2022-7 : état du LIEN physique par NIC ──────────────────────────────────────────────
 * Sans parade, émettre vers un lien mort est FATAL en AF_XDP : le driver ne draine plus le
 * ring XSK (`tx prod full`) → `st20_tx_queue_fatal_error` que libmtl ne sait pas récupérer
 * (« not dpdk user pmd, nothing to do ») ; pire, sa récupération interne (audio) recrée ses
 * mempools en boucle et FUIT des memzones DPDK jusqu'au plafond (2560) → TOUT le TX meurt,
 * même après rebranchement (banc 2026-07-06, nœud 30). La parade PRINCIPALE est dans libmtl
 * (patch bobi.studio patch_afxdp_tx_link_drop : port au lien mort ⇒ paquets jetés comme émis,
 * la session duale ne se fige jamais — vrai hitless). Ici on ne fait que JOURNALISER les
 * changements de lien (boucle principale) ; le backstop anti-wedge reste l'ultime filet. */
static int iface_carrier(const char* iface) {
  if (!iface || !iface[0]) return 1;
  char p[160]; snprintf(p, sizeof(p), "/sys/class/net/%s/carrier", iface);
  FILE* f = fopen(p, "r");
  if (!f) return 1;                    /* illisible (netns, nom exotique) → ne jamais geler à l'aveugle */
  int c = fgetc(f); fclose(f);
  return c == '0' ? 0 : 1;
}

/* Namespace UUIDv5 Bobi.Studio = uuid5(NAMESPACE_DNS, "mxl.bobi.studio") — DOIT être identique à
 * bobimxl._NS_BOBI (sinon les flux écrits ici ne sont pas trouvés par les consommateurs Python). */
static const uint8_t NS_BOBI[16] = {
  0xd4, 0xe7, 0x7c, 0xba, 0x0e, 0x52, 0x55, 0xd9, 0x82, 0xed, 0x72, 0x26, 0xb7, 0xbd, 0xa7, 0x57};

/* UUIDv5 (SHA-1) d'un NOM de flux → chaîne canonique (= bobimxl.flow_id). Le flowDef porte cet id ;
 * le lecteur ouvre le flux par ce même id. */
static void flow_id_str(const char* name, char out[37]) {
  SHA_CTX c; unsigned char h[20];
  SHA1_Init(&c);
  SHA1_Update(&c, NS_BOBI, 16);
  SHA1_Update(&c, name, strlen(name));
  SHA1_Final(h, &c);
  h[6] = (unsigned char)((h[6] & 0x0f) | 0x50);   /* version 5 */
  h[8] = (unsigned char)((h[8] & 0x3f) | 0x80);   /* variant RFC 4122 */
  snprintf(out, 37,
    "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
    h[0],h[1],h[2],h[3],h[4],h[5],h[6],h[7],h[8],h[9],h[10],h[11],h[12],h[13],h[14],h[15]);
}

/* Nom de flux = basename du shm_path historique (strip /dev/shm/). Le câblage orchestrateur
 * propage toujours des noms ({hn}_{idx}) → contrat inchangé. */
static const char* flow_name(const char* shm_path) {
  if (!strncmp(shm_path, "/dev/shm/", 9)) return shm_path + 9;
  return shm_path;
}

/* Cadence (double) → rational standard (grain_rate du flowDef + grille TAL). Miroir de to_st_fps :
 * les fps « drop » sont rendus en n/1001 pour une grille TAI exacte. */
static mxlRational fps_to_rational(double f) {
  mxlRational r = {25, 1};
  if      (fabs(f - 23.98) < 0.05) { r.numerator = 24000; r.denominator = 1001; }
  else if (fabs(f - 24.0)  < 0.05) { r.numerator = 24;    r.denominator = 1; }
  else if (fabs(f - 25.0)  < 0.05) { r.numerator = 25;    r.denominator = 1; }
  else if (fabs(f - 29.97) < 0.05) { r.numerator = 30000; r.denominator = 1001; }
  else if (fabs(f - 30.0)  < 0.05) { r.numerator = 30;    r.denominator = 1; }
  else if (fabs(f - 50.0)  < 0.05) { r.numerator = 50;    r.denominator = 1; }
  else if (fabs(f - 59.94) < 0.1)  { r.numerator = 60000; r.denominator = 1001; }
  else if (fabs(f - 60.0)  < 0.05) { r.numerator = 60;    r.denominator = 1; }
  else if (fabs(f - 100.0) < 0.05) { r.numerator = 100;   r.denominator = 1; }
  else if (fabs(f - 120.0) < 0.05) { r.numerator = 120;   r.denominator = 1; }
  return r;
}

/* Une cible = un FLUX MXL de sortie. Une session en porte 1..N (fan-out même-source → N flux).
 * Chaque cible a son propre writer (RX/simu) ou reader (TX), son index et son propre IDENT. */
struct target {
  int      idx;            /* slot d'origine (pour le log) */
  char     shm_path[300], stats_path[300];   /* shm_path = NOM de flux (basename) après strip */
  mxlFlowWriter writer;    /* RX/simu : producteur du flux MXL (NULL si TX) */
  mxlFlowReader reader;    /* TX : lecteur du flux d'entrée câblé (NULL si RX ; créé paresseusement) */
  uint64_t index;          /* dernier index publié/lu (frame_index/chunk_index exposé en stats) */
  uint64_t recv;           /* compteur reçu (pour le débit) */
  uint64_t late;           /* TX vidéo : trames en retard (get_frame > 1,5 période = epoch raté) */
  uint64_t last_feed_ns;   /* TX vidéo : instant (monotone) du dernier get_frame réussi */
  uint64_t alive_ns;       /* TX (tous kinds) : dernier signe de vie du thread (get_frame OK ou
                            * attente de câblage). Figé session démarrée = queue TX morte (wedge). */
  int      dbg_depth_logged; /* TX vidéo : log one-shot grainSize/out_size/_src8 au 1er grain */
  uint64_t tx_src_idx;     /* TX vidéo : dernier index de grain SOURCE lu (détection flux figé) */
  uint64_t tx_src_idx_ns;  /* TX vidéo : instant (monotone) où tx_src_idx a changé pour la dernière fois */
  int      tx_src_idx_init; /* TX vidéo : tx_src_idx amorcé ? (0 au (ré)ouverture du reader) */
  uint64_t field_base;     /* TX entrelacé : index du 1er champ de la trame émise (MÊME trame pour les
                            * 2 champs → anti-peigne). Parité = TOP(pair) en TFF, BOTTOM(impair) en BFF. */
  /* RX vidéo : latence de réception (segment A = capture média → écriture shm), moyenne glissante
   * sur la fenêtre de stats. lat_sum en ns, lat_cnt = nb d'échantillons ; reset à chaque write_stats. */
  uint64_t lat_sum; uint32_t lat_cnt;
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
  char portname[MTL_PORT_MAX_LEN];   /* leg primaire (NIC résolue depuis `iface`) */
  char iface[64];           /* nom de NIC du leg primaire ('' = port 0 / mono-NIC) */
  /* ── 2022-7 (leg redondant red/blue) — conçu mais NON émis par le socle (num_leg=1) ── */
  char portname_r[MTL_PORT_MAX_LEN]; /* leg redondant (vide = mono-leg) */
  char iface_r[64];         /* nom de NIC du leg redondant */
  char mcast_r[64]; int udp_port_r;  /* mcast/port de la 2ᵉ patte */
  int  num_leg;             /* 1 (socle) ou 2 (2022-7) */
  /* réseau */
  char mcast[64];
  int  udp_port, payload_type;
  uint32_t ssrc;            /* TX seulement : RFC3550 SSRC annoncé en a=ssrc du SDP (0=aléatoire) */
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
  uint8_t* tx_scratch;     /* TX entrelacé + IDENT : trame pleine de travail (overlay avant weave) */
  uint8_t* rx_scratch;     /* RX entrelacé : trame pleine où l'on weave les 2 champs avant OpenGrain */
  uint8_t* tx_frame;       /* TX entrelacé : trame pleine source (copie stable du grain) entre champs */
  /* ── MXL ── grille du flux (grain_rate vidéo/data, sample_rate audio) pour l'index TAI. */
  mxlRational mrate;       /* vidéo/data : cadence trame (grain_rate) */
  mxlRational srate;       /* audio : {48000,1} (sample_rate) */
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

/* ── flowDef MXL (ressource Flow NMOS IS-04) — mêmes champs/conventions que bobimxl.build_*. ── */
static void jcomp(struct json_object* arr, const char* nm, int w, int h, int bd) {
  struct json_object* c = json_object_new_object();
  json_object_object_add(c, "name", json_object_new_string(nm));
  json_object_object_add(c, "width", json_object_new_int(w));
  json_object_object_add(c, "height", json_object_new_int(h));
  json_object_object_add(c, "bit_depth", json_object_new_int(bd));
  json_object_array_add(arr, c);
}
static struct json_object* jgrouphint(const char* name, const char* role) {
  struct json_object* tags = json_object_new_object();
  struct json_object* gh = json_object_new_array();
  char hint[420]; snprintf(hint, sizeof(hint), "%s:%s", name, role);
  json_object_array_add(gh, json_object_new_string(hint));
  json_object_object_add(tags, "urn:x-nmos:tag:grouphint/v1.0", gh);
  return tags;
}
static struct json_object* jrate(mxlRational r) {
  struct json_object* o = json_object_new_object();
  json_object_object_add(o, "numerator", json_object_new_int64(r.numerator));
  json_object_object_add(o, "denominator", json_object_new_int64(r.denominator));
  return o;
}

/* Vidéo planar (video/x-mxl-planar) : grain = somme des plans Y+Cb+Cr (octet-identique au shm
 * maison historique). 422 → Cb/Cr en demi-largeur, pleine hauteur. Renvoie une chaîne heap. */
static char* build_video_flowdef(struct sess* s, struct target* t) {
  const char* name = flow_name(t->shm_path);
  char id[37]; flow_id_str(name, id);
  struct json_object* o = json_object_new_object();
  json_object_object_add(o, "id", json_object_new_string(id));
  json_object_object_add(o, "tags", jgrouphint(name, "Video"));
  json_object_object_add(o, "format", json_object_new_string("urn:x-nmos:format:video"));
  json_object_object_add(o, "label", json_object_new_string(name));
  json_object_object_add(o, "media_type", json_object_new_string("video/x-mxl-planar"));
  json_object_object_add(o, "grain_rate", jrate(s->mrate));
  json_object_object_add(o, "frame_width", json_object_new_int(s->width));
  json_object_object_add(o, "frame_height", json_object_new_int(s->height));
  /* ENTRELACÉ NATIF « 1 grain = 1 CHAMP » (modèle SDK MXL) : interlace_mode=interlaced_tff/bff +
   * grain_rate = cadence TRAME (25/1 ou 30000/1001). libmxl double la cadence en interne (cadence
   * champ) et dimensionne chaque grain = 1 CHAMP (½ trame). L'identité du champ = parité de l'index
   * de grain (index pair = 1er champ ; tff ⇒ 1er=top). On écrit/lit donc des grains-champs ;
   * field_height reste pleine (libmxl fait le /2). */
  json_object_object_add(o, "interlace_mode", json_object_new_string(
      s->interlaced ? (s->tff ? "interlaced_tff" : "interlaced_bff") : "progressive"));
  json_object_object_add(o, "colorspace", json_object_new_string("BT709"));
  struct json_object* comps = json_object_new_array();
  jcomp(comps, "Y",  s->width,     s->height, s->bit_depth);
  jcomp(comps, "Cb", s->width / 2, s->height, s->bit_depth);
  jcomp(comps, "Cr", s->width / 2, s->height, s->bit_depth);
  json_object_object_add(o, "components", comps);
  char* out = strdup(json_object_to_json_string(o));
  json_object_put(o);
  return out;
}

/* Audio (audio/float32) : flux CONTINU de samples float32 par canal, 48 kHz. */
static char* build_audio_flowdef(struct sess* s, struct target* t) {
  const char* name = flow_name(t->shm_path);
  char id[37]; flow_id_str(name, id);
  struct json_object* o = json_object_new_object();
  json_object_object_add(o, "id", json_object_new_string(id));
  json_object_object_add(o, "tags", jgrouphint(name, "Audio"));
  json_object_object_add(o, "format", json_object_new_string("urn:x-nmos:format:audio"));
  json_object_object_add(o, "label", json_object_new_string(name));
  json_object_object_add(o, "media_type", json_object_new_string("audio/float32"));
  mxlRational sr = {48000, 1};
  json_object_object_add(o, "sample_rate", jrate(sr));
  json_object_object_add(o, "channel_count", json_object_new_int(s->channels));
  json_object_object_add(o, "bit_depth", json_object_new_int(32));
  char* out = strdup(json_object_to_json_string(o));
  json_object_put(o);
  return out;
}

/* Data/ANC (video/smpte291) : grain = payload ANC sérialisée (meta+udw). */
static char* build_data_flowdef(struct sess* s, struct target* t) {
  const char* name = flow_name(t->shm_path);
  char id[37]; flow_id_str(name, id);
  struct json_object* o = json_object_new_object();
  json_object_object_add(o, "id", json_object_new_string(id));
  json_object_object_add(o, "tags", jgrouphint(name, "Data"));
  json_object_object_add(o, "format", json_object_new_string("urn:x-nmos:format:data"));
  json_object_object_add(o, "label", json_object_new_string(name));
  json_object_object_add(o, "media_type", json_object_new_string("video/smpte291"));
  json_object_object_add(o, "grain_rate", jrate(s->mrate));
  char* out = strdup(json_object_to_json_string(o));
  json_object_put(o);
  return out;
}

/* Crée le writer MXL d'une cible à partir d'un flowDef (RX/simu). Renvoie 0 si OK.
 * GC-AND-RETRY : l'id du flux ne dépend que du NOM du slot (pas de l'interlace_mode/grain_rate).
 * Quand un slot passe d'un producteur SIMU progressif (contrôleur) à un RX entrelacé (ce process),
 * un flux PÉRIMÉ de def DIFFÉRENTE squatte l'id → mxlCreateFlowWriter échoue (def incompatible).
 * Une fois l'ancien producteur libéré (le contrôleur ferme sa simu quand le slot devient live), un
 * garbage-collect récupère le flux orphelin → on réessaie (jusqu'à ~2 s). Cas sans transition de def
 * (progressif↔progressif : attache au flux simu existant) : succès au 1er essai, boucle non exécutée. */
static int open_writer(struct target* t, char* flowdef) {
  bool created = false;
  mxlStatus st = mxlCreateFlowWriter(g_mxl, flowdef, NULL, &t->writer, NULL, &created);
  for (int attempt = 0; st != MXL_STATUS_OK && attempt < 10; attempt++) {
    mxlGarbageCollectFlows(g_mxl);          /* récupère le flux périmé une fois son producteur libéré */
    usleep(200000);                         /* laisse la simu du contrôleur se fermer (slot devenu live) */
    st = mxlCreateFlowWriter(g_mxl, flowdef, NULL, &t->writer, NULL, &created);
  }
  free(flowdef);
  if (st != MXL_STATUS_OK) {
    fprintf(stderr, "mtl_rx: mxlCreateFlowWriter(%s) -> %d\n", flow_name(t->shm_path), (int)st);
    return -1;
  }
  return 0;
}

/* Latence de réception (segment A) : Δ entre l'instant de CAPTURE média (media_ts, TAI) et
 * l'instant d'écriture shm (now_ns, REALTIME). Les deux horloges peuvent différer d'un nombre
 * ENTIER de secondes (offset TAI↔UTC, p.ex. 37 s ; ou 0 si le système est calé sur TAI). La vraie
 * latence étant << 1 s, on retire l'écart entier de secondes par arrondi → reste la part sub-seconde.
 * Accumule la moyenne glissante (fenêtre = write_stats). media_ts=0 (inconnu) → ignoré. */
static void accum_rx_latency(struct target* t, uint64_t media_ts) {
  if (!media_ts) return;
  int64_t d   = (int64_t)now_ns() - (int64_t)media_ts;
  int64_t off = (int64_t)llround((double)d / 1e9) * 1000000000LL;
  d -= off;
  if (d < 0) d = 0;                 /* garde-fou : jamais négatif après calibration */
  t->lat_sum += (uint64_t)d; t->lat_cnt++;
}
/* sampling de l'horloge média ST 2110-10 : 90 kHz vidéo, 48 kHz audio (pour st10_get_tai). */
#define MEDIA_CLK_VIDEO 90000u
#define MEDIA_CLK_AUDIO 48000u

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

/* dst_bd = profondeur du BUFFER destination `dst` (≠ s->bit_depth quand TX : le buffer libmtl
 * est toujours 10-bit, cf. input_fmt PLANAR10LE). Le patch reste un plan Y 8-bit. */
static void overlay_ident(struct sess* s, struct target* t, uint8_t* dst, int dst_bd) {
  if (!t->ident_patch || t->id_bw <= 0) return;
  int W = s->width, H = s->height, bw = t->id_bw, bh = t->id_bh;
  if (bw > W || bh > H) return;
  int x0 = W - bw - 8, y0 = 8;
  x0 -= x0 & 1; y0 -= y0 & 1; if (x0 < 0) x0 = 0;
  int deep = (dst_bd >= 10), shift = dst_bd - 8;
  size_t ysz = (size_t)W * H, uvsz = (size_t)(W / 2) * H;
  int bps = deep ? 2 : 1;
  for (int r = 0; r < bh; r++) {
    const uint8_t* p = t->ident_patch + (size_t)r * bw;
    if (deep) { uint16_t* y = (uint16_t*)dst + (size_t)(y0 + r) * W + x0;
                for (int x = 0; x < bw; x++) y[x] = (uint16_t)p[x] << shift; }
    else      { uint8_t*  y = dst + (size_t)(y0 + r) * W + x0;
                for (int x = 0; x < bw; x++) y[x] = p[x]; }
  }
  int neutral = 1 << (dst_bd - 1);
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
  uint64_t frame_idx_latch = 0;   /* entrelacé : index TRAME latché sur le 1er champ (→ index champ = ×2+sf) */
  while (!s->stop) {
    struct st_frame* frame = st20p_rx_get_frame(s->vh);
    if (!frame) { usleep(1000); continue; }
    if (!frame->addr[0]) { st20p_rx_put_frame(s->vh, frame); continue; }
    /* Timestamp MÉDIA (capture) de la frame, en TAI ns — commun audio/vidéo via PTP (ST 2110-10).
     * st10_get_tai normalise TAI ou media-clk → ns (sampling 90 kHz vidéo). */
    uint64_t mts = media_ts_to_tai(s->st, frame->tfmt, frame->timestamp, MEDIA_CLK_VIDEO);
    /* af_xdp/copy_mode : décodage unique → fan-out vers chaque cible (slot shm).
     * Chaque cible a son propre ring/index + son propre IDENT. */
    /* CHAMP-NATIF : chaque get_frame = 1 CHAMP (entrelacé) ou 1 TRAME (progressif) → 1 grain.
     * Index : entrelacé = index TRAME × 2 + parité de champ (libmtl pose second_field). On LATCH
     * l'index trame sur le 1er champ — le 2e champ arrive ½ période trame plus tard, mxlTimestampToIndex
     * l'arrondirait mal. Index pair = 1er champ (= top en tff). Plus de weave : on écrit le champ tel
     * quel (≡ chemin progressif, juste à l'index champ). */
    int sf = frame->second_field ? 1 : 0;
    uint64_t fi;
    if (s->interlaced) {
      if (!sf) frame_idx_latch = mts ? mxlTimestampToIndex(&s->mrate, mts) : mxlGetCurrentIndex(&s->mrate);
      fi = frame_idx_latch * 2 + (uint64_t)sf;
    } else {
      fi = mts ? mxlTimestampToIndex(&s->mrate, mts) : mxlGetCurrentIndex(&s->mrate);
    }
    for (int ti = 0; ti < s->ntg; ti++) {
      struct target* t = &s->tg[ti];
      mxlGrainInfo gi; uint8_t* payload;
      if (mxlFlowWriterOpenGrain(t->writer, fi, &gi, &payload) != MXL_STATUS_OK) continue;
      size_t _ncp = s->slotsize;                   /* grain = 1 champ/trame = slotsize */
      if (gi.grainSize && (size_t)gi.grainSize < _ncp) _ncp = gi.grainSize;   /* garde-fou débordement */
      if (s->conv8) {                              /* 10→8 bits (planar10 libmtl → planar8 grain) */
        const uint16_t* src = (const uint16_t*)frame->addr[0];
        for (size_t k = 0; k < _ncp; k++) payload[k] = (uint8_t)(src[k] >> 2);
      } else {
        memcpy(payload, frame->addr[0], _ncp);
      }
      /* IDENT : positionné en espace TRAME → désactivé en entrelacé (champ = ½ hauteur). TODO champ-aware. */
      if (t->has_ident && !s->interlaced) { load_ident_patch(t); overlay_ident(s, t, payload, s->bit_depth); }
      gi.validSlices = gi.totalSlices;
      mxlFlowWriterCommitGrain(t->writer, &gi);
      accum_rx_latency(t, mts);
      t->index = fi; t->recv++;
    }
    st20p_rx_put_frame(s->vh, frame);
  }
  return NULL;
}

/* ═══ AUDIO ═══════════════════════════════════════════════════════════════════ */
/* st30p délivre le payload L24 du fil = BIG-ENDIAN, entrelacé par échantillon (chs canaux × 3 o).
 * On le CONVERTIT en samples float32 par canal (contrat MXL audio/float32) et on l'écrit dans le
 * flux continu MXL, à l'INDEX SAMPLE TAI (mxlTimestampToIndex sur la grille 48 kHz) → MÊME grille
 * PTP que la vidéo (phase-lock A/V structurel). */
static void* audio_rx_thread(void* arg) {
  struct sess* s = arg;
  int chs = s->channels;
  while (!s->stop) {
    struct st30_frame* frame = st30p_rx_get_frame(s->ah);
    if (!frame) { usleep(500); continue; }
    const uint8_t* src = (const uint8_t*)frame->addr;
    size_t n = frame->data_size / (size_t)(chs * 3);   /* samples par canal dans ce chunk (~48) */
    if (!n) { st30p_rx_put_frame(s->ah, frame); continue; }
    uint64_t mts = media_ts_to_tai(s->st, frame->tfmt, frame->timestamp, MEDIA_CLK_AUDIO);
    for (int ti = 0; ti < s->ntg; ti++) {        /* fan-out (même source audio → N flux) */
      struct target* t = &s->tg[ti];
      uint64_t idx = mts ? mxlTimestampToIndex(&s->srate, mts)
                         : mxlGetCurrentIndex(&s->srate) - n;
      mxlMutableWrappedMultiBufferSlice slc; memset(&slc, 0, sizeof(slc));
      if (mxlFlowWriterOpenSamples(t->writer, idx, n, &slc) != MXL_STATUS_OK) continue;
      size_t stride = slc.stride;
      for (size_t c = 0; c < slc.count; c++) {
        size_t pos = 0;
        for (int f = 0; f < 2; f++) {            /* 2 fragments (wrap d'anneau) */
          size_t fb = slc.base.fragments[f].size;
          if (!fb) continue;
          float* dst = (float*)((uint8_t*)slc.base.fragments[f].pointer + c * stride);
          size_t cnt = fb / 4;
          for (size_t k = 0; k < cnt; k++) {
            const uint8_t* p = src + ((pos + k) * (size_t)chs + c) * 3;   /* L24 BE entrelacé */
            int32_t v = (int32_t)((uint32_t)p[0] << 16 | (uint32_t)p[1] << 8 | p[2]);
            if (v & 0x800000) v |= ~0xFFFFFF;    /* sign-extend 24→32 bits */
            dst[k] = (float)v / 8388608.0f;
          }
          pos += cnt;
        }
      }
      mxlFlowWriterCommitSamples(t->writer);
      t->index = idx; t->recv++;
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

/* Crée le writer MXL de chaque cible RX/simu (le flowDef dépend du kind ; mrate/srate déjà posés). */
static int open_targets(struct sess* s) {
  for (int ti = 0; ti < s->ntg; ti++) {
    char* fd = (s->kind == K_AUDIO) ? build_audio_flowdef(s, &s->tg[ti])
             : (s->kind == K_DATA)  ? build_data_flowdef(s, &s->tg[ti])
             :                        build_video_flowdef(s, &s->tg[ti]);
    if (open_writer(&s->tg[ti], fd) != 0) return -1;
  }
  return 0;
}

/* Nettoyage d'un setup RX vidéo PARTIEL (échec après st20p_rx_create) : on NE DOIT JAMAIS orpheliner
 * la session libmtl. Une session st20p créée puis abandonnée continue de recevoir les paquets, remplit
 * son pool de framebuffers (jamais drainé car le thread RX n'a pas démarré) → « framebuff pool empty,
 * back-pressure » et 0 trame remontée. On libère writers MXL + handle st20p. */
static void _abort_video_setup(struct sess* s) {
  for (int ti = 0; ti < s->ntg; ti++)
    if (s->tg[ti].writer) { mxlReleaseFlowWriter(g_mxl, s->tg[ti].writer); s->tg[ti].writer = NULL; }
  if (s->vh) { st20p_rx_free(s->vh); s->vh = NULL; }
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
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.ip_addr[MTL_SESSION_PORT_P]);
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.ip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
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
  /* Champ-natif : 1 grain = 1 CHAMP (= slotsize, déjà ½ trame en entrelacé côté libmtl) ou 1 trame
   * en progressif. Plus de ×2 (plus de weave). shm_slotsize == slotsize. */
  s->shm_slotsize = s->slotsize;
  s->mrate = fps_to_rational(s->fps);            /* grain_rate du flowDef + grille TAI vidéo */
  if (open_targets(s) != 0) { _abort_video_setup(s); return -1; }
  fprintf(stderr, "mtl_rx[video] %dx%d%s fps=%.2f pt=%d mc=%s:%d slot=%zu ring=%d → %d cible(s):",
          s->width, s->height, s->interlaced ? "i" : "p", s->fps, s->payload_type,
          s->mcast, s->udp_port, s->slotsize, s->ring, s->ntg);
  for (int ti = 0; ti < s->ntg; ti++) fprintf(stderr, " %s", s->tg[ti].shm_path);
  fprintf(stderr, "\n");
  if (pthread_create(&s->thread, NULL, video_rx_thread, s) != 0) { _abort_video_setup(s); return -1; }
  s->started = 1;
  return 0;
}

/* ═══ TX commun (lecture du flux d'entrée câblé via un reader MXL) ═════════════ */
/* Crée (paresseusement) le reader MXL du flux d'entrée câblé. -1 si le flux n'existe pas encore
 * (le producteur peut démarrer après nous) → l'appelant réessaie. */
static int open_reader(struct target* t) {
  char id[37]; flow_id_str(flow_name(t->shm_path), id);
  if (mxlCreateFlowReader(g_mxl, id, NULL, &t->reader) != MXL_STATUS_OK) { t->reader = NULL; return -1; }
  t->tx_src_idx_init = 0;   /* nouveau reader → ré-amorce la détection de flux figé */
  return 0;
}

/* Reconnexion sur flux SOURCE figé : un producteur redéployé (ex. multiview) DÉTRUIT puis RECRÉE son
 * flux MXL SOUS LE MÊME NOM → l'ancien ring orphelin reste lisible et reader_latest/reader_field
 * renvoient indéfiniment le DERNIER grain (index figé, AUCUNE erreur → aucune libération) → la sortie
 * TX se fige sur la dernière trame jusqu'à un re-câble manuel. Si l'index source n'avance plus au-delà
 * de TX_REOPEN_STALE_NS, on libère le reader → open_reader le recrée sur la génération COURANTE du flux
 * (résolution par nom). Sans danger pour une source légitimement statique (elle relit son grain).
 * `idx` = index du grain courant ; `tnow` = mono_ns() ; à appeler APRÈS une lecture réussie. */
#define TX_REOPEN_STALE_NS 1000000000ULL   /* 1 s sans avancée d'index ⇒ flux périmé → reconnexion */
static void tx_reopen_if_stale(struct target* t, uint64_t idx, uint64_t tnow) {
  if (!t->tx_src_idx_init || idx != t->tx_src_idx) {
    t->tx_src_idx = idx; t->tx_src_idx_ns = tnow; t->tx_src_idx_init = 1;
    return;
  }
  if (tnow - t->tx_src_idx_ns > TX_REOPEN_STALE_NS) {
    /* Détacher le reader de l'ancien ring PUIS réclamer l'orphelin : le flux périmé a son writer
     * MORT (producteur redéployé) mais reste « vivant » dans le domaine tant qu'un reader y est
     * attaché et qu'aucun GC ne passe → une simple réouverture par nom retomberait DESSUS (c'est
     * pourquoi le re-câble immédiat ne suffisait qu'après un délai laissant le GC opérer). On force
     * donc le GC ici → open_reader (tour suivant) résout le NOUVEAU flux vivant du producteur. */
    mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL;
    mxlGarbageCollectFlows(g_mxl);
  }
}

/* Dernier grain dispo (NON bloquant) → 0 + (gi,payload), ou -1. Flux recréé (producteur redéployé)
 * → libère le reader pour forcer une recréation au tour suivant. */
static int reader_latest(struct target* t, mxlGrainInfo* gi, uint8_t** payload) {
  mxlFlowRuntimeInfo rt;
  mxlStatus st = mxlFlowReaderGetRuntimeInfo(t->reader, &rt);
  if (st == MXL_STATUS_OK && rt.headIndex != MXL_UNDEFINED_INDEX) {
    st = mxlFlowReaderGetGrainNonBlocking(t->reader, rt.headIndex, gi, payload);
    if (st == MXL_STATUS_OK) return 0;
  }
  if (st == MXL_ERR_FLOW_NOT_FOUND || st == MXL_ERR_FLOW_INVALID) {
    mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL;
  }
  return -1;
}

/* Champ-natif TX — émet les 2 champs de la MÊME trame COMPLÈTE, dominance pilotée par `tff`.
 * Mesuré : la RX livre des grains-champs CONSÉCUTIFS réguliers (20 ms, 50/s, aucun saut). Donc on
 * émet simplement la dernière trame COMPLÈTE = (trame de tête − 1) → ses 2 champs sont garantis
 * écrits (anti-peigne), latence constante d'1 trame, AUCUN rattrapage/compteur → la cadence suit la
 * source sans dérive ni répétition (le ×2-catch-up de 0.28.2 saccadait). Grains : index TRAME×2 +
 * parité (pair = TOP, impair = BOTTOM). 1er champ (field==0) = TOP(pair) en TFF, BOTTOM(impair) en BFF.
 *  - field==0 : ancre field_base sur la trame de tête−1, parité selon tff.
 *  - field==1 : l'autre parité de la MÊME trame. */
static int reader_field(struct target* t, int field, int tff, mxlGrainInfo* gi, uint8_t** payload) {
  mxlFlowRuntimeInfo rt;
  mxlStatus st = mxlFlowReaderGetRuntimeInfo(t->reader, &rt);
  if (st == MXL_STATUS_OK && rt.headIndex != MXL_UNDEFINED_INDEX) {
    uint64_t head = rt.headIndex;
    uint64_t latest_frame = ((head & 1ULL) ? head - 1 : head) / 2;
    uint64_t target;
    if (field == 0) {
      uint64_t frame = latest_frame ? latest_frame - 1 : 0;   /* dernière trame COMPLÈTE */
      t->field_base = frame * 2 + (tff ? 0 : 1);              /* TOP(pair) en TFF, BOTTOM(impair) en BFF */
      target = t->field_base;
    } else {
      target = tff ? (t->field_base + 1) : (t->field_base - 1);   /* 2e champ, MÊME trame */
    }
    st = mxlFlowReaderGetGrainNonBlocking(t->reader, target, gi, payload);
    if (st == MXL_STATUS_OK) return 0;
  }
  if (st == MXL_ERR_FLOW_NOT_FOUND || st == MXL_ERR_FLOW_INVALID) {
    mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL;
  }
  return -1;
}

/* ═══ VIDÉO TX (flux MXL → wire, st20p_tx) ═════════════════════════════════════ */

/* Feeder TX : lit le DERNIER grain du flux MXL d'entrée câblé et l'émet. Up-shift 8→10 si le
 * pipeline est en 8 bits (transport 2110-20 = 422-10). Le reader est créé PARESSEUSEMENT (le
 * producteur peut démarrer après nous). Pacing assuré par ST20P_TX_FLAG_BLOCK_GET. */
static void* video_tx_thread(void* arg) {
  struct sess* s = arg;
  struct target* t = &s->tg[0];                 /* la cible TX = l'unique flux d'entrée câblé */
  size_t out_size = st20p_tx_frame_size(s->vth);   /* = taille CHAMP si entrelacé */
  uint64_t latched_fi = 0;                      /* entrelacé : index latché sur le 1er champ */
  /* Période nominale entre deux get_frame (BLOCK_GET pace à la cadence de la session ; en
   * entrelacé un get_frame = un CHAMP). Sert au compteur `late` : un get_frame qui revient
   * > 1,5 période après le précédent = la session a raté au moins un epoch (trame perdue). */
  uint64_t period_ns = s->fps > 0 ? (uint64_t)(1e9 / s->fps) : 0;
  if (s->interlaced) period_ns /= 2;
  while (!s->stop) {
    if (!t->reader) {
      t->last_feed_ns = 0;   /* flux pas encore là : ne pas compter l'attente comme du retard */
      t->alive_ns = mono_ns();   /* attendre un câblage n'est pas un wedge */
      if (open_reader(t) != 0) { usleep(20000); continue; }
    }
    struct st_frame* frame = st20p_tx_get_frame(s->vth);   /* bloque → pacing à fps */
    if (!frame) { usleep(1000); continue; }
    uint64_t tnow = mono_ns();
    t->alive_ns = tnow;      /* la session transmet (frames libérées par MTL) */
    if (period_ns && t->last_feed_ns) {
      uint64_t gap = tnow - t->last_feed_ns;
      if (gap > period_ns + period_ns / 2) {
        uint64_t missed = (gap + period_ns / 2) / period_ns;   /* périodes écoulées (arrondi) */
        t->late += missed > 1 ? missed - 1 : 1;
      }
    }
    t->last_feed_ns = tnow;
    if (s->interlaced) {
      /* CHAMP-NATIF : 1 get_frame = 1 CHAMP à émettre (libmtl pose second_field). On lit le grain-champ
       * de même parité (reader_field) et on le copie directement dans le buffer libmtl (= 1 champ),
       * up-shift 8→10 si pipeline 8 bits. Plus de tx_frame/weave. */
      int sf = frame->second_field ? 1 : 0;
      uint8_t* dst = (uint8_t*)frame->addr[0];
      mxlGrainInfo gi; uint8_t* payload;
      if (reader_field(t, sf, s->tff, &gi, &payload) == 0) {
        /* MXL-NATIF : profondeur SOURCE dérivée du GRAIN, PAS de s->bit_depth (cf. branche
         * progressive). Détection robuste : grain = 1 CHAMP en entrelacé → taille 8-bit attendue
         * = 2·w·(h/2) ; 8-bit si grainSize == exp8 OU == moitié d'out_size (= champ 10-bit). */
        size_t _gs = (size_t)gi.grainSize;
        size_t _exp8 = (size_t)2 * (size_t)s->width * ((size_t)s->height / 2);   /* CHAMP */
        int _src8 = (_gs > 0 && (_gs == _exp8 || _gs * 2 == out_size));
        if (!t->dbg_depth_logged) {
          fprintf(stderr, "mtl_rx[video TX dbg i] shm=%s grainSize=%zu out_size=%zu exp8(2w·h/2)=%zu "
                  "gs2==out=%d gs==exp8=%d _src8=%d\n",
                  t->shm_path, _gs, out_size, _exp8,
                  (int)(_gs * 2 == out_size), (int)(_gs == _exp8), _src8);
          t->dbg_depth_logged = 1;
        }
        if (_src8) {
          uint16_t* d16 = (uint16_t*)dst;
          for (size_t k = 0; k < _gs; k++) d16[k] = (uint16_t)payload[k] << 2;   /* 8→10 */
        } else {
          memcpy(dst, payload, out_size < _gs ? out_size : _gs);
        }
        latched_fi = gi.index;
        tx_reopen_if_stale(t, gi.index, tnow);   /* flux figé (producteur redéployé) → reconnexion */
        /* IDENT entrelacé : positionné en espace TRAME → différé (champ = ½ hauteur). TODO champ-aware. */
      } else {
        memset(dst, 0, out_size);                  /* pas de grain → champ neutre */
      }
      st20p_tx_put_frame(s->vth, frame);
      t->index = latched_fi; t->recv++;
    } else {
      mxlGrainInfo gi; uint8_t* payload;
      uint8_t* dst = (uint8_t*)frame->addr[0];
      if (reader_latest(t, &gi, &payload) == 0) {
        /* MXL-NATIF : profondeur SOURCE dérivée du GRAIN (flux auto-descriptif), PAS de s->bit_depth
         * (valeur poussée — au câblage à chaud la session TX défaute à bit_depth=10, donc une source
         * 8-bit comme la multiview était versée brute dans le buffer 10-bit → sortie 2110 VERTE).
         * Détection ROBUSTE immune au padding : 8-bit si grainSize == taille planar 8-bit attendue
         * (géométrie, 422 : Y+Cb+Cr = 2·w·h) OU == moitié d'out_size (buffer libmtl 10-bit). Une
         * vraie source 10-bit (grain == out_size == 2·exp8) ne déclenche aucune des deux → memcpy. */
        size_t _gs = (size_t)gi.grainSize;
        size_t _exp8 = (size_t)2 * (size_t)s->width * (size_t)s->height;
        int _src8 = (_gs > 0 && (_gs == _exp8 || _gs * 2 == out_size));
        if (!t->dbg_depth_logged) {
          fprintf(stderr, "mtl_rx[video TX dbg] shm=%s grainSize=%zu out_size=%zu exp8(2wh)=%zu "
                  "gs2==out=%d gs==exp8=%d _src8=%d\n",
                  t->shm_path, _gs, out_size, _exp8,
                  (int)(_gs * 2 == out_size), (int)(_gs == _exp8), _src8);
          t->dbg_depth_logged = 1;
        }
        if (_src8) {
          uint16_t* d16 = (uint16_t*)dst;
          for (size_t k = 0; k < _gs; k++) d16[k] = (uint16_t)payload[k] << 2;   /* 8→10 */
        } else {
          memcpy(dst, payload, out_size < _gs ? out_size : _gs);
        }
        /* IDENT : dst est une trame pleine planaire TOUJOURS 10-bit (input_fmt PLANAR10LE). */
        if (t->has_ident) { load_ident_patch(t); overlay_ident(s, t, dst, 10); }
        t->index = gi.index;
        tx_reopen_if_stale(t, gi.index, tnow);   /* flux figé (producteur redéployé) → reconnexion */
      } else {
        memset(dst, 0, out_size);               /* pas de grain → trame neutre */
      }
      st20p_tx_put_frame(s->vth, frame);
      t->recv++;
    }
  }
  return NULL;
}

static int setup_video_tx(struct sess* s) {
  if (s->ring < 2) s->ring = 2;                 /* (vestige) — MXL gère le ring du flux d'entrée */
  /* Champ-natif : taille d'un GRAIN = 1 CHAMP en entrelacé (½ hauteur), 1 trame en progressif.
   * 422 planar : 8b = 2·w·h_grain, 10b = 4·w·h_grain. = grainSize du flux d'entrée. */
  size_t h_grain = s->interlaced ? (size_t)s->height / 2 : (size_t)s->height;
  s->slotsize = (size_t)(s->bit_depth == 8 ? 2 : 4) * (size_t)s->width * h_grain;
  s->shm_slotsize = s->slotsize;

  struct st20p_tx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_vtx";
  ops.priv = s;
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.dip_addr[MTL_SESSION_PORT_P]);   /* destination */
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.dip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.port.payload_type = s->payload_type;
  ops.port.ssrc = s->ssrc;   /* fixe (≠0) pour matcher le a=ssrc annoncé dans le SDP TX */
  /* CADENCE ENTRELACÉ (fix racine du peigne) : libmtl TX entrelacé avec fps=cadence TRAME (P25)
   * pace get_frame à la période TRAME (40 ms) → 1 champ/40 ms = 25 champs/s = MOITIÉ de 1080i50 →
   * le désentrelaceur récepteur peigne. On passe la cadence CHAMP (×2 → P50) : libmtl pace alors à
   * 20 ms/champ = 50 champs/s, ET signale toujours exactframerate=25 dans le SDP (il /2 en entrelacé).
   * Le RX n'était pas affecté (cadencé par le réseau, pas par ops.fps). */
  ops.width = s->width; ops.height = s->height;
  ops.fps = to_st_fps(s->interlaced ? s->fps * 2.0 : s->fps);
  ops.interlaced = s->interlaced ? true : false;
  ops.input_fmt = ST_FRAME_FMT_YUV422PLANAR10LE;   /* on fournit du 10-bit (up-shift 8→10 nous-mêmes) */
  ops.transport_fmt = ST20_FMT_YUV_422_10BIT;
  ops.device = ST_PLUGIN_DEVICE_AUTO;
  ops.framebuff_cnt = 3;
  ops.flags = ST20P_TX_FLAG_BLOCK_GET;             /* get_frame bloque → pacing à fps */

  s->vth = st20p_tx_create(s->st, &ops);
  if (!s->vth) { fprintf(stderr, "mtl_rx: st20p_tx_create fail (video %s:%d)\n", s->mcast, s->udp_port); return -1; }
  fprintf(stderr, "mtl_rx[video TX] %dx%d%s fps=%.2f pt=%d ssrc=%u → %s:%d (in shm=%s bd%d ring%d)\n",
          s->width, s->height, s->interlaced ? "i" : "p", s->fps, s->payload_type, s->ssrc,
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
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.ip_addr[MTL_SESSION_PORT_P]);
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.ip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.port.payload_type = s->payload_type;
  ops.fmt = ST30_FMT_PCM24;
  ops.channel = (uint16_t)s->channels;
  ops.sampling = ST30_SAMPLING_48K;
  ops.ptime = to_st30_ptime(s->a_ptime);   /* AUTO depuis le SDP (a=ptime) / défaut réglage */
  ops.framebuff_size = (uint32_t)s->slotsize;   /* 1 chunk = 1 ms */
  ops.framebuff_cnt = 4;

  s->ah = st30p_rx_create(s->st, &ops);
  if (!s->ah) { fprintf(stderr, "mtl_rx: st30p_rx_create fail (audio %s:%d)\n", s->mcast, s->udp_port); return -1; }
  s->srate.numerator = 48000; s->srate.denominator = 1;   /* grille sample TAI */
  if (open_targets(s) != 0) return -1;
  fprintf(stderr, "mtl_rx[audio] %dch L24/48k pt=%d mc=%s:%d slot=%zu ring=%d → %d cible(s):",
          s->channels, s->payload_type, s->mcast, s->udp_port, s->slotsize, s->ring, s->ntg);
  for (int ti = 0; ti < s->ntg; ti++) fprintf(stderr, " %s", s->tg[ti].shm_path);
  fprintf(stderr, "\n");
  return pthread_create(&s->thread, NULL, audio_rx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

/* ═══ AUDIO TX (flux MXL → wire, st30p_tx) ═════════════════════════════════════ */
/* Derniers `n` samples dispo (NON bloquant) du flux audio → slc, ou -1. Recrée le reader si le
 * flux a été invalidé (producteur redéployé). */
static int reader_samples(struct target* t, size_t n, mxlWrappedMultiBufferSlice* slc) {
  mxlFlowRuntimeInfo rt;
  mxlStatus st = mxlFlowReaderGetRuntimeInfo(t->reader, &rt);
  if (st == MXL_STATUS_OK && rt.headIndex != MXL_UNDEFINED_INDEX) {
    st = mxlFlowReaderGetSamplesNonBlocking(t->reader, rt.headIndex, n, slc);
    if (st == MXL_STATUS_OK) return 0;
  }
  if (st == MXL_ERR_FLOW_NOT_FOUND || st == MXL_ERR_FLOW_INVALID) {
    mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL;
  }
  return -1;
}

/* Feeder audio TX : lit les derniers samples float32 du flux MXL d'entrée câblé, les CONVERTIT en
 * L24 BG (wire-native ST 2110-30) et les émet. reader créé paresseusement, pacing par BLOCK_GET. */
static void* audio_tx_thread(void* arg) {
  struct sess* s = arg;
  struct target* t = &s->tg[0];
  int chs = s->channels;
  while (!s->stop) {
    if (!t->reader) {
      t->alive_ns = mono_ns();   /* attendre un câblage n'est pas un wedge */
      if (open_reader(t) != 0) { usleep(20000); continue; }
    }
    struct st30_frame* frame = st30p_tx_get_frame(s->a_tx);   /* bloque (BLOCK_GET) → pacing 1ms */
    if (!frame) { usleep(500); continue; }
    t->alive_ns = mono_ns();     /* la session transmet (frames libérées par MTL) */
    uint8_t* dst = (uint8_t*)frame->addr;
    size_t n = frame->data_size / (size_t)(chs * 3);          /* samples par canal à émettre */
    mxlWrappedMultiBufferSlice slc; memset(&slc, 0, sizeof(slc));
    if (n && reader_samples(t, n, &slc) == 0) {
      size_t stride = slc.stride;
      for (size_t c = 0; c < slc.count; c++) {
        size_t pos = 0;
        for (int f = 0; f < 2; f++) {
          size_t fb = slc.base.fragments[f].size;
          if (!fb) continue;
          const float* sp = (const float*)((const uint8_t*)slc.base.fragments[f].pointer + c * stride);
          size_t cnt = fb / 4;
          for (size_t k = 0; k < cnt; k++) {
            float v = sp[k];
            if (v > 1.0f) v = 1.0f; else if (v < -1.0f) v = -1.0f;
            int32_t iv = (int32_t)lrintf(v * 8388607.0f);      /* float → 24 bits signés */
            uint8_t* p = dst + ((pos + k) * (size_t)chs + c) * 3;
            p[0] = (uint8_t)((iv >> 16) & 0xff);               /* big-endian (wire) */
            p[1] = (uint8_t)((iv >> 8) & 0xff);
            p[2] = (uint8_t)(iv & 0xff);
          }
          pos += cnt;
        }
      }
    } else {
      memset(dst, 0, frame->data_size);                        /* pas de samples → silence */
    }
    st30p_tx_put_frame(s->a_tx, frame);
    t->recv++;
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
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.dip_addr[MTL_SESSION_PORT_P]);   /* destination */
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.dip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.port.payload_type = s->payload_type;
  ops.port.ssrc = s->ssrc;   /* fixe (≠0) pour matcher le a=ssrc annoncé dans le SDP TX */
  ops.fmt = ST30_FMT_PCM24;
  ops.channel = (uint16_t)s->channels;
  ops.sampling = ST30_SAMPLING_48K;
  ops.ptime = to_st30_ptime(s->a_ptime);   /* AUTO depuis le SDP (a=ptime) / défaut réglage */
  ops.framebuff_size = (uint32_t)s->slotsize;
  ops.framebuff_cnt = 4;
  ops.flags = ST30P_TX_FLAG_BLOCK_GET;

  s->a_tx = st30p_tx_create(s->st, &ops);
  if (!s->a_tx) { fprintf(stderr, "mtl_rx: st30p_tx_create fail (audio %s:%d)\n", s->mcast, s->udp_port); return -1; }
  fprintf(stderr, "mtl_rx[audio TX] %dch L24/48k pt=%d ssrc=%u → %s:%d (in shm=%s)\n",
          s->channels, s->payload_type, s->ssrc, s->mcast, s->udp_port, s->tg[0].shm_path);
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
    uint64_t idx = mxlGetCurrentIndex(&s->mrate);   /* grille trame TAI (ANC ~1 grain/trame) */
    for (int ti = 0; ti < s->ntg; ti++) {
      struct target* t = &s->tg[ti];
      mxlGrainInfo gi; uint8_t* dst;
      if (mxlFlowWriterOpenGrain(t->writer, idx, &gi, &dst) != MXL_STATUS_OK) continue;
      if (need <= gi.grainSize) {
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
      } else {   /* frame anormalement gros → grain vide (on ne déborde jamais) */
        ((uint32_t*)dst)[0] = 0; ((uint32_t*)dst)[1] = 0;
      }
      if (got_tc) { memcpy(t->tc, tc, sizeof(t->tc)); t->tc_df = df; t->tc_valid = 1; }
      gi.validSlices = gi.totalSlices;
      mxlFlowWriterCommitGrain(t->writer, &gi);
      t->index = idx; t->recv++;
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
    if (!t->reader) {
      t->alive_ns = mono_ns();   /* attendre un câblage n'est pas un wedge */
      if (open_reader(t) != 0) { usleep(20000); continue; }
    }
    struct st40_frame_info* frame = st40p_tx_get_frame(s->d_tx);   /* bloque (BLOCK_GET) → pacing fps */
    if (!frame) { usleep(1000); continue; }
    t->alive_ns = mono_ns();     /* la session transmet (frames libérées par MTL) */
    mxlGrainInfo gi; uint8_t* src;
    if (reader_latest(t, &gi, &src) == 0) {
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
      t->index = gi.index;
    } else {
      frame->meta_num = 0; frame->udw_buffer_fill = 0;   /* pas de grain → ANC vide */
    }
    st40p_tx_put_frame(s->d_tx, frame);
    t->recv++;
  }
  return NULL;
}

static int setup_data(struct sess* s) {
  if (s->ring > 8) s->ring = 8; if (s->ring < 2) s->ring = 2;
  s->slotsize = ANC_SLOT; s->max_udw = ANC_MAX_UDW; s->copy_mode = 1;

  struct st40p_rx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_d";
  ops.priv = s;
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.ip_addr[MTL_SESSION_PORT_P]);
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.ip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.port.payload_type = s->payload_type;
  ops.framebuff_cnt = 4;
  ops.max_udw_buff_size = s->max_udw;
  ops.rtp_ring_size = 1024;   /* requis (>0, puissance de 2 : ring DPDK des paquets RTP ANC) */
  ops.flags = ST40P_RX_FLAG_BLOCK_GET;

  s->d_rx = st40p_rx_create(s->st, &ops);
  if (!s->d_rx) { fprintf(stderr, "mtl_rx: st40p_rx_create fail (data %s:%d)\n", s->mcast, s->udp_port); return -1; }
  s->mrate = fps_to_rational(s->fps);            /* grille TAI ANC (cadence trame) */
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
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.dip_addr[MTL_SESSION_PORT_P]);   /* destination */
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.dip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.port.payload_type = s->payload_type;
  ops.port.ssrc = s->ssrc;   /* fixe (≠0) pour matcher le a=ssrc annoncé dans le SDP TX */
  ops.fps = to_st_fps(s->fps);
  ops.interlaced = false;
  ops.framebuff_cnt = 4;
  ops.max_udw_buff_size = s->max_udw;
  ops.flags = ST40P_TX_FLAG_BLOCK_GET;

  s->d_tx = st40p_tx_create(s->st, &ops);
  if (!s->d_tx) { fprintf(stderr, "mtl_rx: st40p_tx_create fail (data %s:%d)\n", s->mcast, s->udp_port); return -1; }
  fprintf(stderr, "mtl_rx[data TX] ANC fps=%.2f pt=%d ssrc=%u → %s:%d (in shm=%s)\n",
          s->fps, s->payload_type, s->ssrc, s->mcast, s->udp_port, s->tg[0].shm_path);
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
  s->ssrc=(uint32_t)jint(j,"ssrc",0);   /* TX : 0=aléatoire (défaut libmtl), sinon fixe (borné 31 bits côté générateur) */
  s->ring=jint(j,"ring", s->kind==K_AUDIO?100:8); s->hdr=jint(j,"hdr",64);
  /* multi-NIC : `iface` (leg primaire) → portname résolu (vide = iface inconnu, détecté au create).
   * `iface` absent → port 0 (mono-NIC). 2022-7 : `iface2`/`mcast2`/`udp_port2` = 2ᵉ leg (red/blue). */
  snprintf(s->iface, sizeof(s->iface), "%s", jstr(j,"iface",""));
  resolve_port(s->iface, s->portname);
  const char* ifn2 = jstr(j,"iface2","");
  const char* mc2  = jstr(j,"mcast2","");
  int up2 = jint(j,"udp_port2",0);
  if (ifn2[0] && mc2[0] && up2) {
    snprintf(s->iface_r, sizeof(s->iface_r), "%s", ifn2);
    snprintf(s->mcast_r, sizeof(s->mcast_r), "%s", mc2);
    s->udp_port_r = up2;
    resolve_port(ifn2, s->portname_r);
    s->num_leg = 2;
  } else {
    s->num_leg = 1;
  }
  /* 2022-7 : PAS de gel/recréation au lien mort — la session reste DUALE en permanence.
   * C'est libmtl (patch bobi.studio patch_afxdp_tx_link_drop) qui jette les paquets d'un
   * port au lien mort comme s'ils étaient émis → le leg vivant ne s'arrête JAMAIS (vrai
   * hitless), reprise silencieuse au link-up. Recréer la session ici couperait ~1 s le
   * leg sain (les ports d'une session MTL sont figés à la création). */
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
  int n = snprintf(s->sig, sizeof(s->sig),
                   "%d|%d|%s|%d|%d|%u|%dx%d|%.2f|i%d|f%d|bd%d|r%d|ch%d|ap%.3f|if%s|if2%s|mc2%s|p2%d|",
                   s->role, s->kind, s->mcast, s->udp_port, s->payload_type, s->ssrc,
                   s->width, s->height, s->fps, s->interlaced, s->tff, s->bit_depth, s->ring,
                   s->channels, s->a_ptime, s->iface, s->iface_r, s->mcast_r, s->udp_port_r);
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
    if (t->writer) mxlReleaseFlowWriter(g_mxl, t->writer);   /* RX/simu */
    if (t->reader) mxlReleaseFlowReader(g_mxl, t->reader);   /* TX */
    if (t->ident_patch) free(t->ident_patch);
  }
  if (s->tx_scratch) free(s->tx_scratch);
  if (s->rx_scratch) free(s->rx_scratch);
  if (s->tx_frame)   free(s->tx_frame);
  memset(s, 0, sizeof(*s));
}

/* Écrit un statut d'ERREUR dans le stats json d'une cible (création de session ratée). Le contrôleur
 * le relaie en `rx_error` sur :8080 → l'orchestrateur affiche « abonné mais ne reçoit pas » avec la
 * cause précise (typiquement budget lcores du nœud dépassé : st20p_rx_create → no available lcore). */
static void write_stats_error(struct target* t, const char* err) {
  if (!t->stats_path[0]) return;
  FILE* sf = fopen(t->stats_path, "w");
  if (sf) { fprintf(sf, "{\"fps\": 0.0, \"frame_index\": 0, \"error\": \"%s\"}\n", err); fclose(sf); }
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

  /* Pass 1 — MATCH : marquer « seen » les sessions désirées DÉJÀ présentes (sig identique).
   * On ne crée RIEN ici (une session inchangée — ex. la vidéo — n'est pas touchée → pas de faute PTP). */
  for (int k = 0; k < n; k++) {
    struct sess want;
    if (parse_session_into(json_object_array_get_idx(arr, k), &want) != 0) continue;
    compute_sig(&want);
    for (int i = 0; i < MAX_SESS; i++)
      if (reg[i].used && !reg[i].seen && !strcmp(reg[i].sig, want.sig)) { reg[i].seen = 1; break; }
  }

  /* Pass 2 — FREE-BEFORE-CREATE : libérer les sessions périmées AVANT toute création. Libère leur
   * file/flow AF_XDP (mt_rx_xdp_put) ; sinon recréer un flux pour une source CHANGÉE échoue
   * (« create flow fail -5 ») tant que l'ancienne session tient ses ressources → on se retrouvait
   * sans aucune session (cas changement de source audio). */
  for (int i = 0; i < MAX_SESS; i++)
    if (reg[i].used && !reg[i].seen) {
      fprintf(stderr,"mtl_rx: retrait session %s:%d\n", reg[i].mcast, reg[i].udp_port);
      free_session(&reg[i]);
    }

  /* Pass 3 — CREATE : monter les sessions désirées encore absentes (file/flow maintenant libres). */
  for (int k = 0; k < n; k++) {
    struct sess want;
    if (parse_session_into(json_object_array_get_idx(arr, k), &want) != 0) continue;
    compute_sig(&want);
    int found = -1;
    for (int i = 0; i < MAX_SESS; i++)
      if (reg[i].used && !strcmp(reg[i].sig, want.sig)) { found = i; break; }
    if (found >= 0) continue;                                /* déjà présente (gardée en Pass 1) */
    int slot = -1;
    for (int i = 0; i < MAX_SESS; i++) if (!reg[i].used) { slot = i; break; }
    if (slot < 0) { fprintf(stderr,"mtl_rx: registre plein, session ignorée\n"); continue; }
    struct sess* s = &reg[slot];
    *s = want;
    s->st = st; s->stop = 0;
    /* `iface` absent → portname déjà résolu sur le port 0 par parse. Repli mono-NIC ultime
     * (g_ports vide) : le portname global du device. */
    if (!s->portname[0] && !s->iface[0]) snprintf(s->portname, sizeof(s->portname), "%s", portname);
    /* iface DEMANDÉE mais inconnue du device → pas de session, erreur explicite sur le slot. */
    if (!s->portname[0]) {
      fprintf(stderr,"mtl_rx: session %s:%d sur iface inconnue '%s' — ignorée\n",
              s->mcast, s->udp_port, s->iface);
      if (s->role != ROLE_TX)
        for (int ti = 0; ti < s->ntg; ti++) write_stats_error(&s->tg[ti], "unknown_iface");
      memset(s,0,sizeof(*s));
      continue;
    }
    /* 2022-7 : leg redondant sur iface inconnue → dégrade en mono-leg (le primaire reste valide). */
    if (s->num_leg == 2 && !s->portname_r[0]) {
      fprintf(stderr,"mtl_rx: leg redondant %s:%d sur iface inconnue '%s' — mono-leg\n",
              s->mcast_r, s->udp_port_r, s->iface_r);
      s->num_leg = 1;
    }
    int r = (s->role == ROLE_TX)
            ? ((s->kind == K_AUDIO) ? setup_audio_tx(s) : (s->kind == K_DATA) ? setup_data_tx(s) : setup_video_tx(s))
            : ((s->kind == K_AUDIO) ? setup_audio(s)    : (s->kind == K_DATA) ? setup_data(s)    : setup_video(s));
    if (r == 0) { s->used = 1; s->seen = 1; }
    else {
      fprintf(stderr,"mtl_rx: création session %s:%d échouée\n", s->mcast, s->udp_port);
      /* RX : remonter l'échec dans le stats json du slot (sinon le contrôleur le croit « mtl »). */
      if (s->role != ROLE_TX)
        for (int ti = 0; ti < s->ntg; ti++) write_stats_error(&s->tg[ti], "rx_create_failed");
      memset(s,0,sizeof(*s));
    }
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
      /* Latence de réception (segment A) : moyenne de la fenêtre, en ms. -1 = pas d'échantillon
       * (TX, ou flux média sans timestamp) → sérialisé `null`. Reset du cumul à chaque fenêtre. */
      double rx_lat = t->lat_cnt ? (double)t->lat_sum / (double)t->lat_cnt / 1e6 : -1.0;
      t->lat_sum = 0; t->lat_cnt = 0;
      FILE* sf = fopen(t->stats_path, "w");
      if (sf) {
        char latbuf[32];
        if (rx_lat >= 0.0) snprintf(latbuf, sizeof(latbuf), "%.1f", rx_lat);
        else               snprintf(latbuf, sizeof(latbuf), "null");
        if (s->kind == K_DATA && t->tc_valid)
          fprintf(sf, "{\"fps\": %.1f, \"frame_index\": %llu, \"timecode\": \"%s\", \"df\": %s, \"rx_latency_ms\": %s}\n",
                  rate, (unsigned long long)t->index, t->tc, t->tc_df ? "true" : "false", latbuf);
        else
          fprintf(sf, "{\"fps\": %.1f, \"frame_index\": %llu, \"late\": %llu, \"rx_latency_ms\": %s}\n",
                  rate, (unsigned long long)t->index, (unsigned long long)t->late, latbuf);
        fclose(sf);
      }
    }
  }
}

/* Stats I/O PAR PORT (NIC) — contrat /tmp/mtl_ports.json (cf. DPDK_NARROW.md « Contrats de la
 * nuit ») : remplace `ethtool -S` côté contrôleur quand un port est en PMD DPDK (l'iface kernel
 * a disparu en vfio). Source = mtl_get_port_stats() (mtl_api.h) : compteurs CUMULÉS depuis
 * mtl_init (struct mtl_port_status). Écrit pour TOUS les ports (af_xdp compris — le contrôleur
 * n'y bascule ses débits que pour pmd=dpdk). Écriture atomique (tmp + rename) : le contrôleur
 * peut lire à tout instant sans lire un JSON tronqué. Cadence = celle de write_stats (~2 s). */
static void write_port_stats(mtl_handle st) {
  if (g_nports <= 0) return;
  const char* tmp = "/tmp/mtl_ports.json.tmp";
  FILE* f = fopen(tmp, "w");
  if (!f) return;
  fprintf(f, "{\"ts\": %llu, \"ports\": [",
          (unsigned long long)(now_ns() / 1000000000ULL));
  for (int k = 0; k < g_nports; k++) {
    struct mtl_port_status ps; memset(&ps, 0, sizeof(ps));
    if (mtl_get_port_stats(st, (enum mtl_port)k, &ps) != 0)
      memset(&ps, 0, sizeof(ps));          /* échec → champs à 0 (contrat : absents = 0) */
    fprintf(f,
      "%s{\"port\": \"%s\", \"pmd\": \"%s\", "
      "\"rx_packets\": %llu, \"tx_packets\": %llu, "
      "\"rx_bytes\": %llu, \"tx_bytes\": %llu, "
      "\"rx_err\": %llu, \"tx_err\": %llu, "
      "\"rx_hw_dropped\": %llu, \"rx_nombuf\": %llu}",
      k ? ", " : "",
      g_ports[k].iface[0] ? g_ports[k].iface : g_ports[k].portname,
      g_ports[k].pmd[0] ? g_ports[k].pmd : "af_xdp",
      (unsigned long long)ps.rx_packets,            (unsigned long long)ps.tx_packets,
      (unsigned long long)ps.rx_bytes,              (unsigned long long)ps.tx_bytes,
      (unsigned long long)ps.rx_err_packets,        (unsigned long long)ps.tx_err_packets,
      (unsigned long long)ps.rx_hw_dropped_packets, (unsigned long long)ps.rx_nombuf_packets);
  }
  fprintf(f, "]}\n");
  fclose(f);
  rename(tmp, "/tmp/mtl_ports.json");
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

  /* Instance MXL (domaine tmpfs partagé avec les consommateurs). À vie, comme mtl_init. */
  const char* mxl_domain = getenv("MXL_DOMAIN");
  if (!mxl_domain || !*mxl_domain) mxl_domain = "/dev/shm/mxl";
  mkdir(mxl_domain, 0777);   /* idempotent ; /dev/shm existe déjà */
  g_mxl = mxlCreateInstance(mxl_domain, NULL);
  if (!g_mxl) { fprintf(stderr, "mtl_rx: mxlCreateInstance(%s) fail (tmpfs ?)\n", mxl_domain); return 1; }
  fprintf(stderr, "mtl_rx: domaine MXL = %s\n", mxl_domain);

  if (config) {
    /* ═══ DAEMON ═══ mtl_init UNE fois (à vie), puis réconciliation des sessions à chaud.
     * Le device reste démarré (XDP attaché) tant que le process vit → ptp4l ne faute qu'au boot. */
    struct json_object* root = json_object_from_file(config);
    if (!root) { fprintf(stderr, "mtl_rx: config illisible: %s\n", config); return 1; }
    snprintf(pmd,sizeof(pmd),"%s",jstr(root,"pmd","af_xdp"));
    snprintf(iface,sizeof(iface),"%s",jstr(root,"iface",""));
    snprintf(sip,sizeof(sip),"%s",jstr(root,"sip",""));
    snprintf(lcores,sizeof(lcores),"%s",jstr(root,"lcores",""));
    char pacing[16]; snprintf(pacing,sizeof(pacing),"%s",jstr(root,"pacing","auto"));
    int rx_q = jint(root,"rx_queues", 8);   /* plafond de sessions RX (= rx_count du moteur) */
    int tx_q = jint(root,"tx_queues", 1);   /* TX : Phase 2 (1 = file de contrôle IGMP/ARP) */
    int quota_mbs = jint(root,"quota_mbs", 5000);   /* Mb/s max par scheduler (lcore), cf. mtl_init */
    struct mtl_init_params p; memset(&p, 0, sizeof(p));
    /* ── Ports (NIC) du device ── multi-NIC : tableau "ports":[{iface,sip,rx_queues,tx_queues}].
     * Repli rétro-compat : "ports" absent → un seul port depuis les scalaires iface/sip/rx_queues/
     * tx_queues (config legacy / mono-NIC). g_ports sert ensuite à résoudre session→port par iface. */
    struct json_object* jports;
    if (json_object_object_get_ex(root,"ports",&jports) && json_object_is_type(jports,json_type_array)) {
      int np = json_object_array_length(jports);
      for (int k = 0; k < np && g_nports < MTL_PORT_MAX; k++) {
        struct json_object* pj = json_object_array_get_idx(jports, k);
        const char* pif = jstr(pj,"iface","");
        if (!pif[0]) continue;
        int idx = g_nports++;
        snprintf(g_ports[idx].iface, sizeof(g_ports[idx].iface), "%s", pif);
        /* PMD PAR PORT (chantier DPDK) : un nœud peut être MIXTE (af_xdp + dpdk). "pmd"/"bdf"
         * absents de l'entrée → pmd global (rétro-compat, comportement STRICTEMENT inchangé).
         * pmd=="dpdk" : le portname EST le BDF PCI tel quel ("0000:xx:00.y", aucun préfixe →
         * mtl_pmd_by_port_name rend MTL_PMD_DPDK_USER, cf. lib/src/mt_util.c). `iface` reste le
         * nom kernel historique : clé de résolution session→port (resolve_port) côté config. */
        const char* ppmd = jstr(pj, "pmd", "");
        const char* pbdf = jstr(pj, "bdf", "");
        if (!ppmd[0]) ppmd = pmd;                    /* repli : pmd global existant */
        if (!strcmp(ppmd, "dpdk") && pbdf[0])
          snprintf(g_ports[idx].portname, MTL_PORT_MAX_LEN, "%s", pbdf);
        else
          build_portname(ppmd, pif, g_ports[idx].portname);
        snprintf(g_ports[idx].pmd, sizeof(g_ports[idx].pmd), "%s", ppmd);
        snprintf(p.port[idx], MTL_PORT_MAX_LEN, "%s", g_ports[idx].portname);
        const char* psip = jstr(pj,"sip","");
        if (psip[0]) inet_pton(AF_INET, psip, p.sip_addr[idx]);
        p.pmd[idx] = mtl_pmd_by_port_name(g_ports[idx].portname);
        int prxq = jint(pj,"rx_queues", 8), ptxq = jint(pj,"tx_queues", 1);
        p.rx_queues_cnt[idx] = prxq > 0 ? prxq : 1;
        p.tx_queues_cnt[idx] = ptxq > 0 ? ptxq : 1;
      }
    } else if (iface[0]) {   /* repli scalaire (mono-NIC) */
      int idx = g_nports++;
      snprintf(g_ports[idx].iface, sizeof(g_ports[idx].iface), "%s", iface);
      build_portname(pmd, iface, g_ports[idx].portname);
      snprintf(g_ports[idx].pmd, sizeof(g_ports[idx].pmd), "%s", pmd);
      snprintf(p.port[idx], MTL_PORT_MAX_LEN, "%s", g_ports[idx].portname);
      if (sip[0]) inet_pton(AF_INET, sip, p.sip_addr[idx]);
      p.pmd[idx] = mtl_pmd_by_port_name(g_ports[idx].portname);
      p.rx_queues_cnt[idx] = rx_q > 0 ? rx_q : 1;
      p.tx_queues_cnt[idx] = tx_q > 0 ? tx_q : 1;
    }
    json_object_put(root);
    if (g_nports < 1) { fprintf(stderr,"mtl_rx: config sans port/iface\n"); return 1; }
    p.num_ports = g_nports;
    snprintf(portname, sizeof(portname), "%s", g_ports[0].portname);   /* défaut mono-NIC (reconcile) */
    /* AUTO_START_STOP : en AF_XDP le flow/XSK se crée AVEC le démarrage du device, déclenché par le
     * 1ᵉʳ st20p_rx_create (impossible de pré-démarrer un device vide : « add flow fail »). Le device
     * se start/stop au gré du nombre de sessions, mais le XDP reste attaché tant que mtl_init vit
     * (le détache n'a lieu qu'à mtl_uninit) → ptp4l ne faute qu'au boot, pas sur les changements. */
    p.flags |= MTL_FLAG_DEV_AUTO_START_STOP;
    /* Répartition des sessions sur les lcores : sans quota, libmtl empile TOUTES les sessions
     * vidéo sur sch_0 → saturation (epoch drop / build timeout → « RTP alignment failure » au
     * récepteur). quota_mbs (Mb/s) borne chaque scheduler (~2×1080p50 à 5000) → éclatement initial
     * sur les lcores fournis ; les flags MIGRATE rééquilibrent à chaud un sch détecté trop busy. */
    p.flags |= MTL_FLAG_TX_VIDEO_MIGRATE | MTL_FLAG_RX_VIDEO_MIGRATE;
    /* PMD DPDK/ice sur PF média : filet optionnel pour le multicast. En AF-XDP le noyau programme
     * le filtre MAC mcast du groupe rejoint ; en DPDK user c'est libmtl (mt_mcast:
     * rte_eth_dev_mac_addr_add) qui l'ajoute. Si un déploiement observe rx_packets=0 AU NIVEAU PORT
     * malgré steering+join OK, activer MTL_FLAG_NIC_RX_PROMISCUOUS (env NIC_PROMISCUOUS=1) laisse
     * passer tout le trafic sur le port média dédié. Défaut OFF : sur ice le mac_addr_add suffit
     * dès que la SOURCE IGMP (sip du port) est correcte (cf. dl360-1 2026-07-07 : rx=0 était dû au
     * sip erroné, pas au filtre MAC). Sans objet en AF-XDP (jamais de port dpdk). */
    if (getenv("NIC_PROMISCUOUS") && atoi(getenv("NIC_PROMISCUOUS")))
      for (int k = 0; k < g_nports; k++)
        if (!strcmp(g_ports[k].pmd, "dpdk")) { p.flags |= MTL_FLAG_NIC_RX_PROMISCUOUS; break; }
    p.data_quota_mbs_per_sch = quota_mbs > 0 ? (uint32_t)quota_mbs : 0;
    /* Pacing TX ST 2110-21 (mtl_init_params.pacing, niveau DEVICE — enum st21_tx_pacing_way,
     * cf. mtl_api.h) : "rl" = rate-limit MATÉRIEL (prérequis profil narrow, PMD DPDK/ice
     * uniquement) ; "tsc" = logiciel (chemin actuel AF-XDP) ; "auto" (défaut) = libmtl choisit
     * (memset a déjà posé ST21_TX_PACING_WAY_AUTO=0 → clé absente = comportement inchangé). */
    if      (!strcmp(pacing, "rl"))         p.pacing = ST21_TX_PACING_WAY_RL;
    else if (!strcmp(pacing, "tsc_narrow")) p.pacing = ST21_TX_PACING_WAY_TSC_NARROW;
    else if (!strcmp(pacing, "tsc"))        p.pacing = ST21_TX_PACING_WAY_TSC;
    else                                    p.pacing = ST21_TX_PACING_WAY_AUTO;
    p.log_level = MTL_LOG_LEVEL_INFO;
    p.lcores = lcores[0] ? lcores : NULL;

    mtl_handle st = mtl_init(&p);
    if (!st) { fprintf(stderr, "mtl_rx: mtl_init fail\n"); return 1; }
    fprintf(stderr, "mtl_rx: daemon up (%d port(s), rx_q[0]=%u tx_q[0]=%u) — réconciliation à chaud\n",
            g_nports, p.rx_queues_cnt[0], p.tx_queues_cnt[0]);

    /* Détection de changement en NANOSECONDES (st_mtim complet) : en rafale de commutations le
     * contrôleur réécrit le config plusieurs fois DANS LA MÊME SECONDE — comparer st_mtime seul
     * (secondes) ratait la dernière écriture → daemon figé sur un état intermédiaire jusqu'au
     * changement suivant (vu au banc de churn Horace : slot jamais monté pendant 12 s). */
    struct timespec cfg_mt = {0, 0}; struct stat cst;
    reconcile(reg, config, st, portname);                 /* état initial */
    if (stat(config, &cst) == 0) cfg_mt = cst.st_mtim;
    time_t last_t = time(NULL);
    int carrier[MTL_PORT_MAX];
    for (int k = 0; k < g_nports; k++) carrier[k] = iface_carrier(g_ports[k].iface);
    while (!g_stop) {
      for (int z = 0; z < 5 && !g_stop; z++) usleep(100000);   /* ~0.5s, réactif au SIGTERM */
      if (g_stop) break;
      struct stat cs;
      if (stat(config, &cs) == 0 &&
          (cs.st_mtim.tv_sec != cfg_mt.tv_sec || cs.st_mtim.tv_nsec != cfg_mt.tv_nsec)) {
        cfg_mt = cs.st_mtim;
        reconcile(reg, config, st, portname);             /* config changé → converge à chaud */
      }
      /* Lien : journalisation seule. Les sessions ne sont PAS touchées — un port au lien
       * mort jette ses paquets côté libmtl (patch_afxdp_tx_link_drop), le leg vivant
       * continue sans interruption et le leg mort réémet seul au retour du lien. */
      for (int k = 0; k < g_nports; k++) {
        int c = iface_carrier(g_ports[k].iface);
        if (c != carrier[k]) {
          fprintf(stderr, "mtl_rx: lien %s → %s%s\n", g_ports[k].iface, c ? "UP" : "DOWN",
                  c ? "" : " (TX de ce port jeté par libmtl jusqu'au retour du lien)");
          carrier[k] = c;
        }
      }
      time_t now = time(NULL); double dt = difftime(now, last_t);
      if (dt >= 2.0) { write_stats(reg, last, dt); last_t = now;
        write_port_stats(st);            /* contrat /tmp/mtl_ports.json (stats I/O par NIC) */
        mxlGarbageCollectFlows(g_mxl);   /* récupère les flux orphelins (producteurs morts) */
        /* Backstop wedge TX (ultime filet — le lien mort est normalement absorbé par le patch
         * libmtl link_drop) : session démarrée dont le thread n'a plus AUCUN signe de vie depuis
         * > 5 s (get_frame ne rend plus rien) = queue TX XDP morte — irrécupérable in-process
         * (st20_tx_queue_fatal_error « nothing to do ») et la « récupération » interne de libmtl
         * fuit des memzones DPDK jusqu'au plafond → sortie IMMÉDIATE : le contrôleur relance le
         * daemon (purge XDP/ntuple + backoff, chemin crash déjà prévu), le process neuf repart
         * avec des memzones vierges. Pas de cleanup ici : libérer des queues mortes peut
         * bloquer, et _launch_mtl est conçu pour l'après-crash. */
        uint64_t wnow = mono_ns();
        for (int i = 0; i < MAX_SESS; i++) {
          struct sess* s2 = &reg[i];
          if (!s2->used || s2->role != ROLE_TX || !s2->started || !s2->tg[0].alive_ns) continue;
          if (wnow - s2->tg[0].alive_ns > 5ull * 1000000000ull) {
            fprintf(stderr, "mtl_rx: TX FIGÉ %s:%d (aucune frame depuis %.1fs) — restart du daemon\n",
                    s2->mcast, s2->udp_port, (wnow - s2->tg[0].alive_ns) / 1e9);
            fflush(stderr);
            _exit(3);
          }
        }
      }
    }
    for (int i = 0; i < MAX_SESS; i++) if (reg[i].used) free_session(&reg[i]);
    mtl_uninit(st);
    mxlDestroyInstance(g_mxl);
    return 0;
  }

  /* ═══ LEGACY one-shot ═══ (tests manuels : 1 session vidéo depuis les args, pas de réconciliation) */
  if (!l_mcast || !l_port || !l_shm || !iface[0]) { usage(argv[0]); return 1; }
  build_portname(pmd, iface, portname);

  struct mtl_init_params p; memset(&p, 0, sizeof(p));
  p.num_ports = 1;
  snprintf(p.port[MTL_PORT_P], MTL_PORT_MAX_LEN, "%s", portname);
  if (sip[0]) inet_pton(AF_INET, sip, p.sip_addr[MTL_PORT_P]);
  p.pmd[MTL_PORT_P] = mtl_pmd_by_port_name(portname);
  p.flags |= MTL_FLAG_DEV_AUTO_START_STOP;
  p.flags |= MTL_FLAG_TX_VIDEO_MIGRATE | MTL_FLAG_RX_VIDEO_MIGRATE;
  p.data_quota_mbs_per_sch = 5000;
  p.log_level = MTL_LOG_LEVEL_INFO;
  p.lcores = lcores[0] ? lcores : NULL;
  p.rx_queues_cnt[MTL_PORT_P] = 1;
  p.tx_queues_cnt[MTL_PORT_P] = 1;

  mtl_handle st = mtl_init(&p);
  if (!st) { fprintf(stderr, "mtl_rx: mtl_init fail\n"); return 1; }

  struct sess* s = &reg[0]; s->kind = K_VIDEO; s->num_leg = 1;
  snprintf(s->mcast,sizeof(s->mcast),"%s",l_mcast);
  s->udp_port=l_port; s->payload_type=l_pt; s->ring=l_ring; s->hdr=l_hdr;
  s->width=l_w; s->height=l_h; s->fps=l_fps; s->interlaced=l_inter; s->bit_depth=l_bd;
  s->ntg = 1;
  snprintf(s->tg[0].shm_path,sizeof(s->tg[0].shm_path),"%s",l_shm);
  if (l_stats) snprintf(s->tg[0].stats_path,sizeof(s->tg[0].stats_path),"%s",l_stats);
  if (l_ident) { snprintf(s->tg[0].ident_file,sizeof(s->tg[0].ident_file),"%s",l_ident); s->tg[0].has_ident=1; }
  s->st = st; snprintf(s->portname, sizeof(s->portname), "%s", portname); s->stop = 0;
  if (setup_video(s) != 0) { fprintf(stderr,"mtl_rx: setup échoué\n"); mtl_uninit(st); mxlDestroyInstance(g_mxl); return 1; }
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
  mxlDestroyInstance(g_mxl);
  return 0;
}
