#!/usr/bin/env python3
"""
sargassum_collector.py
======================
Collecte automatique de données sargasses depuis 6 sources → SQLite.

Sources
-------
  1. FORESEA CNRS          – page WordPress + API REST WP
  2. NOAA SIR              – page HTML (PDF hebdomadaire) + ERDDAP AFAI_7D
  3. Sargassum Monitoring  – API REST WordPress
  4. Copernicus Marine     – courants totaux surface Caraïbes (CMEMS)
  5. AVISO+ DUACS          – courants géostrophiques SSH (CMEMS / AVISO+)
  6. OpenDrift             – simulation de dérive 5 jours depuis positions AFAI

Usage
-----
  python sargassum_collector.py              # collecte unique (toutes sources)
  python sargassum_collector.py --schedule   # collecte toutes les 6 h
  python sargassum_collector.py --query      # afficher les dernières entrées
  python sargassum_collector.py --simulate   # simulation OpenDrift seule

Fichier .env (même dossier)
---------------------------
  COPERNICUS_USERNAME=<login>      # https://data.marine.copernicus.eu
  COPERNICUS_PASSWORD=<mot_de_passe>
  AVISO_USERNAME=<login>           # https://www.aviso.altimetry.fr  (optionnel)
  AVISO_PASSWORD=<mot_de_passe>    # fallback sur Copernicus si absent

Dépendances
-----------
  pip install requests beautifulsoup4 numpy schedule copernicusmarine opendrift psutil
"""

import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import traceback
import warnings
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import requests
from bs4 import BeautifulSoup

try:
    import schedule
    HAS_SCHEDULE = True
except ImportError:
    HAS_SCHEDULE = False

# ── Configuration ─────────────────────────────────────────────────────────────

DB_PATH   = Path(__file__).parent / "sargassum_data.db"
CARIB     = dict(lat_min=8.0, lat_max=28.0, lon_min=-90.0, lon_max=-55.0)
HEADERS   = {"User-Agent": "Mozilla/5.0 (compatible; SargassumCollector/1.0)"}
TIMEOUT   = 45      # secondes par requête HTTP
SCHEDULE_H = 6     # intervalle entre deux collectes (mode --schedule)

# Supprime les avertissements SSL pour les sites à cert auto-signé (FORESEA/CNRS)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")


# ── Utilitaires ───────────────────────────────────────────────────────────────

def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def strip_html(html: str) -> str:
    return re.sub(r"<[^>]+>", " ", html or "").strip()

def safe_float(val) -> float | None:
    try:
        f = float(val)
        return None if (f != f) else f   # NaN → None
    except (TypeError, ValueError):
        return None


# ── Base de données ───────────────────────────────────────────────────────────

SCHEMA = """
PRAGMA journal_mode = WAL;

-- Courants géostrophiques AVISO+ DUACS (SSH → ugos/vgos)
CREATE TABLE IF NOT EXISTS aviso_geostrophic (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at     TEXT    NOT NULL,
    data_date        TEXT,
    dataset          TEXT,
    credentials_used TEXT,             -- 'AVISO+' ou 'Copernicus' (fallback)
    lat_min REAL, lat_max REAL, lon_min REAL, lon_max REAL,
    valid_pixels     INTEGER,
    mean_ugos        REAL,             -- vitesse géostrophique zonale moy (m/s)
    mean_vgos        REAL,             -- vitesse géostrophique méridionale moy (m/s)
    mean_speed       REAL,
    max_speed        REAL,
    dominant_dir_deg REAL,
    raw_metadata     TEXT
);

-- Prédictions de dérive OpenDrift (snapshots journaliers)
CREATE TABLE IF NOT EXISTS drift_predictions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    simulated_at     TEXT    NOT NULL,  -- horodatage de la simulation
    sim_start        TEXT,              -- t0 des particules
    sim_end          TEXT,              -- t0 + 5 jours
    n_particles      INTEGER,           -- nb de particules semées
    current_source   TEXT,              -- ex: 'DUACS+Copernicus'
    day_offset       INTEGER,           -- 0=position initiale, 1=+1j, …, 5=+5j
    hour_offset      INTEGER,           -- heures depuis t0 (0,3,…,120) ; jour = hour_offset//24
    lon_min          REAL,
    lon_max          REAL,
    lat_min          REAL,
    lat_max          REAL,
    active_fraction  REAL,              -- fraction de particules encore actives
    positions_json   TEXT,              -- JSON [[lon,lat], …] échantillon UNIFORME (scoring plages)
    positions_viz_json TEXT,            -- JSON [[lon,lat], …] échantillon dense RÉGIONAL (carte only)
    raw_metadata     TEXT
);

-- Rapports SIR hebdomadaires (HTML NOAA)
CREATE TABLE IF NOT EXISTS noaa_sir_reports (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at TEXT    NOT NULL,
    report_date  TEXT,                 -- ex: 20260216
    report_url   TEXT,                 -- URL PDF complet
    extra_files  TEXT,                 -- JSON : autres fichiers (KMZ…)
    raw_metadata TEXT                  -- JSON brut de la page
);

-- Données AFAI 7-jours (ERDDAP griddap, zone Caraïbes)
CREATE TABLE IF NOT EXISTS noaa_afai (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at     TEXT    NOT NULL,
    data_date        TEXT,             -- horodatage de la mesure satellite
    dataset          TEXT,
    lat_min REAL, lat_max REAL, lon_min REAL, lon_max REAL,
    stride           INTEGER,          -- pas d'échantillonnage
    total_pixels     INTEGER,          -- pixels sur la zone
    valid_pixels     INTEGER,          -- pixels non-NaN
    sargassum_pixels INTEGER,          -- pixels > seuil AFAI
    coverage_pct     REAL,             -- % de pixels avec sargasse
    mean_afai        REAL,
    max_afai         REAL,
    raw_metadata     TEXT
);

-- Métadonnées FORESEA CNRS
CREATE TABLE IF NOT EXISTS foresea_forecasts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at    TEXT    NOT NULL,
    page_title      TEXT,
    latest_post_date TEXT,
    latest_post_title TEXT,
    forecast_snippet TEXT,             -- extrait du contenu de la page
    product_links   TEXT,              -- JSON : liens vers données/AVISO+
    raw_metadata    TEXT
);

-- Posts WordPress Sargassum Monitoring
CREATE TABLE IF NOT EXISTS sargassum_monitoring (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at TEXT    NOT NULL,
    post_date    TEXT,
    post_title   TEXT,
    post_excerpt TEXT,
    post_url     TEXT,
    extra_posts  TEXT,                 -- JSON : autres posts récents
    raw_metadata TEXT
);

-- Courants de surface Caraïbes (Copernicus Marine)
CREATE TABLE IF NOT EXISTS copernicus_currents (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at     TEXT    NOT NULL,
    data_date        TEXT,             -- horodatage de la mesure
    dataset          TEXT,
    lat_min REAL, lat_max REAL, lon_min REAL, lon_max REAL,
    valid_pixels     INTEGER,
    mean_u           REAL,             -- vitesse zonale moyenne (m/s, + = Est)
    mean_v           REAL,             -- vitesse méridionale moy (m/s, + = Nord)
    mean_speed       REAL,             -- module moyen (m/s)
    max_speed        REAL,             -- module maximum (m/s)
    dominant_dir_deg REAL,             -- direction dominante en degrés (0-360, 0=Est)
    raw_metadata     TEXT
);

-- Captures webcam (image par caméra, toutes les heures)
CREATE TABLE IF NOT EXISTS webcam_captures (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at  TEXT    NOT NULL,   -- ISO-8601 local, ex: 2026-02-21T14:30:00
    island       TEXT    NOT NULL,   -- ex: 'Saint-Barth'
    camera_name  TEXT    NOT NULL,   -- ex: 'Flamand_Beach'
    camera_key   TEXT,               -- clé vision-environnement ou yt_id
    file_path    TEXT,               -- chemin absolu du .jpg (NULL si échec)
    success      INTEGER NOT NULL,   -- 1 = OK, 0 = échec
    http_status  INTEGER,            -- code HTTP reçu (NULL si exception réseau)
    file_size    INTEGER,            -- taille en bytes (NULL si échec)
    raw_metadata TEXT                -- JSON (url, erreur…)
);
"""

def get_conn(path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    # Migration idempotente : colonne hour_offset (résolution 3h) sur bases existantes
    cols = {row[1] for row in conn.execute("PRAGMA table_info(drift_predictions)")}
    if "hour_offset" not in cols:
        conn.execute("ALTER TABLE drift_predictions ADD COLUMN hour_offset INTEGER")
        conn.commit()
    # Migration idempotente : échantillon dense régional pour la carte
    if "positions_viz_json" not in cols:
        conn.execute("ALTER TABLE drift_predictions ADD COLUMN positions_viz_json TEXT")
        conn.commit()
    # Migration idempotente (2026-11-09) : jeu RÉGIONAL COMPLET pour le scoring
    # des plages — toutes les particules de la boîte Antilles, actives ET
    # échouées, JSON [[lon,lat,flag],…] compressé zlib (flag 1 = échouée).
    if "positions_region_blob" not in cols:
        conn.execute("ALTER TABLE drift_predictions ADD COLUMN positions_region_blob BLOB")
        conn.commit()
    conn.row_factory = sqlite3.Row
    return conn


# ── Source 1 : NOAA SIR ───────────────────────────────────────────────────────

def collect_noaa_sir(conn: sqlite3.Connection) -> bool:
    """
    Scrape la page SIR pour récupérer le rapport hebdomadaire PDF + KMZ.
    URL : https://cwcgom.aoml.noaa.gov/SIR/
    """
    url = "https://cwcgom.aoml.noaa.gov/SIR/"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

    # Dates de rapport dans les noms de fichiers PDF (format SIR_YYYYMMDD.pdf)
    pdf_dates = re.findall(r"SIR_(\d{8})\.pdf", r.text)
    pdf_dates.sort(reverse=True)
    latest_date = pdf_dates[0] if pdf_dates else None
    latest_url  = f"https://cwcgom.aoml.noaa.gov/SIR/pdf/SIR_{latest_date}.pdf" if latest_date else None

    # Autres fichiers liés (KMZ, CSV…)
    extra = re.findall(r'href=["\']([^"\']*\.(?:kmz|csv|nc|zip))["\']', r.text, re.I)
    extra_full = [f"https://cwcgom.aoml.noaa.gov/SIR/{e.lstrip('./')}" for e in extra]

    meta = {"pdf_dates_found": pdf_dates[:10], "extra_files": extra_full}

    conn.execute(
        """INSERT INTO noaa_sir_reports
           (collected_at, report_date, report_url, extra_files, raw_metadata)
           VALUES (?,?,?,?,?)""",
        (now_utc(), latest_date, latest_url,
         json.dumps(extra_full), json.dumps(meta)),
    )
    conn.commit()
    print(f"  ✅ NOAA SIR       | rapport={latest_date} | fichiers_extra={len(extra_full)}")
    return True


# ── Source 2 : NOAA ERDDAP AFAI 7-jours ──────────────────────────────────────

def collect_noaa_afai(conn: sqlite3.Connection) -> bool:
    """
    Télécharge via ERDDAP un échantillon de l'AFAI 7-jours sur les Caraïbes
    et calcule des statistiques agrégées.

    Dataset  : noaa_aoml_atlantic_oceanwatch_AFAI_7D
    Variable : AFAI (Alternative Floating Algae Index, W/m²/µm/sr)
    Résolution nominale ≈ 4 km → stride=10 → ~40 km (quelques centaines de pixels)
    Seuil sargasse : AFAI > 0.0001 (référence NOAA)
    """
    DATASET   = "noaa_aoml_atlantic_oceanwatch_AFAI_7D"
    ERDDAP    = "https://cwcgom.aoml.noaa.gov/erddap/griddap"
    STRIDE    = 10
    THRESHOLD = 0.0001

    url = (
        f"{ERDDAP}/{DATASET}.csv?"
        f"AFAI[(last)][({CARIB['lat_min']}):{STRIDE}:({CARIB['lat_max']})]"
        f"[({CARIB['lon_min']}):{STRIDE}:({CARIB['lon_max']})]"
    )
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

    lines = r.text.strip().splitlines()
    if len(lines) < 3:
        raise ValueError(f"Réponse ERDDAP trop courte ({len(lines)} lignes)")

    # Ligne 0 : noms des colonnes ; ligne 1 : unités ; lignes 2+ : données
    cols     = lines[0].split(",")
    t_idx    = next((i for i, h in enumerate(cols) if "time"  in h.lower()), 0)
    afai_idx = next((i for i, h in enumerate(cols) if "AFAI"  in h.upper()), 3)

    data_date   = None
    all_vals    = []
    valid_vals  = []
    nan_count   = 0

    for line in lines[2:]:
        parts = line.split(",")
        if len(parts) <= afai_idx:
            continue
        if data_date is None and len(parts) > t_idx:
            data_date = parts[t_idx].strip()
        raw = parts[afai_idx].strip()
        all_vals.append(raw)
        if raw.lower() in ("nan", ""):
            nan_count += 1
        else:
            v = safe_float(raw)
            if v is not None:
                valid_vals.append(v)

    total_pixels    = len(all_vals)
    valid_pixels    = len(valid_vals)
    sarg_pixels     = sum(1 for v in valid_vals if v > THRESHOLD)
    coverage_pct    = 100.0 * sarg_pixels / valid_pixels if valid_pixels else 0.0
    mean_afai       = float(np.mean(valid_vals)) if valid_vals else None
    max_afai        = float(np.max(valid_vals))  if valid_vals else None

    conn.execute(
        """INSERT INTO noaa_afai
           (collected_at, data_date, dataset,
            lat_min, lat_max, lon_min, lon_max,
            stride, total_pixels, valid_pixels, sargassum_pixels,
            coverage_pct, mean_afai, max_afai, raw_metadata)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            now_utc(), data_date, DATASET,
            CARIB["lat_min"], CARIB["lat_max"], CARIB["lon_min"], CARIB["lon_max"],
            STRIDE, total_pixels, valid_pixels, sarg_pixels,
            round(coverage_pct, 4),
            round(mean_afai, 8) if mean_afai is not None else None,
            round(max_afai,  8) if max_afai  is not None else None,
            json.dumps({"url": url, "nan_count": nan_count,
                        "threshold": THRESHOLD, "stride": STRIDE}),
        ),
    )
    conn.commit()
    print(f"  ✅ NOAA AFAI_7D   | date={data_date} | pixels_valides={valid_pixels} "
          f"| sargasse={sarg_pixels} ({coverage_pct:.2f}%)"
          f"| AFAI_moy={mean_afai:.6f}" if mean_afai else "")
    return True


# ── Source 3 : FORESEA CNRS ───────────────────────────────────────────────────

def collect_foresea(conn: sqlite3.Connection) -> bool:
    """
    Collecte via :
      - API REST WordPress  → derniers posts / dates de mise à jour
      - Page principale     → extrait de prévision, liens vers données AVISO+/Zenodo

    Note : les fichiers NetCDF de prévision sont protégés par un compte AVISO+.
    Ce collecteur capture les métadonnées publiques.
    """
    BASE = "https://sargassum-foresea.cnrs.fr"
    KSSL = dict(verify=False)   # certificat CNRS non reconnu par macOS

    # 1) Page principale
    r_page = requests.get(f"{BASE}/sargassum-forecast/",
                          headers=HEADERS, timeout=TIMEOUT, **KSSL)
    r_page.raise_for_status()
    soup = BeautifulSoup(r_page.text, "html.parser")
    page_title = soup.find("title")
    page_title = page_title.get_text(strip=True) if page_title else ""

    # Extrait de prévision (premier paragraphe significatif)
    forecast_snippet = ""
    for tag in soup.find_all(["p", "h2", "h3", "li"]):
        t = tag.get_text(strip=True)
        if len(t) > 60 and any(k in t.lower() for k in
                                ["forecast", "sargassum", "biomass", "aviso", "updated"]):
            forecast_snippet = t[:500]
            break

    # Liens vers données (AVISO+, Zenodo, NetCDF…)
    product_links = [
        {"url": a["href"], "text": a.get_text(strip=True)[:80]}
        for a in soup.find_all("a", href=True)
        if any(k in (a["href"] + a.get_text()).lower()
               for k in ["aviso", "zenodo", "netcdf", "download", "data", "product"])
    ][:15]

    # 2) API REST WordPress → derniers posts
    r_api = requests.get(f"{BASE}/wp-json/wp/v2/posts?per_page=5",
                         headers=HEADERS, timeout=TIMEOUT, **KSSL)
    latest_post_date  = None
    latest_post_title = None
    wp_posts = []

    if r_api.status_code == 200:
        posts = r_api.json()
        if posts:
            latest_post_date  = posts[0].get("date", "")[:10]
            latest_post_title = strip_html(posts[0]["title"]["rendered"])
        wp_posts = [
            {"date": p.get("date","")[:10],
             "title": strip_html(p["title"]["rendered"]),
             "link": p.get("link","")}
            for p in posts
        ]

    meta = {
        "page_url": f"{BASE}/sargassum-forecast/",
        "wp_posts": wp_posts,
        "product_links": product_links,
    }

    conn.execute(
        """INSERT INTO foresea_forecasts
           (collected_at, page_title, latest_post_date, latest_post_title,
            forecast_snippet, product_links, raw_metadata)
           VALUES (?,?,?,?,?,?,?)""",
        (now_utc(), page_title, latest_post_date, latest_post_title,
         forecast_snippet, json.dumps(product_links), json.dumps(meta)),
    )
    conn.commit()
    print(f"  ✅ FORESEA CNRS   | post={latest_post_date} | '{latest_post_title}'")
    return True


# ── Source 4 : Sargassum Monitoring ──────────────────────────────────────────

def collect_sargassum_monitoring(conn: sqlite3.Connection) -> bool:
    """
    Collecte via l'API REST WordPress de sargassummonitoring.com.
    Pas d'API de données géospatiales publique → on capture les derniers articles.
    """
    BASE = "https://sargassummonitoring.com"
    KSSL = dict(verify=False)

    # API REST WP : posts récents en anglais
    r = requests.get(f"{BASE}/wp-json/wp/v2/posts?per_page=5&lang=en",
                     headers=HEADERS, timeout=TIMEOUT, **KSSL)
    r.raise_for_status()
    posts = r.json()

    if not posts:
        raise ValueError("Aucun post retourné par l'API Sargassum Monitoring")

    latest        = posts[0]
    post_date     = latest.get("date", "")[:10]
    post_title    = strip_html(latest["title"]["rendered"])
    post_excerpt  = strip_html(latest.get("excerpt", {}).get("rendered", ""))[:400]
    post_url      = latest.get("link", "")

    extra_posts = [
        {"date":  p.get("date","")[:10],
         "title": strip_html(p["title"]["rendered"]),
         "url":   p.get("link","")}
        for p in posts[1:]
    ]

    # Métadonnées de la page officielle de carte
    r_map = requests.get(f"{BASE}/en/official-map-2025/",
                         headers=HEADERS, timeout=TIMEOUT, **KSSL)
    map_meta = {}
    if r_map.status_code == 200:
        soup_map = BeautifulSoup(r_map.text, "html.parser")
        og_desc  = soup_map.find("meta", property="og:description")
        og_title = soup_map.find("meta", property="og:title")
        map_meta = {
            "og_title":       og_title["content"] if og_title else "",
            "og_description": og_desc["content"]  if og_desc  else "",
            "map_url":        f"{BASE}/en/official-map-2025/",
        }

    meta = {"posts": extra_posts, "map_page": map_meta}

    conn.execute(
        """INSERT INTO sargassum_monitoring
           (collected_at, post_date, post_title, post_excerpt,
            post_url, extra_posts, raw_metadata)
           VALUES (?,?,?,?,?,?,?)""",
        (now_utc(), post_date, post_title, post_excerpt,
         post_url, json.dumps(extra_posts), json.dumps(meta)),
    )
    conn.commit()
    print(f"  ✅ SARG MONITORING | post={post_date} | '{post_title[:60]}'")
    return True


# ── Source 5 : Copernicus Marine – courants surface ───────────────────────────

def collect_copernicus(conn: sqlite3.Connection) -> bool:
    """
    Télécharge les courants de surface Caraïbes depuis Copernicus Marine (CMEMS).

    Produit  : MULTIOBS_GLO_PHY_MYNRT_015_003
    Dataset  : cmems_obs-mob_glo_phy-cur_nrt_0.25deg_PT1H-i
    Variables: uo (vitesse zonale, m/s), vo (vitesse méridionale, m/s)

    Prérequis
    ---------
    pip install copernicusmarine
    Variables : COPERNICUS_USERNAME / COPERNICUS_PASSWORD
    """
    try:
        import copernicusmarine
    except ImportError:
        raise ImportError("Installez le package : pip install copernicusmarine")

    username = os.environ.get("COPERNICUS_USERNAME", "").strip()
    password = os.environ.get("COPERNICUS_PASSWORD", "").strip()
    if not username or not password:
        raise EnvironmentError(
            "Identifiants Copernicus manquants.\n"
            "  Ajoutez dans votre shell ou dans .env :\n"
            "    COPERNICUS_USERNAME=votre_login\n"
            "    COPERNICUS_PASSWORD=votre_mot_de_passe\n"
            "  Inscription gratuite : https://data.marine.copernicus.eu"
        )

    # Strategie en cascade : prefere anfc (modele analyse+forecast, latence ~0h, 0.083 deg)
    # mais fallback sur nrt (observations satellite, latence 2-3j, 0.25 deg) avec decalages
    DATASET_ANFC = "cmems_mod_glo_phy_anfc_0.083deg_PT1H-m"   # haute resolution, frais
    DATASET_NRT  = "cmems_obs-mob_glo_phy-cur_nrt_0.25deg_PT1H-i"  # observations, plus ancien

    from datetime import timedelta
    fmt = "%Y-%m-%dT%H:%M:%S"

    def _open_ds(dataset_id: str, end_offset_days: int = 0):
        """Ouvre un dataset Copernicus sur fenetre [-2j; 0] decalee vers le passe."""
        end_dt   = datetime.now(timezone.utc) - timedelta(days=end_offset_days)
        start_dt = end_dt - timedelta(days=2)
        return copernicusmarine.open_dataset(
            dataset_id        = dataset_id,
            username          = username,
            password          = password,
            variables         = ["uo", "vo"],
            minimum_latitude  = CARIB["lat_min"],
            maximum_latitude  = CARIB["lat_max"],
            minimum_longitude = CARIB["lon_min"],
            maximum_longitude = CARIB["lon_max"],
            start_datetime    = start_dt.strftime(fmt),
            end_datetime      = end_dt.strftime(fmt),
        )

    # Cascade : anfc d abord, puis nrt avec fallbacks de fenetre de plus en plus anciens
    attempts = [
        (DATASET_ANFC, 0, "anfc 0.083deg (frais, haute resolution)"),
        (DATASET_NRT,  0, "nrt 0.25deg (observations satellite)"),
        (DATASET_NRT,  2, "nrt fallback fenetre [-4j; -2j]"),
        (DATASET_NRT,  4, "nrt fallback fenetre [-6j; -4j]"),
        (DATASET_NRT,  6, "nrt fallback fenetre [-8j; -6j]"),
    ]

    ds = None
    DATASET = None
    for ds_id, offset, label in attempts:
        try:
            ds = _open_ds(ds_id, end_offset_days=offset)
            DATASET = ds_id
            # Loggue uniquement les fallbacks (pas le cas nominal anfc)
            if ds_id != DATASET_ANFC or offset > 0:
                print(f"  ⚠️  Copernicus fallback : {label}")
            break
        except Exception as e:
            err_str = str(e)
            # Erreur de bornes temporelles : essayer le suivant
            if "CoordinatesOutOfDatasetBounds" in err_str or "coordinate" in err_str.lower():
                continue
            # Erreur sur anfc (indispo, auth, etc.) : on tente quand meme le nrt
            if ds_id == DATASET_ANFC:
                print(f"  ⚠️  anfc indispo ({type(e).__name__}), bascule sur nrt")
                continue
            raise
    if ds is None:
        raise RuntimeError("Copernicus indisponible : aucune source ne repond")

    # Dernière tranche temporelle disponible
    ds_t      = ds.isel(time=-1)
    data_date = str(ds_t.time.values)[:19]

    uo_raw = ds_t["uo"].values.flatten()
    vo_raw = ds_t["vo"].values.flatten()
    mask   = ~(np.isnan(uo_raw) | np.isnan(vo_raw))
    uo, vo = uo_raw[mask], vo_raw[mask]

    if len(uo) == 0:
        raise ValueError("Aucune donnée de courant valide sur la zone Caraïbes")

    speed       = np.sqrt(uo**2 + vo**2)
    mean_u      = float(np.mean(uo))
    mean_v      = float(np.mean(vo))
    mean_speed  = float(np.mean(speed))
    max_speed   = float(np.max(speed))
    # Convention : 0° = flux vers l'Est, 90° = flux vers le Nord
    dom_dir_deg = float(np.degrees(np.arctan2(mean_v, mean_u)) % 360)

    conn.execute(
        """INSERT INTO copernicus_currents
           (collected_at, data_date, dataset,
            lat_min, lat_max, lon_min, lon_max,
            valid_pixels, mean_u, mean_v, mean_speed, max_speed,
            dominant_dir_deg, raw_metadata)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            now_utc(), data_date, DATASET,
            CARIB["lat_min"], CARIB["lat_max"],
            CARIB["lon_min"], CARIB["lon_max"],
            int(len(uo)),
            round(mean_u,      4), round(mean_v,    4),
            round(mean_speed,  4), round(max_speed, 4),
            round(dom_dir_deg, 2),
            json.dumps({"dataset": DATASET, "valid_pixels": int(len(uo))}),
        ),
    )
    conn.commit()
    print(f"  ✅ COPERNICUS      | date={data_date} | vitesse_moy={mean_speed:.3f} m/s "
          f"| dir={dom_dir_deg:.1f}° | pixels={len(uo)}")
    return True


# ── Source 6 : AVISO+ DUACS – courants géostrophiques ────────────────────────

def collect_aviso_duacs(conn: sqlite3.Connection) -> bool:
    """
    Collecte les courants géostrophiques de surface (DUACS L4 NRT) sur les Caraïbes.

    Produit  : SEALEVEL_GLO_PHY_L4_NRT_008_046
    Dataset  : cmems_obs-sl_glo_phy-ssh_nrt_allsat-l4-duacs-0.125deg_P1D
    Variables: ugos (vitesse géostrophique zonale, m/s)
               vgos (vitesse géostrophique méridionale, m/s)

    Authentification : AVISO+ en priorité (via CMEMS), fallback Copernicus.
    Note : le NRT DUACS est hébergé sur CMEMS depuis 2024 ; les credentials
    AVISO+ classiques (FTP/THREDDS) ne donnent plus accès au NRT global.
    """
    try:
        import copernicusmarine
    except ImportError:
        raise ImportError("pip install copernicusmarine")

    DATASET = "cmems_obs-sl_glo_phy-ssh_nrt_allsat-l4-duacs-0.125deg_P1D"
    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=3)
    fmt      = "%Y-%m-%dT%H:%M:%S"

    # Essayer AVISO+ d'abord, puis Copernicus en fallback
    ds = None
    used_creds = None
    errors = []

    for user_env, pass_env, label in [
        ("AVISO_USERNAME",       "AVISO_PASSWORD",       "AVISO+"),
        ("COPERNICUS_USERNAME",  "COPERNICUS_PASSWORD",  "Copernicus"),
    ]:
        u = os.environ.get(user_env, "").strip()
        p = os.environ.get(pass_env, "").strip()
        if not u or not p:
            continue
        try:
            ds = copernicusmarine.open_dataset(
                dataset_id        = DATASET,
                username          = u,
                password          = p,
                variables         = ["ugos", "vgos"],
                minimum_latitude  = CARIB["lat_min"],
                maximum_latitude  = CARIB["lat_max"],
                minimum_longitude = CARIB["lon_min"],
                maximum_longitude = CARIB["lon_max"],
                start_datetime    = start_dt.strftime(fmt),
                end_datetime      = end_dt.strftime(fmt),
            )
            used_creds = label
            break
        except Exception as e:
            errors.append(f"{label}: {e}")

    if ds is None:
        raise RuntimeError(
            "Impossible d'accéder aux données DUACS.\n" + "\n".join(errors)
        )

    ds_t      = ds.isel(time=-1)
    data_date = str(ds_t.time.values)[:19]

    ugos_raw = ds_t["ugos"].values.flatten()
    vgos_raw = ds_t["vgos"].values.flatten()
    mask     = ~(np.isnan(ugos_raw) | np.isnan(vgos_raw))
    ugos, vgos = ugos_raw[mask], vgos_raw[mask]

    if len(ugos) == 0:
        raise ValueError("Aucune donnée géostrophique valide sur la zone Caraïbes")

    speed       = np.sqrt(ugos**2 + vgos**2)
    mean_u      = float(np.mean(ugos))
    mean_v      = float(np.mean(vgos))
    mean_speed  = float(np.mean(speed))
    max_speed   = float(np.max(speed))
    dom_dir_deg = float(np.degrees(np.arctan2(mean_v, mean_u)) % 360)
    ds.close()

    conn.execute(
        """INSERT INTO aviso_geostrophic
           (collected_at, data_date, dataset, credentials_used,
            lat_min, lat_max, lon_min, lon_max,
            valid_pixels, mean_ugos, mean_vgos, mean_speed, max_speed,
            dominant_dir_deg, raw_metadata)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            now_utc(), data_date, DATASET, used_creds,
            CARIB["lat_min"], CARIB["lat_max"], CARIB["lon_min"], CARIB["lon_max"],
            int(len(ugos)),
            round(mean_u,     4), round(mean_v,    4),
            round(mean_speed, 4), round(max_speed, 4),
            round(dom_dir_deg, 2),
            json.dumps({"dataset": DATASET, "credentials": used_creds,
                        "valid_pixels": int(len(ugos))}),
        ),
    )
    conn.commit()
    print(f"  ✅ AVISO+ DUACS    | creds={used_creds} | date={data_date} "
          f"| ugos_moy={mean_u:.3f} m/s | vgos_moy={mean_v:.3f} m/s "
          f"| speed_moy={mean_speed:.3f} m/s | dir={dom_dir_deg:.1f}°")
    return True


# ── Source 7 : OpenDrift – simulation de dérive 5 jours ──────────────────────

# ⚠️ Stokes drift désactivé par défaut sur ce VPS (3.9 GB RAM, pas de swap) :
# avec 3 readers (ANFC + GFS_wind + Stokes) et ~15 000 particules, OpenDrift
# atteint ~2.7 GB anon-rss et déclenche l'OOM-killer. Pour réactiver, il faut
# soit ajouter du swap (`fallocate -l 2G /swapfile && mkswap … && swapon …`),
# soit réduire stride AFAI à 4-5 pour avoir moins de particules.
ENABLE_STOKES_DRIFT = bool(int(os.environ.get("ENABLE_STOKES_DRIFT", "0")))


def _fetch_gfs_wind_caribbean(out_path: Path, forecast_hours: int = 120) -> str | None:
    """
    Télécharge le vent 10m GFS 0.25° depuis NOMADS pour la zone Caraïbes,
    concatène les pas (toutes les 3h) et écrit un NetCDF compatible OpenDrift
    avec les variables CF `x_wind` (= u10) et `y_wind` (= v10).

    Retourne la timestamp du run GFS utilisé (ISO) ou None si échec total.

    Source : NOMADS filter_gfs_0p25.pl — public, pas d'auth requise.
    Convention : windage ~1-2 % appliqué côté OpenDrift via drift:wind_drift_factor.
    """
    import cfgrib
    import xarray as xr

    # 1) Choisir le dernier run GFS dispo (~3h30 de latence après l'heure ronde)
    now = datetime.now(timezone.utc)
    available = now - timedelta(hours=3, minutes=30)
    run_hour = (available.hour // 6) * 6
    run_dt = available.replace(hour=run_hour, minute=0, second=0, microsecond=0)
    if run_dt > available:
        run_dt -= timedelta(hours=6)

    date_str = run_dt.strftime("%Y%m%d")
    hh       = run_dt.strftime("%H")

    # 2) Pas de prévision : toutes les 3h sur 5 jours = 41 fichiers
    fh_steps = list(range(0, forecast_hours + 1, 3))

    tmpdir = out_path.parent / "gfs_tmp"
    tmpdir.mkdir(exist_ok=True)

    datasets = []
    n_ok = 0
    for fh in fh_steps:
        fh_str = str(fh).zfill(3)
        url = (
            f"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
            f"?file=gfs.t{hh}z.pgrb2.0p25.f{fh_str}"
            f"&var_UGRD=on&var_VGRD=on&lev_10_m_above_ground=on"
            f"&leftlon={CARIB['lon_min'] - 3}&rightlon={CARIB['lon_max'] + 3}"
            f"&toplat={CARIB['lat_max'] + 3}&bottomlat={CARIB['lat_min'] - 3}"
            f"&dir=%2Fgfs.{date_str}%2F{hh}%2Fatmos"
        )
        grib_path = tmpdir / f"gfs_{date_str}_{hh}z_f{fh_str}.grib2"
        try:
            r = requests.get(url, timeout=120, stream=True)
            r.raise_for_status()
            with open(grib_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)
            ds = xr.open_dataset(grib_path, engine="cfgrib",
                                 backend_kwargs={"indexpath": ""})
            # GFS UGRD/VGRD à 10 m → noms cfgrib : u10, v10
            ds = ds[["u10", "v10"]].rename({"u10": "x_wind", "v10": "y_wind"})
            # NOMADS retourne longitudes 0-360, OpenDrift veut -180..180
            if float(ds["longitude"].max()) > 180.0:
                ds = ds.assign_coords(
                    longitude=(((ds["longitude"] + 180) % 360) - 180)
                ).sortby("longitude")
            # Subset Caraïbes (NOMADS ne respecte pas toujours le filtre URL).
            # GFS donne latitudes décroissantes (90 → -90) ; on slice puis on
            # remet en ordre croissant pour respecter la convention CF attendue
            # par OpenDrift (sinon le vent est interprété verticalement inversé).
            ds = ds.sel(
                latitude=slice(CARIB["lat_max"] + 3, CARIB["lat_min"] - 3),
                longitude=slice(CARIB["lon_min"] - 3, CARIB["lon_max"] + 3),
            ).sortby("latitude")
            # Garantir une dimension `time` (les fichiers single-step n'en ont pas)
            valid_time = run_dt + timedelta(hours=fh)
            if "time" not in ds.dims:
                ds = ds.expand_dims(time=[np.datetime64(valid_time.replace(tzinfo=None))])
            datasets.append(ds)
            n_ok += 1
        except Exception as e:
            print(f"    [warn] vent f{fh_str} indisponible : {e}")
        finally:
            if grib_path.exists():
                grib_path.unlink()

    # Cleanup des index cfgrib éventuels
    for f in tmpdir.glob("*.idx"):
        f.unlink()
    try:
        tmpdir.rmdir()
    except OSError:
        pass

    if n_ok < 4:
        print(f"    [warn] seulement {n_ok} pas vent récupérés — abandon vent")
        return None

    combined = xr.concat(datasets, dim="time").sortby("time")
    # Attributs CF minimum pour qu'OpenDrift reconnaisse
    combined["x_wind"].attrs.update({"units": "m s-1", "standard_name": "x_wind"})
    combined["y_wind"].attrs.update({"units": "m s-1", "standard_name": "y_wind"})
    combined.to_netcdf(out_path)
    combined.close()
    for ds in datasets:
        ds.close()

    print(f"    → vent GFS : run {date_str}_{hh}z, {n_ok}/{len(fh_steps)} pas, → {out_path.name}")
    return run_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch_stokes_drift_caribbean(
    out_path: Path,
    cp_user: str,
    cp_pass: str,
    start_dt: datetime,
    end_dt: datetime,
) -> bool:
    """
    Télécharge la dérive de Stokes (vagues) depuis CMEMS pour la zone Caraïbes.

    Dataset  : cmems_mod_glo_wav_anfc_0.083deg_PT3H-i (résolution 0.083°, pas 3h)
    Variables: VSDX, VSDY (m/s) → renommées vers les noms CF attendus par OpenDrift
              (sea_surface_wave_stokes_drift_x_velocity / _y_velocity)

    La dérive de Stokes représente le transport résiduel induit par les vagues —
    typiquement 5-15 cm/s vers l'ouest aux Antilles (houle d'alizé). Elle s'ajoute
    au courant et au windage pour préciser la direction d'arrivée des sargasses.

    Retourne True si succès, False sinon.
    """
    try:
        import copernicusmarine
    except ImportError:
        print("    [warn] copernicusmarine indisponible — Stokes drift désactivé")
        return False

    fmt = "%Y-%m-%dT%H:%M:%S"
    try:
        ds = copernicusmarine.open_dataset(
            dataset_id        = "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i",
            username          = cp_user, password = cp_pass,
            variables         = ["VSDX", "VSDY"],
            minimum_latitude  = CARIB["lat_min"] - 3,
            maximum_latitude  = CARIB["lat_max"] + 3,
            minimum_longitude = CARIB["lon_min"] - 3,
            maximum_longitude = CARIB["lon_max"] + 3,
            start_datetime    = start_dt.strftime(fmt),
            end_datetime      = end_dt.strftime(fmt),
        )
    except Exception as e:
        print(f"    [warn] CMEMS Wave indisponible : {e}")
        return False

    # Renommage vers les noms CF attendus par OpenDrift
    ds = ds.rename({
        "VSDX": "sea_surface_wave_stokes_drift_x_velocity",
        "VSDY": "sea_surface_wave_stokes_drift_y_velocity",
    })
    ds["sea_surface_wave_stokes_drift_x_velocity"].attrs.update({
        "units": "m s-1",
        "standard_name": "sea_surface_wave_stokes_drift_x_velocity",
    })
    ds["sea_surface_wave_stokes_drift_y_velocity"].attrs.update({
        "units": "m s-1",
        "standard_name": "sea_surface_wave_stokes_drift_y_velocity",
    })
    # Sous-échantillonnage temporel à 6h (1 sur 2) — réduit la RAM consommée
    # par OpenDrift de ~50 % ; les Stokes drift varient peu sur 6h.
    ds = ds.isel(time=slice(None, None, 2))
    n_t = int(ds.sizes.get("time", 0))
    ds.to_netcdf(out_path)
    ds.close()

    print(f"    → Stokes drift : {n_t} pas de temps (6h) → {out_path.name}")
    return True


def _fetch_afai_positions(stride: int = 3, threshold: float = 0.0001):
    """
    Interroge ERDDAP pour obtenir les lat/lon réels des pixels avec
    signal sargasse (AFAI > threshold). Retourne (lons, lats, data_date).
    """
    DATASET = "noaa_aoml_atlantic_oceanwatch_AFAI_7D"
    ERDDAP  = "https://cwcgom.aoml.noaa.gov/erddap/griddap"
    url = (
        f"{ERDDAP}/{DATASET}.csv?"
        f"AFAI[(last)][({CARIB['lat_min']}):{stride}:({CARIB['lat_max']})]"
        f"[({CARIB['lon_min']}):{stride}:({CARIB['lon_max']})]"
    )
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

    lats, lons = [], []
    data_date  = None

    for line in r.text.strip().splitlines()[2:]:   # skip header + units rows
        parts = line.split(",")
        if len(parts) < 4:
            continue
        if data_date is None:
            data_date = parts[0].strip()
        raw = parts[3].strip()
        if raw.lower() in ("nan", ""):
            continue
        try:
            v = float(raw)
            if v > threshold:
                lats.append(float(parts[1]))
                lons.append(float(parts[2]))
        except ValueError:
            pass

    return np.array(lons), np.array(lats), data_date


def _distance_cote_km(lons: np.ndarray, lats: np.ndarray, pas_deg: float = 0.015) -> np.ndarray:
    """
    Distance (km) de chaque point à la terre la plus proche (trait de côte GSHHG).

    Sert au masque côtier des points de départ. Pourquoi ce masque : mesuré le
    2026-11-10 sur un an de composites AFAI 7 jours, le taux de « détection »
    à moins de 10 km d'une côte reste entre 23 et 44 % des pixels lisibles
    TOUTE L'ANNÉE (décembre-février comme mai-juillet), alors qu'au large il
    suit la saison (0,7 % hors saison, 7 à 9 % en pleine saison). Près des
    côtes le satellite voit donc surtout un faux signal lié à la terre, pas
    des sargasses. Part de faux signal : > 90 % à moins de 10 km, ~75 % de
    10 à 15 km, ~50 % de 15 à 20 km, ~10 % au-delà de 30 km.

    Méthode : grille fine autour des points, masque terre, puis distance à la
    cellule de terre la plus proche. Fiable jusqu'à ~50 km (marge de la grille).
    """
    from roaring_landmask import RoaringLandmask
    from scipy.ndimage import distance_transform_edt

    lons = np.asarray(lons, dtype="float64")
    lats = np.asarray(lats, dtype="float64")
    marge = 0.5   # degrés : la terre au-delà n'est pas vue (distance alors >= ~50 km)
    glat = np.arange(lats.min() - marge, lats.max() + marge + pas_deg, pas_deg)
    glon = np.arange(lons.min() - marge, lons.max() + marge + pas_deg, pas_deg)
    grille_lon, grille_lat = np.meshgrid(glon, glat)
    terre = RoaringLandmask.new().contains_many(
        grille_lon.ravel(), grille_lat.ravel()
    ).reshape(grille_lon.shape)
    if not terre.any():
        return np.full(len(lons), np.inf)
    km_lat = pas_deg * 111.0
    km_lon = km_lat * float(np.cos(np.radians(lats.mean())))
    dist = distance_transform_edt(~terre, sampling=(km_lat, km_lon))
    ii = np.rint((lats - glat[0]) / pas_deg).astype(int)
    jj = np.rint((lons - glon[0]) / pas_deg).astype(int)
    return dist[ii, jj]


def _lire_masque_km() -> float:
    """
    Rayon du masque côtier (km) lu dans SARGA_SEED_COAST_KM. 0 = masque éteint.

    Une valeur illisible (« 15km », « quinze ») lève ValueError : on préfère
    une simulation en échec, signalée par logs/ECHEC_SIMULATION, à une
    simulation qui repartirait en silence avec les points côtiers.
    """
    brut = (os.environ.get("SARGA_SEED_COAST_KM", "") or "0").strip()
    try:
        km = float(brut)
    except ValueError:
        raise ValueError(f"SARGA_SEED_COAST_KM illisible : {brut!r} (attendu un nombre de km)")
    if km < 0 or not np.isfinite(km):
        raise ValueError(f"SARGA_SEED_COAST_KM hors limites : {brut!r}")
    return km


def _masque_cotier(lons: np.ndarray, lats: np.ndarray, km: float) -> np.ndarray:
    """
    Points de départ à GARDER (tableau de booléens) : ceux à au moins `km` km
    de toute côte. km <= 0 garde tout. Un point aux coordonnées invalides (NaN)
    est écarté, il ne pourrait de toute façon pas être semé.
    """
    lons = np.asarray(lons, dtype="float64")
    lats = np.asarray(lats, dtype="float64")
    if km <= 0 or len(lons) == 0:
        return np.ones(len(lons), dtype=bool)
    valides = np.isfinite(lons) & np.isfinite(lats)
    garder = np.zeros(len(lons), dtype=bool)
    if valides.any():
        garder[valides] = _distance_cote_km(lons[valides], lats[valides]) >= km
    return garder


def simulate_drift(conn: sqlite3.Connection) -> bool:
    """
    Simulation de dérive de sargasses sur 5 jours avec OpenDrift.

    Étapes
    ------
    1. Positions de départ : pixels AFAI > seuil depuis ERDDAP (stride=3, ~12 km)
    2. Courants : AVISO+ DUACS géostrophiques + Copernicus totaux (via NetCDF temp)
    3. Modèle   : OceanDrift (advection pure, pas de diffusion)
    4. Résultats: snapshots journaliers (j+0 … j+5) dans drift_predictions

    Prérequis
    ---------
    pip install opendrift psutil
    Variables : COPERNICUS_USERNAME + COPERNICUS_PASSWORD
    """
    try:
        from opendrift.models.oceandrift import OceanDrift
        from opendrift.readers.reader_netCDF_CF_generic import Reader as NCReader
        import copernicusmarine
        import xarray as xr
    except ImportError as e:
        raise ImportError(f"pip install opendrift psutil  ({e})")

    cp_user = os.environ.get("COPERNICUS_USERNAME", "").strip()
    cp_pass = os.environ.get("COPERNICUS_PASSWORD", "").strip()
    if not cp_user or not cp_pass:
        raise EnvironmentError("COPERNICUS_USERNAME/PASSWORD requis pour la simulation")

    # ── 1. Positions de graine depuis NOAA AFAI ──────────────────────────────
    # stride paramétrable pour pouvoir MESURER le lien entre nombre de
    # particules et empreinte mémoire (défaut 3 = comportement historique).
    _stride = int(os.environ.get("SARGA_AFAI_STRIDE", "3"))
    print(f"    → Positions sargasses (ERDDAP AFAI stride={_stride})…")
    lons_seed, lats_seed, afai_date = _fetch_afai_positions(stride=_stride)
    n_particles = len(lons_seed)

    # ── 1bis. Masque côtier des points de départ ─────────────────────────────
    # SARGA_SEED_COAST_KM=15 écarte les points à moins de 15 km d'une côte,
    # qui sont surtout du faux signal (voir _distance_cote_km). À 0 ou absent,
    # rien ne change.
    # Depuis le 2026-11-11 : si le masque est demandé et échoue, la simulation
    # ÉCHOUE (exception). Avant, elle repartait en silence avec tous les points
    # côtiers, et les plages repassaient en « moyen » sans que rien ne le dise.
    # Une simulation en échec garde la dernière dérive (masquée) en base et pose
    # logs/ECHEC_SIMULATION, que le contrôle de santé quotidien signale.
    _coast_km = _lire_masque_km()
    n_avant_masque = n_particles
    if _coast_km > 0 and n_particles > 0:
        _loin = _masque_cotier(lons_seed, lats_seed, _coast_km)
        print(f"    → Masque côtier {_coast_km:g} km : {int((~_loin).sum())} points écartés "
              f"sur {n_particles}, {int(_loin.sum())} gardés")
        lons_seed, lats_seed = lons_seed[_loin], lats_seed[_loin]
        n_particles = len(lons_seed)
        if n_particles == 0:
            # La grille de repli sèmerait des particules fictives partout : on refuse.
            raise RuntimeError(f"le masque côtier {_coast_km:g} km a écarté les "
                               f"{n_avant_masque} points de départ, simulation annulée")

    if n_particles == 0:
        # Fallback : grille régulière sur la zone
        print("    → Aucun pixel sargasse détecté — grille de fallback 5°×5°")
        lo_g, la_g = np.meshgrid(
            np.arange(CARIB["lon_min"], CARIB["lon_max"], 5.0),
            np.arange(CARIB["lat_min"], CARIB["lat_max"], 5.0),
        )
        lons_seed, lats_seed = lo_g.flatten(), la_g.flatten()
        n_particles = len(lons_seed)

    print(f"    → {n_particles} particules à semer | AFAI date={afai_date}")

    # ── 2. Téléchargement des champs de courants ──────────────────────────────
    end_dt   = datetime.now(timezone.utc)
    fmt      = "%Y-%m-%dT%H:%M:%S"

    tmpdir = Path(tempfile.mkdtemp(prefix="sarg_drift_"))
    try:
        # CMEMS Global Ocean Physics Analysis + Forecast
        # Couvre J-1 à J+6, donc toute la durée de simulation (5 jours)
        print("    → Téléchargement CMEMS ANFC (analyse + prévision 5 jours)…")
        ds_anfc = copernicusmarine.open_dataset(
            dataset_id        = "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
            username          = cp_user, password = cp_pass,
            variables         = ["uo", "vo"],
            minimum_latitude  = CARIB["lat_min"] - 3,
            maximum_latitude  = CARIB["lat_max"] + 3,
            minimum_longitude = CARIB["lon_min"] - 3,
            maximum_longitude = CARIB["lon_max"] + 3,
            minimum_depth     = 0.5,
            maximum_depth     = 0.5,
            start_datetime    = (end_dt - timedelta(days=1)).strftime(fmt),
            end_datetime      = (end_dt + timedelta(days=6)).strftime(fmt),
        )
        # Renommage → noms CF attendus par OpenDrift
        ds_anfc = ds_anfc.rename({
            "uo": "x_sea_water_velocity",
            "vo": "y_sea_water_velocity",
        })
        anfc_nc = tmpdir / "anfc.nc"
        ds_anfc.to_netcdf(anfc_nc)
        ds_anfc.close()

        # ── 2bis. Téléchargement du vent GFS 10 m (NOMADS) ───────────────────
        wind_nc = tmpdir / "gfs_wind.nc"
        wind_run = _fetch_gfs_wind_caribbean(wind_nc, forecast_hours=120)

        # ── 2ter. Téléchargement du Stokes drift (vagues) CMEMS Wave ─────────
        # Désactivé par défaut sur VPS contraint en RAM (cf. ENABLE_STOKES_DRIFT)
        stokes_nc = tmpdir / "stokes.nc"
        stokes_ok = False
        if ENABLE_STOKES_DRIFT:
            stokes_ok = _fetch_stokes_drift_caribbean(
                stokes_nc, cp_user, cp_pass,
                end_dt - timedelta(days=1),
                end_dt + timedelta(days=6),
            )
        else:
            print("    → Stokes drift désactivé (ENABLE_STOKES_DRIFT=0)")

        # ── 3. Simulation OpenDrift ───────────────────────────────────────────
        print("    → Initialisation OpenDrift…")
        reader_anfc = NCReader(str(anfc_nc))
        readers = [reader_anfc]
        if wind_run and wind_nc.exists():
            try:
                reader_wind = NCReader(str(wind_nc))
                readers.append(reader_wind)
                print(f"    → vent activé (run {wind_run})")
            except Exception as e:
                print(f"    [warn] reader vent KO : {e}")
                wind_run = None
        if stokes_ok and stokes_nc.exists():
            try:
                reader_stokes = NCReader(str(stokes_nc))
                readers.append(reader_stokes)
                print("    → Stokes drift activé")
            except Exception as e:
                print(f"    [warn] reader Stokes KO : {e}")
                stokes_ok = False

        od = OceanDrift(loglevel=50)      # silent
        od.add_reader(readers)
        od.set_config("general:use_auto_landmask", True)
        # Échouage : une particule qui touche la côte est désactivée avec le
        # statut « stranded » et sa position d'impact est conservée dans la
        # sortie. C'est le comportement par défaut, rendu explicite ici car
        # l'extraction ci-dessous en dépend : avant le 2026-11-09 ces
        # particules étaient tout simplement perdues (NaN) avant le scoring,
        # alors que ce sont précisément les arrivées sur les plages.
        od.set_config("general:coastline_action", "stranding")
        # Windage : sargasses dérivent à ~1 % de la vitesse du vent (Putman et al. 2018)
        # Appliqué au seeding ; calibré ensuite via sarga_calibration_spatial.py.
        if wind_run:
            od.set_config("seed:wind_drift_factor", 0.01)

        sim_start_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        od.seed_elements(lon=lons_seed, lat=lats_seed, time=sim_start_naive)

        sim_out = tmpdir / "output.nc"
        print(f"    → Simulation ({n_particles} ptcl × 5 jours × pas 3h)…")
        od.run(
            duration  = timedelta(days=5),
            time_step = timedelta(hours=3),
            outfile   = str(sim_out),
        )

        # ── 4. Extraction et stockage des snapshots toutes les 3h ─────────────
        # OpenDrift calcule un pas toutes les 3h (time_step=3h) sur 5 jours
        # → 41 indices temporels (0,3,…,120h). On stocke chacun ; day_offset
        # reste = hour_offset//24 pour la compatibilité des vues journalières
        # (snapshot journalier = hour_offset % 24 == 0).
        sim_ds = xr.open_dataset(sim_out)
        times  = sim_ds["time"].values
        n_times = len(times)

        simulated_at = now_utc()
        sim_start_str = sim_start_naive.strftime("%Y-%m-%dT%H:%M:%S")
        sim_end_str   = (sim_start_naive + timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%S")

        current_source = "DUACS+Copernicus"
        if wind_run:
            current_source += "+GFS_wind1pct"
        if stokes_ok:
            current_source += "+Stokes"

        # ── Échantillon FIXE de particules, biaisé vers la région SBH ─────────
        # Avant : 500 pts répartis sur tout le bassin → ~15 seulement près de
        # SBH, donc traînée/animation invisibles à l'échelle régionale.
        # Maintenant : on privilégie les particules qui passent par la boîte
        # Petites Antilles (résidentes ET arrivantes au fil des 5 jours), plus
        # un échantillon « contexte » du reste du bassin. Le jeu d'indices est
        # FIXE pour tous les snapshots → l'identité des particules est conservée
        # d'un pas à l'autre (traînée cohérente côté carte).
        REG_LON = (-66.0, -59.0)   # boîte un peu plus large que l'affichage
        REG_LAT = (15.5, 20.5)
        REG_BUDGET = 450           # particules régionales max
        CTX_BUDGET = 150           # particules contexte bassin max

        _lon_all = sim_ds["lon"].values
        _lat_all = sim_ds["lat"].values
        _taxis   = sim_ds["lon"].dims.index("time")
        _lon_t   = np.moveaxis(_lon_all, _taxis, 0)   # (n_times, n_traj)
        _lat_t   = np.moveaxis(_lat_all, _taxis, 0)

        # ── Particules ÉCHOUÉES : les garder, à leur point d'impact ──────────
        # OpenDrift désactive une particule qui touche la côte : sa position est
        # écrite au pas d'échouage (statut « stranded ») puis devient NaN. On
        # propage (forward-fill) la dernière position et le dernier statut le
        # long du temps : une particule échouée à h+30 reste comptée sur sa
        # plage à j+2, j+3… (les sargasses restent plusieurs jours à terre).
        # Les autres désactivations (sortie de grille, données manquantes)
        # sont exclues : ni actives, ni échouées.
        def _ffill_time(a: np.ndarray) -> np.ndarray:
            out = a.astype(float).copy()
            for _t in range(1, out.shape[0]):
                _m = np.isnan(out[_t])
                out[_t, _m] = out[_t - 1, _m]
            return out

        _lon_ff = _ffill_time(_lon_t)
        _lat_ff = _ffill_time(_lat_t)
        _stranded_t = np.zeros_like(_lon_t, dtype=bool)
        if "status" in sim_ds.variables:
            _st_raw = np.moveaxis(sim_ds["status"].values, _taxis, 0).astype(float)
            _meanings = str(sim_ds["status"].attrs.get("flag_meanings", "")).split()
            if "stranded" in _meanings:
                _strand_num = _meanings.index("stranded")
                _st_ff = _ffill_time(_st_raw)
                _stranded_t = _st_ff == _strand_num
        # Active = position valide ET pas (encore) échouée
        _active_t = ~np.isnan(_lon_t) & ~_stranded_t
        n_stranded_final = int(_stranded_t[-1].sum())
        print(f"    → Échouages sur 5 jours : {n_stranded_final} particules "
              f"({100.0 * n_stranded_final / max(n_particles, 1):.1f} %) — conservées pour le scoring")

        # Boîte du jeu RÉGIONAL COMPLET (scoring plages). Doit déborder d'au
        # moins ~150 km (3σ du score régional, σ = 50 km) autour des 5 îles :
        # Martinique 14.4°N → 12.5 ; Saint-Martin 18.1°N → 20.5 ;
        # côte est Martinique −60.8 → −58.5 ; ouest Guadeloupe −61.8 → −65.5.
        SCORE_LON = (-65.5, -58.5)
        SCORE_LAT = (12.5, 20.5)
        _in_box = (
            (_lon_t >= REG_LON[0]) & (_lon_t <= REG_LON[1]) &
            (_lat_t >= REG_LAT[0]) & (_lat_t <= REG_LAT[1])
        )
        _ever   = _in_box.any(axis=0)          # particule passant par la zone à un moment
        reg_idx = np.where(_ever)[0]
        if reg_idx.size > REG_BUDGET:
            reg_idx = reg_idx[np.round(np.linspace(0, reg_idx.size - 1, REG_BUDGET)).astype(int)]
        ctx_pool = np.where(~_ever)[0]
        if ctx_pool.size > CTX_BUDGET:
            ctx_idx = ctx_pool[np.round(np.linspace(0, ctx_pool.size - 1, CTX_BUDGET)).astype(int)]
        else:
            ctx_idx = ctx_pool
        if reg_idx.size or ctx_idx.size:
            sample_idx = np.unique(np.concatenate([reg_idx, ctx_idx]))
        else:
            sample_idx = np.arange(min(n_particles, 600))
        print(f"    → Échantillon stocké : {reg_idx.size} régionaux + {ctx_idx.size} contexte = {sample_idx.size} pts/snapshot")

        rows_drift = []
        n_daily = 0
        for t_idx in range(n_times):
            hour_offset = t_idx * 3
            day_offset  = hour_offset // 24
            if hour_offset % 24 == 0:
                n_daily += 1
            lo_t  = _lon_t[t_idx]
            la_t  = _lat_t[t_idx]
            active = _active_t[t_idx]           # en mer (exclut les échouées)
            n_act  = int(active.sum())          # particules actives (tout le bassin)
            act_frac = n_act / n_particles if n_particles > 0 else 0.0

            # (0) Jeu RÉGIONAL COMPLET (positions_region_blob) — scoring plages.
            #     Toutes les particules de la boîte Antilles, actives (flag 0)
            #     ou échouées à leur point d'impact (flag 1). Pas d'échantillon,
            #     pas d'extrapolation : beaches.py score avec ratio = 1.
            stranded = _stranded_t[t_idx]
            lo_f, la_f = _lon_ff[t_idx], _lat_ff[t_idx]
            keep = (active | stranded) & ~np.isnan(lo_f) & (
                (lo_f >= SCORE_LON[0]) & (lo_f <= SCORE_LON[1]) &
                (la_f >= SCORE_LAT[0]) & (la_f <= SCORE_LAT[1])
            )
            region = [
                [round(float(lo), 3), round(float(la), 3), int(fl)]
                for lo, la, fl in zip(lo_f[keep], la_f[keep], stranded[keep])
            ]
            n_region_stranded = int((stranded & keep).sum())
            region_blob = zlib.compress(json.dumps(region, separators=(",", ":")).encode())

            # (1) Échantillon UNIFORME (positions_json) — représentatif, sert au
            #     scoring plages : ratio = n_active/n_sample suppose l'uniformité.
            lo_u, la_u = lo_t[active], la_t[active]
            if n_act > 500:
                idx = np.round(np.linspace(0, n_act - 1, 500)).astype(int)
                lo_u, la_u = lo_u[idx], la_u[idx]
            positions = [
                [round(float(lo), 4), round(float(la), 4)]
                for lo, la in zip(lo_u, la_u)
            ]

            # (2) Échantillon dense RÉGIONAL (positions_viz_json) — carte uniquement.
            #     Jeu d'indices FIXE (identité conservée) ; on retire les inactives.
            lo_s = lo_t[sample_idx]
            la_s = la_t[sample_idx]
            sel_active = ~np.isnan(lo_s)
            lo_v, la_v = lo_s[sel_active], la_s[sel_active]
            positions_viz = [
                [round(float(lo), 4), round(float(la), 4)]
                for lo, la in zip(lo_v, la_v)
            ]

            rows_drift.append((
                simulated_at, sim_start_str, sim_end_str,
                n_particles,
                current_source,
                day_offset,
                hour_offset,
                round(float(np.nanmin(lo_u)), 3) if n_act else None,
                round(float(np.nanmax(lo_u)), 3) if n_act else None,
                round(float(np.nanmin(la_u)), 3) if n_act else None,
                round(float(np.nanmax(la_u)), 3) if n_act else None,
                round(act_frac, 4),
                json.dumps(positions),
                json.dumps(positions_viz),
                json.dumps({
                    "day": day_offset,
                    "hour": hour_offset,
                    "n_active": n_act,
                    "n_stranded": int(stranded.sum()),
                    "n_stored": len(positions),
                    "n_viz": len(positions_viz),
                    "n_region": len(region),
                    "n_region_stranded": n_region_stranded,
                    "afai_date": afai_date,
                    "wind_run": wind_run,
                    "wind_drift_factor": 0.01 if wind_run else 0.0,
                    "stokes_drift": stokes_ok,
                    # Masque côtier des points de départ (0 = aucun). Permet de
                    # séparer les régimes dans forecast_archive et la vérification.
                    # Grille de repli (ERDDAP sans aucun point) : régime INCONNU (None),
                    # pour ne pas être prise pour l'ancien régime « sans masque » (0).
                    "seed_coast_km": _coast_km if n_avant_masque > 0 else None,
                    "n_seed_avant_masque": int(n_avant_masque),
                    "grille_repli": n_avant_masque == 0,
                }),
                region_blob,
            ))

        conn.executemany(
            """INSERT INTO drift_predictions
               (simulated_at, sim_start, sim_end, n_particles, current_source,
                day_offset, hour_offset, lon_min, lon_max, lat_min, lat_max,
                active_fraction, positions_json, positions_viz_json, raw_metadata,
                positions_region_blob)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows_drift,
        )
        # Purge : conserver les 90 dernières simulations (≈ 3 sem. à 4/j).
        # Chaque sim = 41 lignes 3h ; sans purge la table grossit ~7× plus vite.
        conn.execute("""
            DELETE FROM drift_predictions
            WHERE simulated_at NOT IN (
                SELECT DISTINCT simulated_at FROM drift_predictions
                ORDER BY simulated_at DESC LIMIT 90
            )
        """)
        conn.commit()

        sim_ds.close()
        print(f"  ✅ DRIFT SIM       | t0={sim_start_str} | particules={n_particles} "
              f"| {len(rows_drift)} snapshots 3h ({n_daily} journaliers j+0…j+5) stockés")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    return True


# ── Orchestration ─────────────────────────────────────────────────────────────

COLLECTORS = [
    ("NOAA SIR (rapport hebdo)",  collect_noaa_sir),
    ("NOAA AFAI 7-jours",         collect_noaa_afai),
    ("FORESEA CNRS",              collect_foresea),
    ("Sargassum Monitoring",      collect_sargassum_monitoring),
    ("Copernicus Marine",         collect_copernicus),
    ("AVISO+ DUACS géostrophique", collect_aviso_duacs),
]

def run_all(db_path: Path = DB_PATH):
    ts = now_utc()
    print(f"\n{'='*55}")
    print(f"🌊  Collecte sargasses  —  {ts}")
    print(f"{'='*55}")
    conn = get_conn(db_path)
    ok = 0
    for name, fn in COLLECTORS:
        print(f"\n[{name}]")
        try:
            fn(conn)
            ok += 1
        except EnvironmentError as e:
            print(f"  ⚠️  Config manquante : {e}")
        except ImportError as e:
            print(f"  ⚠️  Dépendance manquante : {e}")
        except Exception:
            print(f"  ❌  Erreur :")
            traceback.print_exc()
    conn.close()
    print(f"\n→  {ok}/{len(COLLECTORS)} sources OK  |  BD : {db_path.resolve()}")


# ── Affichage des dernières entrées ───────────────────────────────────────────

def query_latest(db_path: Path = DB_PATH):
    conn = get_conn(db_path)
    print("\n📊  Dernières entrées par table\n")

    specs = {
        "noaa_sir_reports": [
            "report_date", "report_url",
        ],
        "noaa_afai": [
            "data_date", "total_pixels", "valid_pixels",
            "sargassum_pixels", "coverage_pct", "mean_afai", "max_afai",
        ],
        "foresea_forecasts": [
            "latest_post_date", "latest_post_title", "forecast_snippet",
        ],
        "sargassum_monitoring": [
            "post_date", "post_title", "post_excerpt",
        ],
        "copernicus_currents": [
            "data_date", "valid_pixels",
            "mean_u", "mean_v", "mean_speed", "max_speed", "dominant_dir_deg",
        ],
        "aviso_geostrophic": [
            "data_date", "credentials_used", "valid_pixels",
            "mean_ugos", "mean_vgos", "mean_speed", "dominant_dir_deg",
        ],
        "drift_predictions": [
            "sim_start", "sim_end", "n_particles", "current_source",
            "day_offset", "lon_min", "lon_max", "lat_min", "lat_max", "active_fraction",
        ],
    }

    # Colonne d'horodatage selon la table
    ts_col = {
        "drift_predictions": "simulated_at",
    }

    for table, fields in specs.items():
        tc   = ts_col.get(table, "collected_at")
        cols = ", ".join(fields)
        row  = conn.execute(
            f"SELECT {tc}, {cols} FROM {table} ORDER BY {tc} DESC LIMIT 1"
        ).fetchone()
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"┌─ {table}  ({count} enregistrement(s))")
        if row:
            print(f"│  {tc:<21}: {row[tc]}")
            for f in fields:
                val = row[f]
                if isinstance(val, str) and len(val) > 100:
                    val = val[:100] + "…"
                print(f"│  {f:<21}: {val}")
        else:
            print("│  (vide)")
        print()

    conn.close()


# ── Point d'entrée ────────────────────────────────────────────────────────────

def load_dotenv(path: Path = Path(".env")):
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip().strip("'\""))

if __name__ == "__main__":
    load_dotenv()

    if "--query" in sys.argv:
        query_latest()
    elif "--simulate" in sys.argv:
        conn = get_conn()
        print("\n[OpenDrift – simulation dérive]")
        code_retour = 0
        try:
            simulate_drift(conn)
        except Exception:
            traceback.print_exc()
            # Code 1 : sargassum_run_linux.sh pose alors logs/ECHEC_SIMULATION.
            # Avant le 2026-11-11 l'exception était avalée et le script
            # sortait en succès, donc aucun échec Python n'était jamais signalé.
            code_retour = 1
        finally:
            conn.close()
        sys.exit(code_retour)
    elif "--schedule" in sys.argv:
        if not HAS_SCHEDULE:
            print("Installez schedule : pip install schedule")
            sys.exit(1)
        print(f"⏰  Collecte planifiée toutes les {SCHEDULE_H} h.  Ctrl+C pour arrêter.")
        run_all()
        schedule.every(SCHEDULE_H).hours.do(run_all)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        run_all()
