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

DB_PATH   = Path("./sargassum_data.db")
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
    lon_min          REAL,
    lon_max          REAL,
    lat_min          REAL,
    lat_max          REAL,
    active_fraction  REAL,              -- fraction de particules encore actives
    positions_json   TEXT,              -- JSON [[lon,lat], …] (max 500 pts)
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
"""

def get_conn(path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
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

    DATASET = "cmems_obs-mob_glo_phy-cur_nrt_0.25deg_PT1H-i"

    from datetime import timedelta
    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=2)
    fmt      = "%Y-%m-%dT%H:%M:%S"

    ds = copernicusmarine.open_dataset(
        dataset_id        = DATASET,
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
    print("    → Positions sargasses (ERDDAP AFAI stride=3)…")
    lons_seed, lats_seed, afai_date = _fetch_afai_positions(stride=3)
    n_particles = len(lons_seed)

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
    # Les datasets NRT ont ~1-2 jours de délai : on prend les 3 derniers jours
    start_dt = end_dt - timedelta(days=3)
    sim_end  = end_dt + timedelta(days=5)
    fmt      = "%Y-%m-%dT%H:%M:%S"

    tmpdir = Path(tempfile.mkdtemp(prefix="sarg_drift_"))
    try:
        # DUACS géostrophique → NetCDF temporaire
        print("    → Téléchargement DUACS (courants géostrophiques)…")
        ds_duacs = copernicusmarine.open_dataset(
            dataset_id        = "cmems_obs-sl_glo_phy-ssh_nrt_allsat-l4-duacs-0.125deg_P1D",
            username          = cp_user, password = cp_pass,
            variables         = ["ugos", "vgos"],
            minimum_latitude  = CARIB["lat_min"] - 3,
            maximum_latitude  = CARIB["lat_max"] + 3,
            minimum_longitude = CARIB["lon_min"] - 3,
            maximum_longitude = CARIB["lon_max"] + 3,
            start_datetime    = (end_dt - timedelta(days=2)).strftime(fmt),
            end_datetime      = end_dt.strftime(fmt),
        )
        # Renommage → noms CF attendus par OpenDrift
        ds_duacs = ds_duacs.rename({
            "ugos": "x_sea_water_velocity",
            "vgos": "y_sea_water_velocity",
        })
        duacs_nc = tmpdir / "duacs.nc"
        ds_duacs.to_netcdf(duacs_nc)
        ds_duacs.close()

        # Copernicus courants totaux → NetCDF temporaire
        print("    → Téléchargement Copernicus (courants totaux)…")
        ds_cop = copernicusmarine.open_dataset(
            dataset_id        = "cmems_obs-mob_glo_phy-cur_nrt_0.25deg_PT1H-i",
            username          = cp_user, password = cp_pass,
            variables         = ["uo", "vo"],
            minimum_latitude  = CARIB["lat_min"] - 3,
            maximum_latitude  = CARIB["lat_max"] + 3,
            minimum_longitude = CARIB["lon_min"] - 3,
            maximum_longitude = CARIB["lon_max"] + 3,
            start_datetime    = start_dt.strftime(fmt),
            end_datetime      = end_dt.strftime(fmt),
        )
        ds_cop = ds_cop.rename({
            "uo": "x_sea_water_velocity",
            "vo": "y_sea_water_velocity",
        })
        cop_nc = tmpdir / "copernicus.nc"
        ds_cop.to_netcdf(cop_nc)
        ds_cop.close()

        # ── 3. Simulation OpenDrift ───────────────────────────────────────────
        print("    → Initialisation OpenDrift…")
        reader_duacs = NCReader(str(duacs_nc))
        reader_cop   = NCReader(str(cop_nc))

        od = OceanDrift(loglevel=50)      # silent
        od.add_reader([reader_duacs, reader_cop])
        od.set_config("general:use_auto_landmask", True)

        sim_start_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        od.seed_elements(lon=lons_seed, lat=lats_seed, time=sim_start_naive)

        sim_out = tmpdir / "output.nc"
        print(f"    → Simulation ({n_particles} ptcl × 5 jours × pas 3h)…")
        od.run(
            duration  = timedelta(days=5),
            time_step = timedelta(hours=3),
            outfile   = str(sim_out),
        )

        # ── 4. Extraction et stockage des snapshots journaliers ───────────────
        sim_ds = xr.open_dataset(sim_out)
        times  = sim_ds["time"].values
        # time_step=3h → 8 indices par jour
        steps_per_day = 8

        simulated_at = now_utc()
        sim_start_str = sim_start_naive.strftime("%Y-%m-%dT%H:%M:%S")
        sim_end_str   = (sim_start_naive + timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%S")

        for day in range(6):    # j+0 à j+5
            t_idx = min(day * steps_per_day, len(times) - 1)
            lo_t  = sim_ds["lon"].isel(time=t_idx).values
            la_t  = sim_ds["lat"].isel(time=t_idx).values
            active = ~np.isnan(lo_t)
            n_act  = int(active.sum())
            act_frac = n_act / n_particles if n_particles > 0 else 0.0

            # Stocker max 500 positions (sous-échantillonner si nécessaire)
            lo_act, la_act = lo_t[active], la_t[active]
            if n_act > 500:
                idx = np.round(np.linspace(0, n_act - 1, 500)).astype(int)
                lo_act, la_act = lo_act[idx], la_act[idx]

            positions = [
                [round(float(lo), 4), round(float(la), 4)]
                for lo, la in zip(lo_act, la_act)
            ]

            conn.execute(
                """INSERT INTO drift_predictions
                   (simulated_at, sim_start, sim_end, n_particles, current_source,
                    day_offset, lon_min, lon_max, lat_min, lat_max,
                    active_fraction, positions_json, raw_metadata)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    simulated_at, sim_start_str, sim_end_str,
                    n_particles, "DUACS+Copernicus", day,
                    round(float(np.nanmin(lo_act)), 3) if n_act else None,
                    round(float(np.nanmax(lo_act)), 3) if n_act else None,
                    round(float(np.nanmin(la_act)), 3) if n_act else None,
                    round(float(np.nanmax(la_act)), 3) if n_act else None,
                    round(act_frac, 4),
                    json.dumps(positions),
                    json.dumps({"day": day, "n_active": n_act, "afai_date": afai_date}),
                ),
            )
            conn.commit()

        sim_ds.close()
        print(f"  ✅ DRIFT SIM       | t0={sim_start_str} | particules={n_particles} "
              f"| 6 snapshots (j+0…j+5) stockés")

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
        try:
            simulate_drift(conn)
        except Exception:
            traceback.print_exc()
        finally:
            conn.close()
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
