#!/usr/bin/env python3
"""
sarga_news_scraper.py
=====================
Scrape des sources d'actualite caribeeennes pour extraire des observations
reelles d'echouages de sargasses et les comparer aux predictions OpenDrift.

Objectif : CALIBRATION DES PREDICTIONS
  - Chaque observation matchee avec une plage connue est comparee au
    beach_risk_score predit pour ce jour.
  - Le rapport de calibration permet de savoir si le modele sur-evalue
    ou sous-evalue le risque par ile / saison.

Usage
-----
  python sarga_news_scraper.py              # scrape + stocke + calibre
  python sarga_news_scraper.py --dry-run    # affiche sans ecrire en DB
  python sarga_news_scraper.py --calibrate  # affiche rapport calibration seul
  python sarga_news_scraper.py --verbose    # affiche le detail
  python sarga_news_scraper.py --since 7   # articles des 7 derniers jours (defaut: 30)

Cron recommande
---------------
  0 7 * * * cd /opt/sargassum && venv/bin/python3 sarga_news_scraper.py
"""

import argparse
import hashlib
import json
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz, process

try:
    import dateparser
    HAS_DATEPARSER = True
except ImportError:
    HAS_DATEPARSER = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent / "sargassum_data.db"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SargassumCalibrator/1.0; "
                  "+https://github.com/samax987/sargassum-monitoring)"
}
REQUEST_DELAY = 2.5   # secondes entre requetes
REQUEST_TIMEOUT = 20
MAX_RETRIES = 2

# Mots-cles obligatoires (au moins 1)
KW_SARGASSE = ["sargasse", "sargasses", "algues brunes", "algues pelagiques"]

# Mots-cles de contexte (au moins 1 doit etre present avec les sargasses)
KW_CONTEXT = [
    "echouage", "echouees", "echouee", "arrivee", "arrivees",
    "envahi", "envahit", "envahissent", "couverte", "couvert",
    "ramassage", "nettoyage", "retrait", "plage", "h2s", "odeur",
    "invasion", "quantite", "signale", "signalees", "submergee",
]

# Mapping intensite depuis mots-cles
SEVERITY_HIGH = [
    "massif", "massive", "considerable", "tres importante", "forte",
    "envahi", "submergee", "couverte", "invasion", "saturation"
]
SEVERITY_MEDIUM = [
    "moderee", "modere", "echouage", "arrivee significative",
    "signalees", "presence", "arrivee"
]
SEVERITY_LOW = [
    "quelques", "traces", "faible", "leger", "ponctuel",
    "peu de", "debut"
]
SEVERITY_CLEAR = [
    "nettoye", "nettoyee", "ramasse", "retire", "disparues",
    "fin des", "plus de sargasses"
]

# Noms d'iles et leurs variantes
ISLAND_PATTERNS = {
    "Guadeloupe":    [r"guadeloupe", r"grande.?terre", r"basse.?terre", r"les saintes", r"marie.?galante", r"sainte.?rose", r"sainte.?anne", r"le gosier", r"capesterre"],
    "Martinique":    [r"martinique", r"fort.?de.?france", r"le lamentin", r"sainte.?luce", r"le francois", r"le vauclin", r"le marin", r"les trois.?ilets"],
    "Saint-Barth":   [r"saint.?barth", r"saint.?barthelemy", r"gustavia", r"saint.?jean", r"lorient", r"flamands?"],
    "Saint-Martin":  [r"saint.?martin", r"marigot", r"orient bay", r"grand case"],
    "Marie-Galante": [r"marie.?galante", r"grand.?bourg", r"capesterre.?marie", r"saint.?louis.?marie"],
}

# Communes propres a chaque ile (supplement pour le matching)
_COMMUNE_TO_ISLAND = {
    "le robert": "Martinique",
    "le francois": "Martinique",
    "le marin": "Martinique",
    "le vauclin": "Martinique",
    "le lamentin": "Martinique",
    "fort de france": "Martinique",
    "sainte luce": "Martinique",
    "le precheur": "Martinique",
    "le lorrain": "Martinique",
    "le carbet": "Martinique",
    "saint esprit": "Martinique",
    "riviere salee": "Martinique",
    "ducos": "Martinique",
    "le gosier": "Guadeloupe",
    "petit bourg": "Guadeloupe",
    "baie mahault": "Guadeloupe",
    "capesterre belle eau": "Guadeloupe",
    "trois rivieres": "Guadeloupe",
    "vieux fort": "Guadeloupe",
    "pointe a pitre": "Guadeloupe",
    "anse bertrand": "Guadeloupe",
    "port louis": "Guadeloupe",
    "deshaies": "Guadeloupe",
    "sainte rose": "Guadeloupe",
    "le moule": "Guadeloupe",
    "saint francois": "Guadeloupe",
    "grand bourg": "Marie-Galante",
    "capesterre marie galante": "Marie-Galante",
    "saint louis marie galante": "Marie-Galante",
    "marigot": "Saint-Martin",
    "grand case": "Saint-Martin",
    "gustavia": "Saint-Barth",
    "saint jean saint barth": "Saint-Barth",
}

# Sources RSS (Google News RSS = meilleur acces, agrege tout)
RSS_SOURCES = [
    {
        "name": "Google News — sargasses antilles",
        "url": "https://news.google.com/rss/search?q=sargasses+antilles&hl=fr&gl=FR&ceid=FR:fr",
        "type": "rss",
    },
    {
        "name": "Google News — sargasses martinique guadeloupe",
        "url": "https://news.google.com/rss/search?q=sargasses+martinique+guadeloupe+echouage&hl=fr&gl=FR&ceid=FR:fr",
        "type": "rss",
    },
    {
        "name": "Google News — sargasses saint-barth",
        "url": "https://news.google.com/rss/search?q=sargasses+saint-barthelemy&hl=fr&gl=FR&ceid=FR:fr",
        "type": "rss",
    },
    {
        "name": "RCI.fm RSS",
        "url": "https://www.rci.fm/feed/",
        "type": "rss",
    },
]

# Sources HTML directes (scraping simple)
HTML_SOURCES = [
    {
        "name": "France-Antilles Guadeloupe",
        "url": "https://www.franceantilles.fr/guadeloupe/",
        "type": "html",
        "search_url": "https://www.franceantilles.fr/guadeloupe/recherche/?q=sargasses",
        "article_selector": "article a, .article-title a, h2 a, h3 a",
        "text_selector": "article p, .article-content p, .entry-content p",
    },
    {
        "name": "France-Antilles Martinique",
        "url": "https://www.franceantilles.fr/martinique/",
        "type": "html",
        "search_url": "https://www.franceantilles.fr/martinique/recherche/?q=sargasses",
        "article_selector": "article a, .article-title a, h2 a, h3 a",
        "text_selector": "article p, .article-content p, .entry-content p",
    },
]


# ---------------------------------------------------------------------------
# Normalisation texte
# ---------------------------------------------------------------------------

def _strip_accents(text: str) -> str:
    import unicodedata
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def normalize(text: str) -> str:
    """Normalise pour comparaison : minuscules, sans accents, sans ponctuation."""
    t = text.lower()
    t = _strip_accents(t)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    # Retirer prefixes geographiques
    for prefix in ["plage de la ", "plage de l ", "plage du ", "plage de ",
                   "plage ", "anse de l ", "anse de ", "anse du ", "anse ",
                   "baie de ", "baie du ", "pointe de "]:
        if t.startswith(prefix):
            t = t[len(prefix):]
    return t


# ---------------------------------------------------------------------------
# Construction de l'index de matching plages
# ---------------------------------------------------------------------------

def _build_beach_index() -> list[dict]:
    """
    Cree un index de toutes les plages connues (depuis beaches.py)
    avec des variantes de noms normalisees.
    """
    # Import dynamique pour eviter les dependances circulaires
    sys.path.insert(0, str(Path(__file__).parent))
    from beaches import BEACHES

    index = []
    for b in BEACHES:
        raw_name = b["name"].replace("_", " ")
        norm = normalize(raw_name)
        index.append({
            "island": b["island"],
            "beach_name": b["name"],
            "display_name": raw_name,
            "normalized": norm,
            "lat": b["lat"],
            "lon": b["lon"],
        })
    return index


BEACH_INDEX = _build_beach_index()
_NORM_NAMES = [b["normalized"] for b in BEACH_INDEX]


def match_beach(text: str, island_hint: Optional[str] = None,
                threshold: int = 72) -> tuple[Optional[dict], float]:
    """
    Tente de matcher un texte libre avec une plage connue.
    Retourne (beach_dict, score) ou (None, 0.0).

    Logique :
      1. Extraire les tokens qui ressemblent a un nom de lieu (> 4 lettres)
      2. Fuzzy match sur le nom normalise
      3. Si island_hint, booster les plages de cette ile
    """
    # Extraire candidats depuis le texte
    words = re.findall(r"[a-zA-ZÀ-ÿ\s\-]{4,}", normalize(text))
    candidates = [w.strip() for w in words if len(w.strip()) > 3]

    best_beach = None
    best_score = 0.0

    for candidate in candidates:
        if len(candidate) < 4:
            continue
        results = process.extract(candidate, _NORM_NAMES,
                                  scorer=fuzz.token_set_ratio,
                                  limit=3)
        for norm_name, score, idx in results:
            beach = BEACH_INDEX[idx]
            # Booster si l'ile correspond
            boosted = score
            if island_hint and beach["island"] == island_hint:
                boosted = min(100, score + 8)
            if boosted > best_score:
                best_score = boosted
                best_beach = beach

    if best_score >= threshold:
        return best_beach, round(best_score / 100.0, 3)
    return None, 0.0


# ---------------------------------------------------------------------------
# Extraction ile depuis texte
# ---------------------------------------------------------------------------

def extract_island(text: str) -> Optional[str]:
    """Detecte l'ile mentionnee dans un texte (patterns + communes)."""
    t_norm = _strip_accents(text.lower())

    # 1. Patterns directs (noms d'iles)
    for island, patterns in ISLAND_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, t_norm):
                return island

    # 2. Communes specifiques (evite les faux positifs ex: "saint-francois" en France)
    for commune, island in _COMMUNE_TO_ISLAND.items():
        if commune in t_norm:
            return island

    return None


# ---------------------------------------------------------------------------
# Extraction severite
# ---------------------------------------------------------------------------

def extract_severity(text: str) -> str:
    """
    Derive le niveau d'echouage depuis le texte de l'article.
    Retourne : none / low / medium / high
    """
    t = _strip_accents(text.lower())

    # Chercher contexte negatif (situation resolue)
    for kw in SEVERITY_CLEAR:
        if kw in t:
            return "none"

    # Score par categorie
    score = {"high": 0, "medium": 0, "low": 0}
    for kw in SEVERITY_HIGH:
        if kw in t:
            score["high"] += 1
    for kw in SEVERITY_MEDIUM:
        if kw in t:
            score["medium"] += 1
    for kw in SEVERITY_LOW:
        if kw in t:
            score["low"] += 1

    if score["high"] >= 1:
        return "high"
    if score["medium"] >= 1:
        return "medium"
    if score["low"] >= 1:
        return "low"

    # Par defaut si sargasses mentionnees sans contexte de quantite
    return "medium"


# ---------------------------------------------------------------------------
# Extraction date
# ---------------------------------------------------------------------------

def extract_date(text: str, fallback: Optional[datetime] = None) -> Optional[str]:
    """Extrait la date de l'evenement depuis le texte."""
    if HAS_DATEPARSER:
        settings = {
            "PREFER_DAY_OF_MONTH": "first",
            "RETURN_AS_TIMEZONE_AWARE": False,
            "PREFER_LOCALE_DATE_ORDER": True,
        }
        parsed = dateparser.parse(text, settings=settings)
        if parsed:
            return parsed.strftime("%Y-%m-%d")

    # Regex simples comme fallback
    patterns = [
        r"(\d{1,2})\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|"
        r"septembre|octobre|novembre|decembre)\s+(\d{4})",
        r"(\d{4})-(\d{2})-(\d{2})",
        r"(\d{2})/(\d{2})/(\d{4})",
    ]
    for pat in patterns:
        m = re.search(pat, _strip_accents(text.lower()))
        if m:
            try:
                from dateutil import parser as duparser
                return duparser.parse(m.group(0)).strftime("%Y-%m-%d")
            except Exception:
                pass

    if fallback:
        return fallback.strftime("%Y-%m-%d")
    return None


# ---------------------------------------------------------------------------
# Test si article parle de sargasses avec contexte d'echouage
# ---------------------------------------------------------------------------

def is_relevant(text: str) -> bool:
    """Filtre : le texte doit parler de sargasses en contexte d'echouage."""
    t = _strip_accents(text.lower())
    has_sargasse = any(kw in t for kw in KW_SARGASSE)
    has_context = any(kw in t for kw in KW_CONTEXT)
    return has_sargasse and has_context


# ---------------------------------------------------------------------------
# HTTP utils
# ---------------------------------------------------------------------------

def _get(url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.ok:
                return r
            if r.status_code in (403, 429):
                break  # Pas de retry si bloque
        except requests.RequestException:
            pass
        if attempt < retries:
            time.sleep(REQUEST_DELAY * (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# Parsers de sources
# ---------------------------------------------------------------------------

def parse_rss(source: dict, since_days: int = 30) -> list[dict]:
    """Parse un flux RSS et retourne les articles pertinents."""
    items = []
    since_dt = datetime.now() - timedelta(days=since_days)

    try:
        feed = feedparser.parse(source["url"])
    except Exception as e:
        return []

    for entry in feed.entries:
        title = getattr(entry, "title", "")
        summary = getattr(entry, "summary", "")
        link = getattr(entry, "link", "")
        pub = getattr(entry, "published_parsed", None)

        if pub:
            pub_dt = datetime(*pub[:6])
            if pub_dt < since_dt:
                continue
            pub_str = pub_dt.strftime("%Y-%m-%d")
        else:
            pub_str = None

        full_text = f"{title} {summary}"
        if not is_relevant(full_text):
            continue

        items.append({
            "source_name": source["name"],
            "source_url": link,
            "pub_date": pub_str,
            "headline": title,
            "snippet": summary[:500] if summary else title,
            "full_text": full_text,
        })

        time.sleep(0.1)  # Pause legere entre entrees

    return items


def parse_html_source(source: dict, since_days: int = 30) -> list[dict]:
    """Scrape une source HTML pour trouver des articles sargasses."""
    items = []
    url = source.get("search_url") or source["url"]
    r = _get(url)
    if not r:
        return []

    time.sleep(REQUEST_DELAY)

    soup = BeautifulSoup(r.text, "html.parser")
    links = soup.select(source.get("article_selector", "article a"))

    visited = set()
    for tag in links[:15]:  # Limiter a 15 articles par source
        href = tag.get("href", "")
        if not href:
            continue
        full_url = urljoin(url, href)
        if full_url in visited:
            continue
        visited.add(full_url)

        title = tag.get_text(strip=True)
        if not is_relevant(title):
            continue

        # Charger l'article complet
        time.sleep(REQUEST_DELAY)
        art_r = _get(full_url)
        if not art_r:
            continue

        art_soup = BeautifulSoup(art_r.text, "html.parser")
        paragraphs = art_soup.select(
            source.get("text_selector", "article p, .content p")
        )
        art_text = " ".join(p.get_text(strip=True) for p in paragraphs[:10])

        if not art_text or not is_relevant(art_text):
            continue

        items.append({
            "source_name": source["name"],
            "source_url": full_url,
            "pub_date": None,
            "headline": title,
            "snippet": art_text[:500],
            "full_text": f"{title} {art_text}",
        })

    return items


# ---------------------------------------------------------------------------
# Base de donnees
# ---------------------------------------------------------------------------

_SCHEMA_NEWS = """
CREATE TABLE IF NOT EXISTS news_observations (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    scraped_at       TEXT    NOT NULL,
    source_name      TEXT    NOT NULL,
    source_url       TEXT,
    pub_date         TEXT,
    event_date       TEXT,
    raw_island       TEXT,
    island           TEXT,
    beach_name       TEXT,
    match_score      REAL,
    observed_level   TEXT    NOT NULL,
    headline         TEXT,
    snippet          TEXT,
    content_hash     TEXT    UNIQUE,
    -- Calibration prediction
    predicted_level  TEXT,
    predicted_score  REAL,
    prediction_error TEXT
);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA_NEWS)
    conn.commit()
    return conn


def content_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8", errors="replace")).hexdigest()


def already_seen(conn: sqlite3.Connection, chash: str) -> bool:
    row = conn.execute(
        "SELECT id FROM news_observations WHERE content_hash = ?", (chash,)
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Lookup prediction pour calibration
# ---------------------------------------------------------------------------

def lookup_prediction(conn: sqlite3.Connection, beach_name: str,
                      island: str, event_date: str) -> dict:
    """
    Cherche dans beach_risk_scores la prediction la plus proche
    de event_date pour cette plage.
    """
    if not event_date or not beach_name:
        return {}

    # Convertir event_date en datetime pour chercher le computed_at le + proche
    try:
        target_dt = datetime.strptime(event_date, "%Y-%m-%d")
    except ValueError:
        return {}

    # Chercher les computed_at proches (±7 jours)
    date_min = (target_dt - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    date_max = (target_dt + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = conn.execute("""
        SELECT computed_at, simulated_at, day_offset, risk_level, regional_score
        FROM beach_risk_scores
        WHERE beach_name = ?
          AND island = ?
          AND computed_at BETWEEN ? AND ?
          AND day_offset = 0
        ORDER BY computed_at DESC
        LIMIT 5
    """, (beach_name, island, date_min, date_max)).fetchall()

    if not rows:
        return {}

    # Choisir la prediction dont computed_at + day_offset est le + proche
    best = None
    best_diff = timedelta.max
    for row in rows:
        try:
            comp_dt = datetime.strptime(row["computed_at"][:19], "%Y-%m-%dT%H:%M:%S")
            pred_dt = comp_dt + timedelta(days=row["day_offset"])
            diff = abs(pred_dt - target_dt)
            if diff < best_diff:
                best_diff = diff
                best = row
        except ValueError:
            continue

    if not best:
        return {}

    return {
        "predicted_level": best["risk_level"],
        "predicted_score": best["regional_score"],
    }


def prediction_error(observed: str, predicted: str) -> str:
    """
    Compare observation vs prediction.
    Retourne : correct / over_predicted / under_predicted / missing
    """
    if not predicted:
        return "missing"

    RANK = {"none": 0, "low": 1, "medium": 2, "high": 3}
    obs_r = RANK.get(observed, -1)
    pred_r = RANK.get(predicted, -1)

    if obs_r < 0 or pred_r < 0:
        return "unknown"
    if obs_r == pred_r:
        return "correct"
    if pred_r > obs_r:
        return "over_predicted"
    return "under_predicted"


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run_scraper(since_days: int = 30, dry_run: bool = False,
                verbose: bool = False) -> list[dict]:
    """
    Lance le scraping de toutes les sources, extrait les observations,
    les matche avec les plages connues et les stocke en DB.
    """
    conn = None if dry_run else get_conn()
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    all_new = []

    all_sources = RSS_SOURCES + HTML_SOURCES

    for source in all_sources:
        if verbose:
            print(f"[{scraped_at[:16]}] Scraping {source['name']}...")

        try:
            if source["type"] == "rss":
                raw_items = parse_rss(source, since_days)
            else:
                raw_items = parse_html_source(source, since_days)
        except Exception as e:
            if verbose:
                print(f"  Erreur: {e}")
            raw_items = []

        if verbose:
            print(f"  {len(raw_items)} articles pertinents")

        for item in raw_items:
            text = item["full_text"]
            chash = content_hash(text)

            # Deduplication
            if not dry_run and already_seen(conn, chash):
                continue

            island = extract_island(text)
            severity = extract_severity(text)
            event_date = extract_date(
                item.get("snippet", ""),
                fallback=datetime.now() if not item["pub_date"] else
                         datetime.strptime(item["pub_date"], "%Y-%m-%d")
            )

            # Matching plage
            beach, match_score = match_beach(text, island_hint=island)

            # Calibration
            pred = {}
            if beach and event_date and not dry_run:
                pred = lookup_prediction(conn, beach["beach_name"],
                                        beach["island"], event_date)

            obs = {
                "scraped_at": scraped_at,
                "source_name": item["source_name"],
                "source_url": item.get("source_url"),
                "pub_date": item.get("pub_date"),
                "event_date": event_date,
                "raw_island": island,
                "island": beach["island"] if beach else island,
                "beach_name": beach["beach_name"] if beach else None,
                "match_score": match_score if beach else None,
                "observed_level": severity,
                "headline": item.get("headline", "")[:300],
                "snippet": item.get("snippet", "")[:600],
                "content_hash": chash,
                "predicted_level": pred.get("predicted_level"),
                "predicted_score": pred.get("predicted_score"),
                "prediction_error": prediction_error(
                    severity, pred.get("predicted_level")
                ) if pred else None,
            }

            all_new.append(obs)

            if not dry_run:
                try:
                    conn.execute("""
                        INSERT INTO news_observations
                        (scraped_at, source_name, source_url, pub_date, event_date,
                         raw_island, island, beach_name, match_score, observed_level,
                         headline, snippet, content_hash, predicted_level,
                         predicted_score, prediction_error)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        obs["scraped_at"], obs["source_name"], obs["source_url"],
                        obs["pub_date"], obs["event_date"], obs["raw_island"],
                        obs["island"], obs["beach_name"], obs["match_score"],
                        obs["observed_level"], obs["headline"], obs["snippet"],
                        obs["content_hash"], obs["predicted_level"],
                        obs["predicted_score"], obs["prediction_error"],
                    ))
                    conn.commit()
                except sqlite3.IntegrityError:
                    pass  # Doublon (content_hash unique)

    if conn:
        conn.close()

    return all_new


# ---------------------------------------------------------------------------
# Rapport de calibration
# ---------------------------------------------------------------------------

def calibration_report(verbose: bool = False) -> None:
    """
    Analyse toutes les observations matchees en DB et compare aux predictions.
    Affiche les metriques de calibration par ile et globalement.
    """
    conn = get_conn()

    rows = conn.execute("""
        SELECT island, beach_name, observed_level, predicted_level,
               predicted_score, prediction_error, event_date
        FROM news_observations
        WHERE beach_name IS NOT NULL
          AND prediction_error IS NOT NULL
          AND prediction_error != 'missing'
        ORDER BY island, event_date DESC
    """).fetchall()

    conn.close()

    if not rows:
        print("Aucune observation matchee avec prediction disponible.")
        print("Relancez le scraper pour collecter des donnees.")
        return

    # Metriques globales
    total = len(rows)
    by_error = {"correct": 0, "over_predicted": 0, "under_predicted": 0}
    by_island = {}

    for r in rows:
        err = r["prediction_error"]
        by_error[err] = by_error.get(err, 0) + 1

        isle = r["island"] or "?"
        if isle not in by_island:
            by_island[isle] = {"correct": 0, "over": 0, "under": 0, "total": 0}
        by_island[isle]["total"] += 1
        if err == "correct":
            by_island[isle]["correct"] += 1
        elif err == "over_predicted":
            by_island[isle]["over"] += 1
        elif err == "under_predicted":
            by_island[isle]["under"] += 1

    print("\n" + "=" * 60)
    print("  RAPPORT DE CALIBRATION — Observations vs Predictions")
    print("=" * 60)
    print(f"\n  Total observations matchees : {total}")
    print(f"  Correctes (meme niveau)     : {by_error.get('correct', 0)} "
          f"({100*by_error.get('correct',0)//total}%)")
    print(f"  Sous-estimees (manque)      : {by_error.get('under_predicted', 0)} "
          f"({100*by_error.get('under_predicted',0)//total}%)")
    print(f"  Sur-estimees (fausse alarme): {by_error.get('over_predicted', 0)} "
          f"({100*by_error.get('over_predicted',0)//total}%)")

    print("\n  Par ile :")
    print(f"  {'Ile':<16} {'N':>4} {'Correct':>8} {'Sous':>8} {'Sur':>8}")
    print("  " + "-" * 48)
    for isle, m in sorted(by_island.items()):
        n = m["total"]
        print(f"  {isle:<16} {n:>4} "
              f"{m['correct']:>7}({100*m['correct']//n:2}%) "
              f"{m['under']:>7}({100*m['under']//n:2}%) "
              f"{m['over']:>7}({100*m['over']//n:2}%)")

    if verbose and rows:
        print("\n  Dernières observations :")
        print(f"  {'Plage':<22} {'Date':>10} {'Obs':>8} {'Pred':>8} {'Erreur'}")
        print("  " + "-" * 65)
        for r in list(rows)[:20]:
            print(f"  {(r['beach_name'] or '?')[:22]:<22} "
                  f"{(r['event_date'] or '?'):>10} "
                  f"{r['observed_level']:>8} "
                  f"{(r['predicted_level'] or '?'):>8} "
                  f"  {r['prediction_error']}")

    print()

    # Interpretation
    under = by_error.get("under_predicted", 0)
    over = by_error.get("over_predicted", 0)
    if under > over * 2:
        print("  DIAGNOSTIC : Le modele SOUS-EVALUE le risque.")
        print("  Action : Baisser les seuils RISK_THRESHOLDS dans beaches.py")
        print("           ou augmenter REGIONAL_SIGMA.")
    elif over > under * 2:
        print("  DIAGNOSTIC : Le modele SUR-EVALUE le risque (trop d'alertes).")
        print("  Action : Augmenter les seuils RISK_THRESHOLDS dans beaches.py")
        print("           ou reduire REGIONAL_SIGMA.")
    else:
        print("  DIAGNOSTIC : Calibration acceptable.")

    print()


# ---------------------------------------------------------------------------
# Affichage des resultats
# ---------------------------------------------------------------------------

def print_results(observations: list[dict]) -> None:
    matched = [o for o in observations if o["beach_name"]]
    unmatched = [o for o in observations if not o["beach_name"]]

    print(f"\n  {len(observations)} nouvelles observations dont "
          f"{len(matched)} matchees avec une plage connue\n")

    if matched:
        print("  Observations matchees :")
        for o in matched:
            island = o["island"] or "?"
            beach = o["beach_name"].replace("_", " ")
            date = o.get("event_date") or o.get("pub_date") or "date inconnue"
            level = o["observed_level"]
            pred = o.get("predicted_level") or "?"
            err = o.get("prediction_error") or ""
            score = f"{o['match_score']:.0%}" if o["match_score"] else "?"
            ICONS = {"none": "🟢", "low": "🟡", "medium": "🟠", "high": "🔴"}
            icon = ICONS.get(level, "?")
            err_str = {"correct": "OK", "over_predicted": "SUREVAL",
                       "under_predicted": "SOUSEVAL", "missing": "?"}.get(err, "")
            print(f"  {icon} {beach} ({island}) — {date} — obs={level} "
                  f"pred={pred} [{err_str}] (match={score})")
            print(f"     {o['source_name']}: {o['headline'][:80]}")

    if unmatched:
        print(f"\n  {len(unmatched)} observations sans plage identifiee "
              f"(review manuelle recommandee):")
        for o in unmatched[:5]:
            print(f"  - {o['headline'][:80]} [{o['source_name']}]")

    print()


# ---------------------------------------------------------------------------
# Point d'entree
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="Ne pas ecrire en DB, afficher seulement")
    parser.add_argument("--calibrate", action="store_true",
                        help="Afficher le rapport de calibration (sans scraper)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Affichage detaille")
    parser.add_argument("--since", type=int, default=30, metavar="JOURS",
                        help="Articles des N derniers jours (defaut: 30)")
    args = parser.parse_args()

    if args.calibrate:
        calibration_report(verbose=args.verbose)
        return

    print(f"\n  Scraping sargasses — depuis {args.since} jours"
          + (" [DRY-RUN]" if args.dry_run else ""))
    print("  " + "-" * 50)

    new_obs = run_scraper(
        since_days=args.since,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    print_results(new_obs)

    if new_obs and not args.dry_run:
        print(f"  OK Sauvegarde dans news_observations (sargassum_data.db)")

    if not args.dry_run and not args.calibrate:
        calibration_report(verbose=False)


if __name__ == "__main__":
    main()
