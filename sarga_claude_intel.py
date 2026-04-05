#!/usr/bin/env python3
"""
sarga_claude_intel.py
=====================
Collecteur intelligent de sargasses via Claude Haiku (Anthropic).

Fonctions
---------
  analyze_url(url)      → extrait observations sargasses depuis une page web
  analyze_text(text)    → extrait observations depuis un texte brut
  web_collect()         → parcourt sources prédéfinies, stocke en DB

Usage
-----
  python sarga_claude_intel.py              # collecte web auto
  python sarga_claude_intel.py --dry-run    # affiche sans écrire en DB
  python sarga_claude_intel.py --url URL    # analyse une URL spécifique
  python sarga_claude_intel.py --text       # lit stdin

Cron recommandé
---------------
  0 8 * * * cd /opt/sargassum && venv/bin/python3 sarga_claude_intel.py >> logs/claude_intel.log 2>&1
"""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False
    print("[WARN] Package 'anthropic' non installé.", file=sys.stderr)

# ── Config ────────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "sargassum_data.db"

# Chargement .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-haiku-4-5-20251001"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SargassumIntel/1.0; "
                  "+https://github.com/samax987/sargassum-monitoring)"
}
FETCH_TIMEOUT = 20
MAX_CONTENT_CHARS = 12000   # limite avant envoi à Claude

# Sources web à surveiller chaque jour
WEB_SOURCES = [
    # ── Médias caribéens ──────────────────────────────────────────────────────
    {"name": "RCI Guadeloupe",      "url": "https://www.rci.fm/guadeloupe/infos/environnement"},
    {"name": "RCI Martinique",      "url": "https://www.rci.fm/martinique/infos/environnement"},
    {"name": "France-Antilles Mq",  "url": "https://www.martinique.franceantilles.fr/actualite/environnement/"},
    {"name": "France-Antilles Gpe", "url": "https://www.guadeloupe.franceantilles.fr/actualite/environnement/"},
    {"name": "La1ere Martinique",   "url": "https://la1ere.francetvinfo.fr/martinique/"},
    {"name": "La1ere Guadeloupe",   "url": "https://la1ere.francetvinfo.fr/guadeloupe/"},
    {"name": "Carib Journal",       "url": "https://caribjournal.com/?s=sargassum"},
    {"name": "The Sargassum Monitor","url": "https://www.thesargassummonitor.com/"},
    # ── Sources scientifiques ──────────────────────────────────────────────────
    {"name": "NOAA NESDIS Sargassum","url": "https://coastwatch.noaa.gov/cw_html/sargassum.html"},
    {"name": "USF Sargassum Watch", "url": "https://optics.marine.usf.edu/projects/saws.html"},
]

ISLANDS = ["Martinique", "Guadeloupe", "Saint-Barth", "Saint-Martin", "Marie-Galante"]

# ── Prompt Claude ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Tu es un assistant spécialisé dans la surveillance des sargasses aux Antilles.
Tu analyses des textes (articles, rapports, messages) pour en extraire des observations concrètes d'échouages de sargasses sur des plages.

Règles d'extraction :
- N'extrais que des faits concrets mentionnés dans le texte (pas de suppositions)
- La date doit être dans le texte ou déduite du contexte (pas de date inventée)
- Le niveau de risque : "none"=aucune sargasse, "low"=quelques laisses, "medium"=bande visible, "high"=échouage massif
- Les îles valides : Martinique, Guadeloupe, Saint-Barth, Saint-Martin, Marie-Galante, Les Saintes
- Si aucune observation concrète de plage n'est trouvée, retourne une liste vide

Réponds UNIQUEMENT en JSON valide, sans texte avant ou après.
Format :
{
  "observations": [
    {
      "island": "Martinique",
      "beach_name": "Tartane",
      "event_date": "2025-04-03",
      "risk_level": "high",
      "coverage_pct": 80,
      "description": "Échouage massif signalé, algues de 1m d'épaisseur",
      "confidence": 0.9
    }
  ]
}"""


# ── Fetch page ────────────────────────────────────────────────────────────────

def fetch_text(url: str) -> Optional[str]:
    """Télécharge une URL et retourne le texte brut (sans HTML)."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT, verify=False)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        # Supprime scripts, styles, nav
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        # Normalise espaces
        text = re.sub(r"\s+", " ", text)
        return text[:MAX_CONTENT_CHARS]
    except Exception as e:
        print(f"[FETCH] {url} → {e}", file=sys.stderr)
        return None


# ── Claude Haiku call ─────────────────────────────────────────────────────────

def call_claude(content: str, source_hint: str = "") -> list[dict]:
    """Envoie le contenu à Claude Haiku, retourne la liste d'observations."""
    if not HAS_ANTHROPIC or not ANTHROPIC_API_KEY:
        print("[ERROR] Clé API Anthropic manquante.", file=sys.stderr)
        return []

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    user_msg = f"Source : {source_hint}\n\nContenu à analyser :\n{content}"

    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = message.content[0].text.strip()
        # Extraction JSON robuste
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            return data.get("observations", [])
    except json.JSONDecodeError as e:
        print(f"[CLAUDE] JSON invalide : {e}", file=sys.stderr)
    except Exception as e:
        print(f"[CLAUDE] Erreur API : {e}", file=sys.stderr)
    return []


# ── Public API ────────────────────────────────────────────────────────────────

def analyze_url(url: str) -> list[dict]:
    """Analyse une URL et retourne les observations extraites."""
    text = fetch_text(url)
    if not text:
        return []
    return call_claude(text, source_hint=url)


def analyze_text(text: str, source_hint: str = "saisie manuelle") -> list[dict]:
    """Analyse un texte brut et retourne les observations extraites."""
    trimmed = text[:MAX_CONTENT_CHARS]
    return call_claude(trimmed, source_hint=source_hint)


# ── Stockage DB ───────────────────────────────────────────────────────────────

def _ensure_schema(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS beach_observations (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_at   TEXT NOT NULL,
            island        TEXT NOT NULL,
            beach_name    TEXT NOT NULL,
            observed_risk TEXT NOT NULL,
            coverage_pct  INTEGER,
            notes         TEXT,
            source        TEXT DEFAULT 'claude_web'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS claude_intel_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at      TEXT NOT NULL,
            source_name TEXT,
            source_url  TEXT,
            obs_count   INTEGER DEFAULT 0,
            status      TEXT,
            error       TEXT
        )
    """)
    conn.commit()


def _content_hash(island: str, beach: str, date: str) -> str:
    return hashlib.md5(f"{island}|{beach}|{date}".encode()).hexdigest()


def store_observations(
    observations: list[dict],
    source_name: str,
    source_url: str,
    conn: sqlite3.Connection,
    dry_run: bool = False,
) -> int:
    """Stocke les observations en DB. Retourne le nombre de nouvelles entrées."""
    stored = 0
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for obs in observations:
        island     = obs.get("island", "")
        beach      = obs.get("beach_name", "Inconnue")
        event_date = obs.get("event_date") or now[:10]
        risk       = obs.get("risk_level", "low")
        coverage   = obs.get("coverage_pct")
        desc       = obs.get("description", "")
        confidence = obs.get("confidence", 0.0)

        if not island or risk not in ("none", "low", "medium", "high"):
            continue

        note = f"[{source_name}] {desc} (confiance: {confidence:.0%})"
        observed_at = f"{event_date}T12:00:00"

        if dry_run:
            print(f"  [DRY] {island} / {beach} — {risk} — {event_date} — {desc[:60]}")
            stored += 1
            continue

        try:
            conn.execute(
                """INSERT INTO beach_observations
                   (observed_at, island, beach_name, observed_risk, coverage_pct, notes, source)
                   VALUES (?, ?, ?, ?, ?, ?, 'claude_web')""",
                (observed_at, island, beach, risk,
                 int(coverage) if coverage else None, note),
            )
            conn.commit()
            stored += 1
        except Exception as e:
            print(f"  [DB] Erreur insert : {e}", file=sys.stderr)

    return stored


# ── Collecte web automatique ──────────────────────────────────────────────────

def web_collect(dry_run: bool = False, verbose: bool = False) -> int:
    """Parcourt toutes les sources WEB_SOURCES, analyse avec Claude, stocke en DB."""
    conn = sqlite3.connect(DB_PATH)
    _ensure_schema(conn)
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    total = 0

    print(f"[{run_at}] Collecte Claude Intel — {len(WEB_SOURCES)} sources")

    for src in WEB_SOURCES:
        name = src["name"]
        url  = src["url"]
        print(f"  → {name} ...", end=" ", flush=True)

        text = fetch_text(url)
        if not text:
            print("SKIP (fetch échoué)")
            conn.execute(
                "INSERT INTO claude_intel_log (run_at, source_name, source_url, obs_count, status) VALUES (?,?,?,?,?)",
                (run_at, name, url, 0, "fetch_error")
            )
            conn.commit()
            time.sleep(1)
            continue

        observations = call_claude(text, source_hint=f"{name} ({url})")
        count = store_observations(observations, name, url, conn, dry_run=dry_run)

        if verbose and observations:
            for obs in observations:
                print(f"\n    • {obs.get('island')} / {obs.get('beach_name')} — {obs.get('risk_level')} — {obs.get('event_date')}")

        status = "ok" if count >= 0 else "error"
        conn.execute(
            "INSERT INTO claude_intel_log (run_at, source_name, source_url, obs_count, status) VALUES (?,?,?,?,?)",
            (run_at, name, url, count, status)
        )
        conn.commit()

        print(f"{count} observation(s)")
        total += count
        time.sleep(1.5)  # politesse

    conn.close()
    print(f"\n[TOTAL] {total} observation(s) stockée(s).")
    return total


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    import warnings
    warnings.filterwarnings("ignore")

    parser = argparse.ArgumentParser(description="Collecteur IA sargasses (Claude Haiku)")
    parser.add_argument("--dry-run",  action="store_true", help="Affiche sans écrire en DB")
    parser.add_argument("--verbose",  action="store_true", help="Détail des observations")
    parser.add_argument("--url",      type=str, help="Analyse une URL spécifique")
    parser.add_argument("--text",     action="store_true", help="Lit un texte depuis stdin")
    args = parser.parse_args()

    if args.url:
        print(f"Analyse de {args.url}…")
        obs = analyze_url(args.url)
        if not obs:
            print("Aucune observation trouvée.")
        else:
            for o in obs:
                print(json.dumps(o, ensure_ascii=False, indent=2))
        return

    if args.text:
        print("Colle ton texte puis appuie sur Ctrl+D :")
        text = sys.stdin.read()
        obs = analyze_text(text)
        if not obs:
            print("Aucune observation trouvée.")
        else:
            for o in obs:
                print(json.dumps(o, ensure_ascii=False, indent=2))
        return

    web_collect(dry_run=args.dry_run, verbose=args.verbose)


if __name__ == "__main__":
    main()
