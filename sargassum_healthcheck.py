#!/usr/bin/env python3
"""
sargassum_healthcheck.py
========================
Watchdog horaire du systeme sargassum.

Verifie 4 indicateurs cles et envoie une alerte Telegram si l'un d'eux
est defaillant. Anti-spam : un meme type d'alerte n'est envoye qu'une fois
toutes les 6 heures (jusqu'a resolution).

Verifications
-------------
  1. Dernier run cron  : copernicus_currents.collected_at < 8h
  2. Dernier scoring   : beach_risk_scores.computed_at < 8h
  3. Derniere drift sim : drift_predictions.simulated_at < 24h
  4. Dashboard Streamlit : HTTP GET localhost:8501/_stcore/health

Usage
-----
  python sargassum_healthcheck.py             # verification standard
  python sargassum_healthcheck.py --test      # envoie un message de test
  python sargassum_healthcheck.py --status    # affiche l'etat sans alerter
  python sargassum_healthcheck.py --force     # alerte meme si rien n'a change

Cron suggere
------------
  30 * * * * cd /opt/sargassum && venv/bin/python3 sargassum_healthcheck.py >> logs/healthcheck.log 2>&1
"""

import hashlib
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


# ── Configuration ──────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "sargassum_data.db"
ENV_PATH = Path(__file__).parent / ".env"

# Seuils de fraicheur (en heures) avant declenchement d'alerte
THRESHOLDS = {
    'last_cron': 12,    # cron tourne toutes les 6h, marge 6h pour latence Copernicus
    'last_scoring':   8,    # beaches.py tourne dans le cron
    'last_drift':     24,   # drift peut echouer 1 fois sans paniquer
    'last_news':      48,   # scraper tourne 1x/jour
}

# URL du dashboard a verifier
DASHBOARD_URL = "http://localhost:8501/_stcore/health"

# Anti-spam : delai minimum entre 2 alertes du meme type (heures)
ALERT_COOLDOWN_H = 6


# ── Chargement .env ────────────────────────────────────────────────────────────

def _load_env(path: Path) -> dict:
    env = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env


_env = _load_env(ENV_PATH)


def _get(key: str, default: str = "") -> str:
    return os.environ.get(key) or _env.get(key) or default


TELEGRAM_TOKEN = _get("TELEGRAM_TOKEN")
TELEGRAM_CHAT = _get("TELEGRAM_CHAT")


# ── Initialisation de la table d'etat ──────────────────────────────────────────

def init_db() -> sqlite3.Connection:
    """Cree la table healthcheck_state si elle n'existe pas."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS healthcheck_state (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            checked_at   TEXT NOT NULL,
            sent_at      TEXT,
            status       TEXT NOT NULL,     -- 'ok' ou 'fail'
            failures     TEXT,              -- JSON des checks echoues
            payload_hash TEXT
        )
    """)
    conn.commit()
    return conn


# ── Verifications ──────────────────────────────────────────────────────────────

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _hours_since(iso_str: str) -> float:
    """Convertit un timestamp ISO en heures ecoulees depuis maintenant."""
    if not iso_str:
        return float('inf')
    dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = now_utc() - dt
    return delta.total_seconds() / 3600


def check_last_cron(conn: sqlite3.Connection) -> tuple[bool, str]:
    """Verifie que le cron a tourne recemment."""
    cur = conn.execute("SELECT MAX(collected_at) FROM copernicus_currents")
    last = cur.fetchone()[0]
    if not last:
        return False, "Aucune donnee dans copernicus_currents"
    age_h = _hours_since(last)
    threshold = THRESHOLDS['last_cron']
    if age_h > threshold:
        return False, f"Dernier cron il y a {age_h:.1f}h (seuil: {threshold}h)"
    return True, f"OK ({age_h:.1f}h)"


def check_last_scoring(conn: sqlite3.Connection) -> tuple[bool, str]:
    """Verifie que les scores de plages sont a jour."""
    cur = conn.execute("SELECT MAX(computed_at) FROM beach_risk_scores")
    last = cur.fetchone()[0]
    if not last:
        return False, "Aucun score calcule"
    age_h = _hours_since(last)
    threshold = THRESHOLDS['last_scoring']
    if age_h > threshold:
        return False, f"Dernier scoring il y a {age_h:.1f}h (seuil: {threshold}h)"
    return True, f"OK ({age_h:.1f}h)"


def check_last_drift(conn: sqlite3.Connection) -> tuple[bool, str]:
    """Verifie que la simulation OpenDrift a tourne recemment."""
    cur = conn.execute("SELECT MAX(simulated_at) FROM drift_predictions")
    last = cur.fetchone()[0]
    if not last:
        return False, "Aucune simulation drift"
    age_h = _hours_since(last)
    threshold = THRESHOLDS['last_drift']
    if age_h > threshold:
        return False, f"Derniere simulation OpenDrift il y a {age_h:.1f}h (seuil: {threshold}h)"
    return True, f"OK ({age_h:.1f}h)"


def check_dashboard() -> tuple[bool, str]:
    """Verifie que le dashboard Streamlit repond."""
    try:
        resp = requests.get(DASHBOARD_URL, timeout=5)
        if resp.status_code == 200:
            return True, f"OK (HTTP {resp.status_code})"
        return False, f"HTTP {resp.status_code}"
    except requests.RequestException as e:
        return False, f"Inaccessible : {type(e).__name__}"


CHECKS = [
    ('cron',      check_last_cron,    'Pipeline cron (collecte 6h)'),
    ('scoring',   check_last_scoring, 'Scoring plages (beaches.py)'),
    ('drift',     check_last_drift,   'Simulation OpenDrift (5j)'),
    ('dashboard', check_dashboard,    'Dashboard Streamlit :8501'),
]


# ── Telegram ───────────────────────────────────────────────────────────────────

def send_telegram(text: str) -> bool:
    """Envoie un message Telegram."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        print("  [WARN] Telegram non configure")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            'chat_id': TELEGRAM_CHAT,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True,
        }, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        print(f"  [ERROR] Envoi Telegram : {e}")
        return False


def format_alert(failures: list[dict]) -> str:
    """Formate le message d'alerte Telegram."""
    lines = [
        "🚨 <b>SARGASSUM — Alerte systeme</b>",
        "",
        f"{len(failures)} verification(s) en echec :",
        "",
    ]
    for f in failures:
        lines.append(f"  ❌ <b>{f['label']}</b>")
        lines.append(f"     {f['msg']}")
    lines.extend([
        "",
        f"🕐 {now_utc().strftime('%d/%m %H:%M UTC')}",
        f"💻 ssh root@45.55.239.73 → /opt/sargassum/",
    ])
    return "\n".join(lines)


def format_recovery() -> str:
    """Message de retablissement quand tout repasse OK."""
    return (
        "✅ <b>SARGASSUM — Systeme retabli</b>\n\n"
        "Toutes les verifications sont OK.\n\n"
        f"🕐 {now_utc().strftime('%d/%m %H:%M UTC')}"
    )


# ── Anti-spam ──────────────────────────────────────────────────────────────────

def should_alert(conn: sqlite3.Connection, current_hash: str,
                 force: bool = False) -> tuple[bool, str]:
    """Determine si on doit envoyer l'alerte (anti-spam)."""
    if force:
        return True, "force"

    cur = conn.execute("""
        SELECT sent_at, payload_hash, status
        FROM healthcheck_state
        WHERE sent_at IS NOT NULL
        ORDER BY id DESC LIMIT 1
    """)
    last = cur.fetchone()
    if not last:
        return True, "premier envoi"

    last_sent, last_hash, last_status = last

    # Si meme set de failures qu'avant
    if last_hash == current_hash:
        age_h = _hours_since(last_sent)
        if age_h < ALERT_COOLDOWN_H:
            return False, f"meme failures, dernier envoi il y a {age_h:.1f}h"
        return True, f"meme failures mais cooldown depasse ({age_h:.1f}h)"

    return True, "nouvelles failures detectees"


# ── Main ───────────────────────────────────────────────────────────────────────

def run_checks(verbose: bool = True) -> tuple[list[dict], list[dict]]:
    """Execute toutes les verifications et retourne (failures, oks)."""
    conn = init_db()
    failures = []
    oks = []

    for key, func, label in CHECKS:
        # Les checks DB prennent conn, les autres rien
        if func == check_dashboard:
            ok, msg = func()
        else:
            ok, msg = func(conn)

        item = {'key': key, 'label': label, 'msg': msg}
        if ok:
            oks.append(item)
            if verbose:
                print(f"  [OK] {label} : {msg}")
        else:
            failures.append(item)
            if verbose:
                print(f"  [FAIL] {label} : {msg}")

    conn.close()
    return failures, oks


def main():
    args = sys.argv[1:]
    is_test = '--test' in args
    is_status = '--status' in args
    is_force = '--force' in args

    print(f"\n=== HealthCheck Sargassum @ {now_utc().strftime('%Y-%m-%d %H:%M UTC')} ===\n")

    if is_test:
        msg = "🧪 <b>SARGASSUM — Test du healthcheck</b>\n\nLe watchdog est operationnel."
        ok = send_telegram(msg)
        print(f"\nTest envoye : {'OK' if ok else 'ECHEC'}")
        return

    failures, oks = run_checks(verbose=True)

    conn = init_db()
    status = 'fail' if failures else 'ok'
    failures_str = ", ".join(f['key'] for f in failures) if failures else ""
    payload_hash = hashlib.md5(failures_str.encode()).hexdigest()

    if is_status:
        print(f"\nStatut : {status.upper()} ({len(failures)}/{len(CHECKS)} en echec)")
        conn.close()
        return

    # Enregistrement de la verification
    sent_at = None

    if failures:
        should, reason = should_alert(conn, payload_hash, force=is_force)
        print(f"\nAlerte ? {'OUI' if should else 'NON'} ({reason})")
        if should:
            msg = format_alert(failures)
            if send_telegram(msg):
                sent_at = now_utc().isoformat()
                print("  → Telegram envoye")
    else:
        # Tout OK : verifier si on doit envoyer un message de recovery
        cur = conn.execute("""
            SELECT status, payload_hash FROM healthcheck_state
            WHERE sent_at IS NOT NULL ORDER BY id DESC LIMIT 1
        """)
        last = cur.fetchone()
        if last and last[0] == 'fail':
            # Le precedent etait en fail → on annonce le retablissement
            msg = format_recovery()
            if send_telegram(msg):
                sent_at = now_utc().isoformat()
                print("  → Telegram recovery envoye")
        print("\nTout OK")

    conn.execute("""
        INSERT INTO healthcheck_state (checked_at, sent_at, status, failures, payload_hash)
        VALUES (?, ?, ?, ?, ?)
    """, (now_utc().isoformat(), sent_at, status, failures_str, payload_hash))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
