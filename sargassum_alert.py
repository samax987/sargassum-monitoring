#!/usr/bin/env python3
"""
sargassum_alert.py
==================
Envoie une alerte Telegram si des plages atteignent un risque ≥ medium
lors du dernier calcul de scores.

Logique anti-spam : un message n'est envoyé que si le risque a changé
depuis la dernière alerte envoyée (stocké dans alert_state en DB).

Usage
-----
  python sargassum_alert.py            # vérifie et envoie si nécessaire
  python sargassum_alert.py --test     # envoie un message de test
  python sargassum_alert.py --force    # envoie même si rien n'a changé
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Configuration ─────────────────────────────────────────────────────────────

TELEGRAM_TOKEN  = "8567841224:AAHglalKJOd-u2UCYmc7Oc3sQN6KcMhVnAM"
TELEGRAM_CHAT   = "6707653544"
DB_PATH         = Path(__file__).parent / "sargassum_data.db"

# Seuil minimum pour déclencher une alerte (none < low < medium < high)
ALERT_MIN_RISK  = "medium"

# Jours à surveiller (j+0 = aujourd'hui, j+1 = demain)
ALERT_DAY_OFFSETS = [0, 1]

# Île prioritaire : beaches détaillées ligne par ligne (sinon résumé compact)
PRIORITY_ISLAND = "Saint-Barth"

# Fenêtre d'envoi matin (heure UTC) — l'alerte ne part que lors du run de 06h
# pour éviter 4 messages par jour. --force et --test ignorent cette contrainte.
ALERT_HOUR_UTC  = 6   # run de 06:00 UTC
ALERT_TOLERANCE = 1   # ± heures acceptées (couvre les légers décalages cron)

RISK_RANK  = {"none": 0, "low": 1, "medium": 2, "high": 3}
RISK_ICONS = {"none": "🟢", "low": "🟡", "medium": "🟠", "high": "🔴"}
RISK_FR    = {"none": "aucun", "low": "faible", "medium": "moyen", "high": "fort"}


# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_state (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sent_at      TEXT NOT NULL,
            computed_at  TEXT NOT NULL,
            payload_hash TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def last_alert_hash(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT payload_hash FROM alert_state ORDER BY sent_at DESC LIMIT 1"
    ).fetchone()
    return row["payload_hash"] if row else None


def save_alert(conn: sqlite3.Connection, computed_at: str, payload_hash: str):
    conn.execute(
        "INSERT INTO alert_state (sent_at, computed_at, payload_hash) VALUES (?, ?, ?)",
        (datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), computed_at, payload_hash),
    )
    conn.commit()


# ── Scores ────────────────────────────────────────────────────────────────────

def load_all_beaches(conn: sqlite3.Connection) -> list[dict]:
    """
    Retourne tous les scores j+0 et j+1, toutes îles.
    Garde le pire jour par plage.
    """
    placeholders = ",".join("?" * len(ALERT_DAY_OFFSETS))
    rows = conn.execute(
        f"""
        SELECT island, beach_name, day_offset, risk_level, regional_score, closest_km
        FROM beach_risk_scores
        WHERE computed_at = (SELECT MAX(computed_at) FROM beach_risk_scores)
          AND day_offset IN ({placeholders})
        ORDER BY island, beach_name, day_offset
        """,
        ALERT_DAY_OFFSETS,
    ).fetchall()

    worst: dict[tuple, dict] = {}
    for r in rows:
        key = (r["island"], r["beach_name"])
        rank = RISK_RANK.get(r["risk_level"], 0)
        if key not in worst or rank > RISK_RANK.get(worst[key]["risk_level"], 0):
            worst[key] = dict(r)

    return sorted(worst.values(), key=lambda x: (x["island"], -RISK_RANK[x["risk_level"]]))


def computed_at_latest(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT MAX(computed_at) AS c FROM beach_risk_scores"
    ).fetchone()
    return row["c"] if row else None


# ── Message Telegram ──────────────────────────────────────────────────────────

def build_message(all_beaches: list[dict], computed_at: str) -> str:
    """
    Format compact par île.
    Île prioritaire (PRIORITY_ISLAND) : chaque plage ≥ seuil est listée.
    Autres îles : une ligne de résumé (comptage par niveau).
    """
    dt = computed_at[:16].replace("T", " ") + " UTC"
    min_rank = RISK_RANK[ALERT_MIN_RISK]

    by_island: dict[str, list] = {}
    for b in all_beaches:
        by_island.setdefault(b["island"], []).append(b)

    lines = [f"🌊 *Alerte sargasses* — {dt}\n"]

    # Île prioritaire en premier, détaillée
    if PRIORITY_ISLAND in by_island:
        bs = by_island.pop(PRIORITY_ISLAND)
        alert_bs = [b for b in bs if RISK_RANK[b["risk_level"]] >= min_rank]
        if alert_bs:
            lines.append(f"📍 *{PRIORITY_ISLAND}* — à vérifier :")
            for b in alert_bs:
                icon = RISK_ICONS[b["risk_level"]]
                prox = f"{b['closest_km']:.0f} km" if b["closest_km"] else "—"
                name = b["beach_name"].replace("_", " ")
                day  = f"j+{b['day_offset']}"
                lines.append(f"  {icon} {name} ({day}, à {prox})")
        else:
            lines.append(f"📍 *{PRIORITY_ISLAND}* — 🟢 toutes plages OK")
        lines.append("")

    # Autres îles : résumé compact
    for island in sorted(by_island.keys()):
        bs = by_island[island]
        counts = {lvl: 0 for lvl in ("high", "medium", "low", "none")}
        for b in bs:
            counts[b["risk_level"]] = counts.get(b["risk_level"], 0) + 1
        parts = []
        for lvl in ("high", "medium", "low", "none"):
            if counts[lvl]:
                parts.append(f"{RISK_ICONS[lvl]}×{counts[lvl]}")
        lines.append(f"📍 *{island}* — {' '.join(parts)}")

    lines.append("")
    n_alert = sum(1 for b in all_beaches if RISK_RANK[b["risk_level"]] >= min_rank)
    lines.append(f"👉 *{n_alert} plage{'s' if n_alert > 1 else ''} en risque ≥ {ALERT_MIN_RISK}*")
    lines.append("_Dashboard → http://45.55.239.73:8501_")

    return "\n".join(lines)


def build_clear_message(computed_at: str) -> str:
    dt = computed_at[:16].replace("T", " ") + " UTC"
    return (
        f"✅ *Sargasses* — {dt}\n\n"
        "Toutes les plages sont au niveau *faible ou nul*.\n"
        "🟢 Aucune alerte active."
    )


def send_telegram(text: str) -> bool:
    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT, "text": text, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, data=data, timeout=10)
        return r.ok
    except Exception as e:
        print(f"  ❌ Telegram error: {e}")
        return False


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main():
    force = "--force" in sys.argv
    test  = "--test"  in sys.argv

    if test:
        ok = send_telegram(
            "🌊 *Sargassum Bot* — connexion OK\n"
            "Les alertes seront envoyées ici quand une plage passe en risque moyen ou fort."
        )
        print("  ✅ Message test envoyé" if ok else "  ❌ Échec envoi test")
        return

    # Vérifier la fenêtre horaire (sauf --force)
    if not force:
        now_hour = datetime.now(timezone.utc).hour
        if abs(now_hour - ALERT_HOUR_UTC) > ALERT_TOLERANCE and \
           abs(now_hour - ALERT_HOUR_UTC) < (24 - ALERT_TOLERANCE):
            print(f"  ℹ️  Hors fenêtre d'alerte ({now_hour}h UTC, fenêtre : {ALERT_HOUR_UTC}h ±{ALERT_TOLERANCE}h) — pas d'envoi.")
            return

    conn = get_conn()

    computed_at = computed_at_latest(conn)
    if not computed_at:
        print("  ⚠️  Aucun score de plage en base — pas d'alerte.")
        conn.close()
        return

    all_beaches = load_all_beaches(conn)
    min_rank    = RISK_RANK[ALERT_MIN_RISK]
    alert_beaches = [b for b in all_beaches if RISK_RANK[b["risk_level"]] >= min_rank]

    # Hash du contenu pour détecter les changements (sur les plages en alerte seulement)
    import hashlib
    payload = json.dumps(
        [(b["island"], b["beach_name"], b["risk_level"], b["day_offset"]) for b in alert_beaches],
        sort_keys=True,
    )
    payload_hash = hashlib.md5(payload.encode()).hexdigest()

    last_hash = last_alert_hash(conn)

    if not force and payload_hash == last_hash:
        print(f"  ℹ️  Situation inchangée depuis la dernière alerte — pas d'envoi.")
        conn.close()
        return

    if alert_beaches:
        msg = build_message(all_beaches, computed_at)
        label = f"{len(alert_beaches)} plage(s) en risque ≥ {ALERT_MIN_RISK}"
    else:
        msg = build_clear_message(computed_at)
        label = "situation dégagée"

    ok = send_telegram(msg)
    if ok:
        save_alert(conn, computed_at, payload_hash)
        print(f"  ✅ Alerte Telegram envoyée — {label}")
    else:
        print(f"  ❌ Échec envoi Telegram")

    conn.close()


if __name__ == "__main__":
    main()
