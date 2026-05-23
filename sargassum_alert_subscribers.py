#!/usr/bin/env python3
"""
sargassum_alert_subscribers.py
==============================
Envoie des alertes Telegram PERSONNALISEES aux abonnes du bot.

Pour chaque utilisateur ayant souscrit a une ou plusieurs plages via
@Sargasum_alerte_bot, envoie un message uniquement si l'une de ses
plages est en risque medium ou high sur J+0 a J+2.

Complete sargassum_alert.py qui envoie le bulletin groupe au chat principal.

Anti-spam : un meme set d'alertes par utilisateur n'est envoye qu'une fois
toutes les 12h (sauf si la situation change).
"""

import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Config ─────────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "sargassum_data.db"
ENV_PATH = Path(__file__).parent / ".env"
ISLAND = "Saint-Barth"
ALERT_MIN_RISK = "medium"
ALERT_DAYS = [0, 1, 2]
ALERT_HOUR_UTC = 6
ALERT_TOLERANCE = 1
COOLDOWN_HOURS = 12
WEB_URL = "https://sargassum.villasuite.app"

RISK_RANK = {"none": 0, "low": 1, "medium": 2, "high": 3}
RISK_EMOJI = {"none": "🟢", "low": "🟡", "medium": "🟠", "high": "🔴"}
RISK_FR = {"none": "aucun", "low": "faible", "medium": "moyen", "high": "fort"}


# ── Env loader ─────────────────────────────────────────────────────────────────

def _load_env(p: Path) -> dict:
    env = {}
    if not p.exists():
        return env
    for line in p.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env


_env = _load_env(ENV_PATH)
TOKEN = os.environ.get("TELEGRAM_TOKEN") or _env.get("TELEGRAM_TOKEN", "")

if not TOKEN:
    print("[ERROR] TELEGRAM_TOKEN manquant")
    sys.exit(1)


# ── DB ─────────────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Table d'etat anti-spam par utilisateur
    conn.execute("""
        CREATE TABLE IF NOT EXISTS subscriber_alert_state (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id      INTEGER NOT NULL,
            sent_at      TEXT NOT NULL,
            payload_hash TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def get_last_state(conn, chat_id: int) -> tuple[str | None, str | None]:
    row = conn.execute(
        "SELECT sent_at, payload_hash FROM subscriber_alert_state WHERE chat_id=? ORDER BY id DESC LIMIT 1",
        (chat_id,),
    ).fetchone()
    if not row:
        return None, None
    return row["sent_at"], row["payload_hash"]


def save_state(conn, chat_id: int, payload_hash: str):
    conn.execute(
        "INSERT INTO subscriber_alert_state (chat_id, sent_at, payload_hash) VALUES (?, ?, ?)",
        (chat_id, datetime.now(timezone.utc).isoformat(), payload_hash),
    )
    conn.commit()


def load_subscriptions(conn) -> dict[int, list[tuple[str, str]]]:
    """Retourne {chat_id: [(beach_name, user_name), ...]}"""
    rows = conn.execute("""
        SELECT chat_id, user_name, beach_name
        FROM telegram_subscriptions
        ORDER BY chat_id, beach_name
    """).fetchall()
    subs = {}
    for r in rows:
        subs.setdefault(r["chat_id"], []).append((r["beach_name"], r["user_name"] or "ami"))
    return subs


def load_beach_worst(conn, beach: str) -> dict | None:
    """Retourne le pire risque pour une plage sur J+0 a J+2."""
    placeholders = ",".join("?" * len(ALERT_DAYS))
    rows = conn.execute(
        f"""
        SELECT day_offset, risk_level, ROUND(regional_score, 1) as score,
               ROUND(closest_km, 1) as closest_km
        FROM beach_risk_scores
        WHERE island = ? AND beach_name = ? AND day_offset IN ({placeholders})
          AND computed_at = (SELECT MAX(computed_at) FROM beach_risk_scores WHERE island = ?)
        """,
        (ISLAND, beach, *ALERT_DAYS, ISLAND),
    ).fetchall()
    if not rows:
        return None
    # Garde le pire
    worst = max(rows, key=lambda r: RISK_RANK.get(r["risk_level"], 0))
    return dict(worst)


# ── Telegram ───────────────────────────────────────────────────────────────────

def send(chat_id: int, text: str) -> bool:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        return r.ok
    except Exception as e:
        print(f"[ERROR] sendMessage chat_id={chat_id}: {e}")
        return False


def build_personal_message(user_name: str, alerted: list[tuple[str, dict]]) -> str:
    """Compose le message pour un abonne avec uniquement ses plages en alerte."""
    lines = [
        f"🌊 <b>Alerte sargasses</b>",
        f"Hello {user_name}, voici l'état de tes plages :\n",
    ]
    for beach, info in alerted:
        emoji = RISK_EMOJI[info["risk_level"]]
        day = f"J+{info['day_offset']}"
        prox = f"{info['closest_km']:.0f} km" if info["closest_km"] else "—"
        beach_pretty = beach.replace("_", " ")
        lines.append(
            f"  {emoji} <b>{beach_pretty}</b> — risque {RISK_FR[info['risk_level']]} "
            f"({day}, à {prox})"
        )
    lines.append(f"\n🗺 Carte : {WEB_URL}")
    lines.append("ℹ️ Utilise /unsubscribe pour modifier tes abonnements")
    return "\n".join(lines)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    force = "--force" in sys.argv
    test = "--test" in sys.argv
    dry = "--dry" in sys.argv

    if test:
        conn = get_conn()
        subs = load_subscriptions(conn)
        print(f"Test : {len(subs)} abonne(s) trouve(s)")
        for chat_id, beaches in subs.items():
            print(f"  chat_id={chat_id} → {[b[0] for b in beaches]}")
            if not dry:
                send(chat_id, "🧪 <b>Test alerte personnalisée</b>\n\nTu reçois bien les notifications.")
        conn.close()
        return

    # Verifie la fenetre d'envoi (sauf si --force)
    if not force:
        now_hour = datetime.now(timezone.utc).hour
        diff = min(abs(now_hour - ALERT_HOUR_UTC), 24 - abs(now_hour - ALERT_HOUR_UTC))
        if diff > ALERT_TOLERANCE:
            print(f"[INFO] Hors fenêtre d'envoi ({now_hour}h UTC) — abandon")
            return

    conn = get_conn()
    subscriptions = load_subscriptions(conn)

    if not subscriptions:
        print("[INFO] Aucun abonné — rien à envoyer")
        conn.close()
        return

    print(f"[INFO] {len(subscriptions)} abonné(s) à traiter")
    sent_count = 0
    skipped_count = 0

    min_rank = RISK_RANK[ALERT_MIN_RISK]

    for chat_id, beach_list in subscriptions.items():
        user_name = beach_list[0][1] if beach_list else "ami"

        # Recupere le pire risque pour chaque plage abonnee
        alerted = []
        for beach_name, _ in beach_list:
            info = load_beach_worst(conn, beach_name)
            if not info:
                continue
            if RISK_RANK.get(info["risk_level"], 0) >= min_rank:
                alerted.append((beach_name, info))

        # Pas d'alerte pour cet utilisateur
        if not alerted:
            skipped_count += 1
            continue

        # Anti-spam : hash du set d'alertes pour cet utilisateur
        payload = json.dumps(
            [(b, info["risk_level"], info["day_offset"]) for b, info in alerted],
            sort_keys=True,
        )
        payload_hash = hashlib.md5(payload.encode()).hexdigest()

        last_sent, last_hash = get_last_state(conn, chat_id)
        if not force and payload_hash == last_hash and last_sent:
            try:
                last_dt = datetime.fromisoformat(last_sent.replace('Z', '+00:00'))
                hours_since = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
                if hours_since < COOLDOWN_HOURS:
                    print(f"  [skip] chat_id={chat_id} : meme alertes il y a {hours_since:.1f}h")
                    skipped_count += 1
                    continue
            except (ValueError, TypeError):
                pass

        # Envoi
        msg = build_personal_message(user_name, alerted)
        if dry:
            print(f"  [dry] chat_id={chat_id} ({user_name}) :")
            print(msg)
            print()
        else:
            if send(chat_id, msg):
                save_state(conn, chat_id, payload_hash)
                sent_count += 1
                print(f"  [sent] chat_id={chat_id} ({user_name}) : {len(alerted)} plage(s)")
            else:
                print(f"  [fail] chat_id={chat_id}")

    print(f"\n[OK] {sent_count} alertes envoyées, {skipped_count} ignorées")
    conn.close()


if __name__ == "__main__":
    main()
