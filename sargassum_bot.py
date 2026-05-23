#!/usr/bin/env python3
"""
sargassum_bot.py
================
Bot Telegram pour les abonnements aux alertes sargasses par plage.

Polling long-poll de l'API Telegram (pas de webhook = pas besoin de SSL).
Tourne en boucle infinie via systemd.

Commandes
---------
  /start              Message d'accueil + lien web
  /subscribe <plage>  S'abonner aux alertes d'une plage
  /unsubscribe <plage> Se desabonner
  /mybeaches          Lister mes abonnements
  /status [<plage>]   Etat actuel d'une plage (ou toutes)
  /beaches            Lister les plages disponibles
  /map                Lien vers la carte web
  /help               Aide

Deep link
---------
  /start subscribe_<beach>  Abonne automatiquement (depuis le site web)
"""

import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Configuration ──────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "sargassum_data.db"
ENV_PATH = Path(__file__).parent / ".env"
WEB_URL = "https://sargassum.villasuite.app"
ISLAND = "Saint-Barth"

# Polling
POLL_TIMEOUT = 30  # secondes (long poll)
POLL_LIMIT = 100

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger('sargassum_bot')


# ── Env loader ─────────────────────────────────────────────────────────────────

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
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN') or _env.get('TELEGRAM_TOKEN', '')

if not TELEGRAM_TOKEN:
    log.error("TELEGRAM_TOKEN manquant")
    sys.exit(1)

API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


# ── DB : table des abonnements ─────────────────────────────────────────────────

def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS telegram_subscriptions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id      INTEGER NOT NULL,
            user_name    TEXT,
            beach_name   TEXT NOT NULL,
            subscribed_at TEXT NOT NULL,
            UNIQUE(chat_id, beach_name)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    conn.close()


def get_offset() -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("SELECT value FROM bot_state WHERE key='last_update_id'")
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else 0


def set_offset(offset: int) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO bot_state (key, value) VALUES ('last_update_id', ?)",
        (str(offset),)
    )
    conn.commit()
    conn.close()


def get_beaches() -> list[str]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("""
        SELECT DISTINCT beach_name FROM beach_risk_scores
        WHERE island = ? ORDER BY beach_name
    """, (ISLAND,))
    beaches = [row[0] for row in cur.fetchall()]
    conn.close()
    return beaches


def find_beach(name: str) -> str | None:
    """Trouve une plage (matching flexible : insensible casse, underscores)."""
    name_norm = name.strip().lower().replace(' ', '_').replace('-', '_')
    for beach in get_beaches():
        if beach.lower().replace('-', '_') == name_norm:
            return beach
    # Recherche partielle
    for beach in get_beaches():
        if name_norm in beach.lower().replace('-', '_'):
            return beach
    return None


def subscribe(chat_id: int, user_name: str, beach: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            INSERT INTO telegram_subscriptions (chat_id, user_name, beach_name, subscribed_at)
            VALUES (?, ?, ?, ?)
        """, (chat_id, user_name, beach, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False  # deja abonne
    finally:
        conn.close()


def unsubscribe(chat_id: int, beach: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "DELETE FROM telegram_subscriptions WHERE chat_id=? AND beach_name=?",
        (chat_id, beach)
    )
    conn.commit()
    deleted = cur.rowcount > 0
    conn.close()
    return deleted


def list_subscriptions(chat_id: int) -> list[str]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "SELECT beach_name FROM telegram_subscriptions WHERE chat_id=? ORDER BY beach_name",
        (chat_id,)
    )
    beaches = [row[0] for row in cur.fetchall()]
    conn.close()
    return beaches


def get_beach_status(beach: str) -> dict | None:
    """Recupere le risque actuel d'une plage (J+0 a J+2)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("""
        SELECT day_offset, risk_level, ROUND(regional_score, 1) as score,
               ROUND(closest_km, 1) as closest_km, computed_at
        FROM beach_risk_scores
        WHERE island = ? AND beach_name = ?
          AND computed_at = (
              SELECT MAX(computed_at) FROM beach_risk_scores WHERE island = ?
          )
        ORDER BY day_offset
    """, (ISLAND, beach, ISLAND))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# ── Telegram API ───────────────────────────────────────────────────────────────

def send(chat_id: int, text: str) -> bool:
    try:
        r = requests.post(f"{API}/sendMessage", json={
            'chat_id': chat_id, 'text': text,
            'parse_mode': 'HTML', 'disable_web_page_preview': True,
        }, timeout=10)
        r.raise_for_status()
        return True
    except requests.RequestException as e:
        log.error("Erreur sendMessage : %s", e)
        return False


def get_updates(offset: int):
    try:
        r = requests.get(f"{API}/getUpdates", params={
            'offset': offset,
            'timeout': POLL_TIMEOUT,
            'limit': POLL_LIMIT,
        }, timeout=POLL_TIMEOUT + 10)
        r.raise_for_status()
        return r.json().get('result', [])
    except requests.RequestException as e:
        log.error("Erreur getUpdates : %s", e)
        return []


# ── Commandes ──────────────────────────────────────────────────────────────────

RISK_EMOJI = {'none': '🟢', 'low': '🟡', 'medium': '🟠', 'high': '🔴'}
RISK_FR = {'none': 'aucun', 'low': 'faible', 'medium': 'moyen', 'high': 'fort'}


def cmd_start(chat_id: int, user_name: str, args: str) -> str:
    # Deep link : /start subscribe_<beach>
    if args.startswith('subscribe_'):
        beach_raw = args.replace('subscribe_', '', 1)
        beach = find_beach(beach_raw)
        if beach:
            ok = subscribe(chat_id, user_name, beach)
            extra = (
                f"\n\n✅ Tu es abonne aux alertes de <b>{beach.replace('_', ' ')}</b>."
                if ok else
                f"\n\nℹ️ Tu es deja abonne a <b>{beach.replace('_', ' ')}</b>."
            )
        else:
            extra = f"\n\n⚠️ Plage <code>{beach_raw}</code> introuvable."
    else:
        extra = ""

    return (
        f"🌊 <b>Bot Sargasses Saint-Barth</b>\n\n"
        f"Salut {user_name} ! Je t'envoie des alertes quand des sargasses "
        f"sont prevues sur les plages que tu choisis.\n\n"
        f"<b>Commandes :</b>\n"
        f"/subscribe &lt;plage&gt; — s'abonner\n"
        f"/unsubscribe &lt;plage&gt; — se desabonner\n"
        f"/mybeaches — mes abonnements\n"
        f"/status [plage] — etat actuel\n"
        f"/beaches — liste des plages\n"
        f"/map — voir la carte web\n"
        f"/help — aide\n\n"
        f"🗺 Carte : {WEB_URL}"
        f"{extra}"
    )


def cmd_subscribe(chat_id: int, user_name: str, args: str) -> str:
    if not args:
        return "Usage : <code>/subscribe Flamands</code>\n\nVoir /beaches pour la liste."
    beach = find_beach(args)
    if not beach:
        return f"⚠️ Plage <code>{args}</code> introuvable.\n\nVoir /beaches pour la liste."
    ok = subscribe(chat_id, user_name, beach)
    if ok:
        return f"✅ Abonne aux alertes de <b>{beach.replace('_', ' ')}</b>.\n\nTu recevras un message des qu'un risque moyen ou eleve est detecte."
    return f"ℹ️ Tu es deja abonne a <b>{beach.replace('_', ' ')}</b>."


def cmd_unsubscribe(chat_id: int, args: str) -> str:
    if not args:
        return "Usage : <code>/unsubscribe Flamands</code>"
    beach = find_beach(args)
    if not beach:
        return f"⚠️ Plage <code>{args}</code> introuvable."
    ok = unsubscribe(chat_id, beach)
    if ok:
        return f"✅ Desabonne de <b>{beach.replace('_', ' ')}</b>."
    return f"ℹ️ Tu n'etais pas abonne a <b>{beach.replace('_', ' ')}</b>."


def cmd_mybeaches(chat_id: int) -> str:
    beaches = list_subscriptions(chat_id)
    if not beaches:
        return (
            "Tu n'es abonne a aucune plage.\n\n"
            "Utilise <code>/subscribe &lt;plage&gt;</code> pour t'abonner.\n"
            "Liste : /beaches"
        )
    lines = ["📋 <b>Tes abonnements :</b>\n"]
    for b in beaches:
        lines.append(f"  • {b.replace('_', ' ')}")
    return "\n".join(lines)


def cmd_status(args: str) -> str:
    if args:
        beach = find_beach(args)
        if not beach:
            return f"⚠️ Plage <code>{args}</code> introuvable."
        return _format_beach_status(beach)

    # Status global = pire risque par plage sur 3j
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("""
        SELECT beach_name, MAX(risk_level) as worst_level
        FROM beach_risk_scores
        WHERE island = ? AND day_offset <= 2
          AND computed_at = (
              SELECT MAX(computed_at) FROM beach_risk_scores WHERE island = ?
          )
        GROUP BY beach_name
        ORDER BY beach_name
    """, (ISLAND, ISLAND))
    rows = list(cur.fetchall())
    conn.close()

    if not rows:
        return "Aucune donnee disponible."

    lines = ["🌊 <b>Risque sargasses 3 jours (SBH) :</b>\n"]
    rank = {'none': 0, 'low': 1, 'medium': 2, 'high': 3}
    sorted_rows = sorted(rows, key=lambda r: -rank.get(r['worst_level'], 0))
    for r in sorted_rows:
        emoji = RISK_EMOJI.get(r['worst_level'], '⚪')
        lines.append(f"  {emoji} {r['beach_name'].replace('_', ' ')} : {RISK_FR.get(r['worst_level'], '?')}")
    lines.append(f"\n🗺 Carte : {WEB_URL}")
    return "\n".join(lines)


def _format_beach_status(beach: str) -> str:
    rows = get_beach_status(beach)
    if not rows:
        return f"Aucune donnee pour {beach}."
    lines = [f"🏖 <b>{beach.replace('_', ' ')}</b>\n"]
    for r in rows[:6]:
        emoji = RISK_EMOJI.get(r['risk_level'], '⚪')
        d = r['day_offset']
        lines.append(
            f"  {emoji} J+{d} : {RISK_FR.get(r['risk_level'], '?')} "
            f"(score {r['score']}, particule a {r['closest_km']}km)"
        )
    return "\n".join(lines)


def cmd_beaches() -> str:
    beaches = get_beaches()
    lines = ["🏖 <b>Plages disponibles a Saint-Barth :</b>\n"]
    for b in beaches:
        lines.append(f"  • <code>{b}</code>")
    lines.append(f"\nExemple : <code>/subscribe {beaches[0] if beaches else 'Flamands'}</code>")
    return "\n".join(lines)


def cmd_map() -> str:
    return f"🗺 <b>Carte temps reel :</b>\n{WEB_URL}"


def cmd_help() -> str:
    return cmd_start(0, "ami", "").split("\n\n", 1)[1].rsplit("\n\n", 1)[0]


# ── Dispatcher ─────────────────────────────────────────────────────────────────

def handle_message(msg: dict) -> None:
    chat = msg.get('chat', {})
    chat_id = chat.get('id')
    text = (msg.get('text') or '').strip()
    user_name = (msg.get('from', {}).get('first_name') or 'ami')

    if not chat_id or not text or not text.startswith('/'):
        return

    parts = text.split(maxsplit=1)
    cmd = parts[0].lower().split('@')[0]  # /cmd@botname → /cmd
    args = parts[1].strip() if len(parts) > 1 else ''

    log.info("[%s] %s %s", chat_id, cmd, args)

    if cmd == '/start':
        response = cmd_start(chat_id, user_name, args)
    elif cmd == '/subscribe':
        response = cmd_subscribe(chat_id, user_name, args)
    elif cmd == '/unsubscribe':
        response = cmd_unsubscribe(chat_id, args)
    elif cmd == '/mybeaches':
        response = cmd_mybeaches(chat_id)
    elif cmd == '/status':
        response = cmd_status(args)
    elif cmd == '/beaches':
        response = cmd_beaches()
    elif cmd == '/map':
        response = cmd_map()
    elif cmd == '/help':
        response = cmd_help()
    else:
        response = f"Commande inconnue : <code>{cmd}</code>\n\n/help pour la liste."

    send(chat_id, response)


# ── Main loop ──────────────────────────────────────────────────────────────────

def main():
    init_db()
    log.info("Bot Sargassum SBH demarre")
    offset = get_offset()
    log.info("Offset initial : %d", offset)

    while True:
        try:
            updates = get_updates(offset)
            for update in updates:
                update_id = update['update_id']
                offset = max(offset, update_id + 1)
                if 'message' in update:
                    handle_message(update['message'])
            set_offset(offset)
        except KeyboardInterrupt:
            log.info("Arret demande")
            break
        except Exception as e:
            log.exception("Erreur boucle principale : %s", e)
            time.sleep(5)


if __name__ == '__main__':
    main()
