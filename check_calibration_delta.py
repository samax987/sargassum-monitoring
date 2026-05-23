#!/usr/bin/env python3
"""Comparaison ponctuelle calibration spatiale du 18 mai 2026 vs baseline 11 mai.

Lit la derniere ligne de calibration_spatial_bias, calcule les agregats,
compare au baseline pre-Stokes (partiellement dilue) et envoie un rapport
sur Telegram. Conçu pour un seul tir programme via cron.

A la fin de l'exécution, le script se retire automatiquement du crontab pour
éviter toute relance future.
"""
import os
import sys
import sqlite3
import logging
import subprocess
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen, Request

# Baseline calibration spatiale du 2026-05-11T09:00:06Z (snapshot pre-Stokes dilue)
BASELINE = {
    "computed_at": "2026-05-11T09:00:06Z",
    "avg_dist_km": 41.1,
    "avg_abs_dlon": 20.9,
    "avg_abs_dlat": 13.4,
    "total_obs": 164,
    "n_bins": 50,
}

DB_PATH = Path("/opt/sargassum/sargassum_data.db")
ENV_PATH = Path("/opt/sargassum/.env")
LOG_PATH = Path("/opt/sargassum/logs/check_calibration_delta.log")

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)sZ %(levelname)s %(message)s",
)
log = logging.getLogger("calib_delta")


def load_env():
    """Charge .env minimaliste (KEY=VALUE par ligne)."""
    env = {}
    for line in ENV_PATH.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def fetch_latest_calibration():
    """Recupere agregat du dernier snapshot de calibration_spatial_bias."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        row = conn.execute(
            """
            SELECT computed_at,
                   ROUND(AVG(mean_min_dist_km), 1),
                   ROUND(AVG(ABS(mean_delta_lon_km)), 1),
                   ROUND(AVG(ABS(mean_delta_lat_km)), 1),
                   SUM(n_obs),
                   COUNT(*)
            FROM calibration_spatial_bias
            WHERE computed_at = (SELECT MAX(computed_at) FROM calibration_spatial_bias)
            GROUP BY computed_at
            """
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return {
        "computed_at": row[0],
        "avg_dist_km": row[1],
        "avg_abs_dlon": row[2],
        "avg_abs_dlat": row[3],
        "total_obs": row[4],
        "n_bins": row[5],
    }


def verdict(delta_dist, delta_lon, delta_lat):
    """Classification simple amelioration/degradation/stable."""
    # On considere stable si tous les deltas sont dans +/- 2 km
    if abs(delta_dist) < 2 and abs(delta_lon) < 2 and abs(delta_lat) < 2:
        return "stable"
    # Amelioration = distance ET biais baissent
    score = 0
    if delta_dist < -2: score += 1
    if delta_lon < -2: score += 1
    if delta_lat < -2: score += 1
    if delta_dist > 2: score -= 1
    if delta_lon > 2: score -= 1
    if delta_lat > 2: score -= 1
    if score > 0:
        return "amelioration"
    if score < 0:
        return "degradation"
    return "mixte"


def send_telegram(token, chat_id, text):
    url = (
        f"https://api.telegram.org/bot{token}/sendMessage"
        f"?chat_id={chat_id}&parse_mode=HTML&text={quote(text)}"
    )
    req = Request(url, headers={"User-Agent": "sargassum-calib-check/1.0"})
    with urlopen(req, timeout=30) as resp:
        return resp.status, resp.read().decode()


def remove_from_crontab():
    """Auto-suppression de la ligne cron pour eviter relance."""
    try:
        current = subprocess.check_output(["crontab", "-l"], text=True)
        # On retire toute ligne mentionnant check_calibration_delta.py
        filtered = "\n".join(
            line for line in current.splitlines()
            if "check_calibration_delta.py" not in line
        )
        # crontab attend un newline final
        if filtered and not filtered.endswith("\n"):
            filtered += "\n"
        subprocess.run(
            ["crontab", "-"], input=filtered, text=True, check=True
        )
        log.info("Ligne cron retiree apres execution.")
    except Exception as exc:  # pragma: no cover
        log.error("Echec retrait crontab : %s", exc)


def main():
    env = load_env()
    token = env.get("TELEGRAM_TOKEN")
    chat = env.get("TELEGRAM_CHAT")
    if not token or not chat:
        log.error("TELEGRAM_TOKEN ou TELEGRAM_CHAT absent du .env")
        sys.exit(1)

    latest = fetch_latest_calibration()
    if not latest:
        send_telegram(token, chat, "⚠️ Calibration sargasses 18 mai : aucune donnee en DB.")
        log.error("calibration_spatial_bias vide.")
        sys.exit(1)

    # Si le dernier snapshot est encore celui du baseline, la calib hebdo n a
    # pas tourne — on previent au lieu d annoncer faussement une amelioration.
    if latest["computed_at"] == BASELINE["computed_at"]:
        send_telegram(
            token, chat,
            "⚠️ Calibration sargasses 18 mai : pas de nouveau snapshot "
            "(toujours " + BASELINE["computed_at"] + "). "
            "Verifier le cron sarga_calibration_spatial.py."
        )
        log.warning("Pas de nouveau snapshot par rapport au baseline.")
        remove_from_crontab()
        return

    d_dist = round(latest["avg_dist_km"] - BASELINE["avg_dist_km"], 1)
    d_lon = round(latest["avg_abs_dlon"] - BASELINE["avg_abs_dlon"], 1)
    d_lat = round(latest["avg_abs_dlat"] - BASELINE["avg_abs_dlat"], 1)

    v = verdict(d_dist, d_lon, d_lat)
    icon = {"amelioration": "✅", "degradation": "❌", "stable": "➖", "mixte": "🟡"}[v]

    msg = (
        "📊 <b>Calibration sargasses 18 mai</b>\n"
        f"Snapshot : {latest['computed_at']}\n"
        f"\n<b>Distance moy</b> : {latest['avg_dist_km']} km "
        f"(Δ vs 11 mai : {d_dist:+.1f} km)\n"
        f"<b>Biais lon</b> : {latest['avg_abs_dlon']} km ({d_lon:+.1f})\n"
        f"<b>Biais lat</b> : {latest['avg_abs_dlat']} km ({d_lat:+.1f})\n"
        f"<b>Matches</b> : {latest['total_obs']} obs / {latest['n_bins']} bins\n"
        f"\n{icon} <b>Verdict</b> : {v}\n"
        "\n<i>Point partiellement dilue (contient encore des prevs pre-Stokes "
        "du 5 mai). Prochaine mesure 100% post-Stokes : lundi 1er juin.</i>"
    )

    status, body = send_telegram(token, chat, msg)
    log.info("Telegram %s : %s", status, body[:200])

    # Auto-suppression du cron pour eviter une relance l an prochain
    remove_from_crontab()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Echec inattendu")
        raise
