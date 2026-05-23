#!/usr/bin/env python3
"""
sargassum_web.py
================
Vue web publique du suivi sargasses Saint-Barthelemy.

Sert la carte Leaflet sur sargassum.villasuite.app et expose une API
JSON pour les donnees temps reel (lues depuis sargassum_data.db).

Endpoints
---------
  GET  /                  Page d'accueil (carte Leaflet)
  GET  /api/beaches       Liste des plages SBH
  GET  /api/status        Risques actuels par plage (dernier scoring)
  GET  /api/forecast      Previsions J+0 a J+5 par plage
  GET  /api/drift         Positions des particules OpenDrift (dernier sim)
  GET  /api/health        Etat du systeme (healthcheck)
  POST /api/subscribe     Formulaire d'abonnement Telegram (web)
  GET  /dashboard         Lien vers le dashboard Streamlit
"""

import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, render_template, request, redirect

# Permet d'importer beaches_db et sargassum_admin_routes
sys.path.insert(0, str(Path(__file__).parent))

import beaches_db
from sargassum_admin_routes import register_admin_routes

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "sargassum_data.db"
ISLAND = "Saint-Barth"
DASHBOARD_URL = "http://45.55.239.73:8501"

app = Flask(__name__, template_folder=str(Path(__file__).parent / "templates"))

# Enregistre les routes admin et stats
register_admin_routes(app)

# Au demarrage : seed les plages depuis beaches.py si la table est vide
def _seed_beaches_if_empty():
    if not beaches_db.is_table_empty():
        return
    try:
        from beaches import BEACHES
        n = beaches_db.seed_from_hardcoded(BEACHES)
        logger.info("Plages migrees depuis beaches.py vers DB : %d", n)
    except Exception as e:
        logger.error("Echec seed : %s", e)

_seed_beaches_if_empty()


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def risk_to_color(level: str) -> str:
    """Convertit un risk_level en couleur hex."""
    return {
        'none':   '#22c55e',
        'low':    '#eab308',
        'medium': '#f97316',
        'high':   '#ef4444',
    }.get(level, '#9ca3af')


def risk_to_fr(level: str) -> str:
    return {
        'none':   'Aucun',
        'low':    'Faible',
        'medium': 'Moyen',
        'high':   'Fort',
    }.get(level, 'Inconnu')


# ── Routes pages ───────────────────────────────────────────────────────────────

@app.route('/')
def index():
    """Page d'accueil avec la carte Leaflet."""
    return render_template('index.html', dashboard_url=DASHBOARD_URL)


@app.route('/dashboard')
def dashboard():
    """Redirige vers le dashboard Streamlit complet."""
    return redirect(DASHBOARD_URL)


# ── Routes API ─────────────────────────────────────────────────────────────────

@app.route('/api/beaches')
def api_beaches():
    """Liste statique des plages SBH avec coordonnees."""
    conn = get_db()
    cur = conn.execute("""
        SELECT DISTINCT beach_name, beach_lat, beach_lon, radius_km
        FROM beach_risk_scores
        WHERE island = ?
        ORDER BY beach_name
    """, (ISLAND,))
    beaches = [dict(row) for row in cur.fetchall()]
    conn.close()
    return jsonify({'count': len(beaches), 'beaches': beaches})


@app.route('/api/status')
def api_status():
    """Risques actuels par plage (dernier scoring, J+0 a J+5).

    Joint avec beaches_config pour utiliser les coords ACTUELLES (admin)
    et non celles figees au moment du scoring (qui peuvent etre obsoletes).
    """
    conn = get_db()
    cur = conn.execute("""
        SELECT s.beach_name,
               COALESCE(bc.lat, s.beach_lat) AS beach_lat,
               COALESCE(bc.lon, s.beach_lon) AS beach_lon,
               s.day_offset, s.risk_level,
               ROUND(s.regional_score, 1) AS regional_score,
               ROUND(s.closest_km, 1) AS closest_km,
               s.computed_at
        FROM beach_risk_scores s
        LEFT JOIN beaches_config bc
            ON bc.island = s.island AND bc.name = s.beach_name AND bc.active = 1
        WHERE s.island = ?
          AND s.computed_at = (
              SELECT MAX(computed_at) FROM beach_risk_scores WHERE island = ?
          )
        ORDER BY s.beach_name, s.day_offset
    """, (ISLAND, ISLAND))

    # Regroupe par plage
    beaches = {}
    for row in cur.fetchall():
        name = row['beach_name']
        if name not in beaches:
            beaches[name] = {
                'name': name,
                'lat': row['beach_lat'],
                'lon': row['beach_lon'],
                'computed_at': row['computed_at'],
                'forecast': [],
            }
        beaches[name]['forecast'].append({
            'day_offset': row['day_offset'],
            'risk_level': row['risk_level'],
            'color': risk_to_color(row['risk_level']),
            'label': risk_to_fr(row['risk_level']),
            'regional_score': row['regional_score'],
            'closest_km': row['closest_km'],
        })

    # Ajoute le pire risque sur 3 jours
    for beach in beaches.values():
        rank = {'none': 0, 'low': 1, 'medium': 2, 'high': 3}
        worst = max(
            (f['risk_level'] for f in beach['forecast'][:3]),
            key=lambda l: rank.get(l, 0),
        )
        beach['worst_3d'] = worst
        beach['worst_3d_color'] = risk_to_color(worst)
        beach['worst_3d_label'] = risk_to_fr(worst)

    conn.close()
    return jsonify({
        'island': ISLAND,
        'count': len(beaches),
        'beaches': list(beaches.values()),
    })


@app.route('/api/forecast')
def api_forecast():
    """Previsions detaillees J+0 a J+5 (toutes plages SBH)."""
    return api_status()  # alias pour clarte


@app.route('/api/drift')
def api_drift():
    """Positions des particules OpenDrift (dernier sim, par day_offset)."""
    day_offset = request.args.get('day', '0')
    try:
        day_offset = int(day_offset)
    except ValueError:
        day_offset = 0

    conn = get_db()
    cur = conn.execute("""
        SELECT positions_json, n_particles, active_fraction, simulated_at,
               sim_start, current_source
        FROM drift_predictions
        WHERE day_offset = ?
        ORDER BY id DESC LIMIT 1
    """, (day_offset,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return jsonify({'error': 'No drift data', 'day_offset': day_offset}), 404

    try:
        positions = json.loads(row['positions_json'])
    except (json.JSONDecodeError, TypeError):
        positions = []

    return jsonify({
        'day_offset': day_offset,
        'simulated_at': row['simulated_at'],
        'sim_start': row['sim_start'],
        'source': row['current_source'],
        'n_particles': row['n_particles'],
        'active_fraction': row['active_fraction'],
        'positions': positions,
    })


@app.route('/api/health')
def api_health():
    """Etat du systeme (lit la table healthcheck_state)."""
    conn = get_db()
    cur = conn.execute("""
        SELECT checked_at, status, failures
        FROM healthcheck_state
        ORDER BY id DESC LIMIT 1
    """)
    row = cur.fetchone()
    conn.close()

    if not row:
        return jsonify({'status': 'unknown'})

    return jsonify({
        'status': row['status'],
        'checked_at': row['checked_at'],
        'failures': row['failures'] or '',
    })


@app.route('/api/subscribe', methods=['POST'])
def api_subscribe():
    """Formulaire web pour generer un lien d'abonnement Telegram.

    Le bot @sargassum_sbh_bot fournit la vraie subscription via /subscribe.
    Cette route web genere un deep link Telegram pre-rempli.
    """
    data = request.get_json(silent=True) or request.form
    beach = data.get('beach', '').strip()

    if not beach:
        return jsonify({'error': 'beach requis'}), 400

    # Genere le deep link Telegram
    # Format : https://t.me/<bot_username>?start=subscribe_<beach>
    # L'utilisateur clique, ouvre Telegram, le bot recoit /start subscribe_<beach>
    bot_username = "Sargasum_alerte_bot"
    deep_link = f"https://t.me/{bot_username}?start=subscribe_{beach.replace(' ', '_')}"

    return jsonify({
        'beach': beach,
        'telegram_link': deep_link,
        'message': f'Cliquez sur le lien pour vous abonner aux alertes de {beach} via Telegram',
    })


# ── Lancement ──────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5095, debug=False)
