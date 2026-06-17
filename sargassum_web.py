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
import math
import os
import secrets
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import (
    Flask, abort, g, jsonify, render_template, request, redirect,
    send_from_directory, session,
)
from werkzeug.middleware.proxy_fix import ProxyFix

# Permet d'importer beaches_db, contributors_db et les modules de routes
sys.path.insert(0, str(Path(__file__).parent))

import beaches_db
import contributors_db
import contrib_i18n
from sargassum_admin_routes import register_admin_routes
from sargassum_contributor_routes import register_contributor_routes, PHOTOS_DIR

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "sargassum_data.db"
ISLAND = "Saint-Barth"
DASHBOARD_URL = "http://45.55.239.73:8501"


def _load_env_value(key: str, default: str = "") -> str:
    """Lit une clé depuis .env (lecture manuelle, comme le reste du projet)."""
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip()
    return os.environ.get(key, default)


app = Flask(__name__, template_folder=str(Path(__file__).parent / "templates"))

# Derrière nginx (+ Cloudflare) : restaure la vraie IP/proto/hôte du client,
# nécessaire au rate-limiting par IP du portail contributeurs.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

# Clé de signature des sessions (cookies signés). DOIT être stable et partagée
# entre les workers gunicorn → chargée depuis .env. Sans elle, les sessions ne
# survivraient ni à un redémarrage ni au passage d'un worker à l'autre.
app.secret_key = _load_env_value("FLASK_SECRET_KEY")
if not app.secret_key:
    app.secret_key = secrets.token_hex(32)
    logger.warning(
        "FLASK_SECRET_KEY absente du .env : cle ephemere generee. "
        "Ajoute FLASK_SECRET_KEY dans .env en production."
    )

# Cookies de session sécurisés (site servi exclusivement en HTTPS).
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=True,
    PERMANENT_SESSION_LIFETIME=timedelta(days=30),
    # Plafond des requêtes entrantes (jusqu'à 3 photos de téléphone). Au-delà,
    # Werkzeug lève RequestEntityTooLarge → message propre côté portail.
    # Doit rester cohérent avec client_max_body_size côté nginx.
    MAX_CONTENT_LENGTH=30 * 1024 * 1024,
)

# Enregistre les routes admin et stats
register_admin_routes(app)

# Portail contributeurs (bénévoles) : crée les tables au besoin + routes
contributors_db.init_db()
register_contributor_routes(app)


# ── Sécurité : en-têtes HTTP (défense en profondeur) ────────────────────────────
# La CSP bloque tout script chargé depuis un domaine non autorisé : si un
# <script src="https://domaine-malveillant/..."> était injecté dans la page,
# le navigateur refuserait de l'exécuter. On autorise uniquement nos propres
# ressources + les CDN légitimes réellement utilisés (Leaflet, Google Fonts,
# tuiles carto, API météo Open-Meteo).
@app.after_request
def _set_security_headers(resp):
    # script-src par nonce : seuls nos <script nonce="..."> s'exécutent.
    # Un script injecté (inline OU externe) sans le nonce est bloqué par le
    # navigateur. Le nonce est régénéré à chaque requête (cf. index()).
    nonce = getattr(g, "csp_nonce", None)
    script_src = f"'self' 'nonce-{nonce}'" if nonce else "'self'"
    resp.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        f"script-src {script_src}; "
        "style-src 'self' 'unsafe-inline' https://unpkg.com https://fonts.googleapis.com; "
        "font-src 'self' data: https://fonts.gstatic.com; "
        "img-src 'self' data: https:; "
        "connect-src 'self' https://api.open-meteo.com https://marine-api.open-meteo.com; "
        "object-src 'none'; base-uri 'self'; frame-ancestors 'self'"
    )
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return resp


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
        'watch':  '#f59e0b',   # Surveiller : masse qui approche (pas encore sur la plage)
        'medium': '#f97316',
        'high':   '#ef4444',
    }.get(level, '#9ca3af')


def risk_to_fr(level: str) -> str:
    return {
        'none':   'Aucun',
        'low':    'Faible',
        'watch':  'Surveiller',
        'medium': 'Moyen',
        'high':   'Fort',
    }.get(level, 'Inconnu')


# ── Routes pages ───────────────────────────────────────────────────────────────

@app.route('/')
def index():
    """Page d'accueil avec la carte Leaflet.

    Génère un nonce CSP par requête : seules nos balises <script nonce="..."> sont
    exécutées par le navigateur ; tout script injecté sans ce nonce est bloqué.
    """
    nonce = secrets.token_urlsafe(16)
    g.csp_nonce = nonce
    lang = contrib_i18n.current_lang()
    return render_template('index.html', dashboard_url=DASHBOARD_URL, csp_nonce=nonce,
                           t=contrib_i18n.get_map_strings(lang), lang=lang)


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

    Le payload expose DEUX lectures :
      - risk_level (regional, sigma=50km) : signal d'APPROCHE -> tag "masse au large",
        et c'est ce que consomment alertes Telegram / timeline / calibration.
      - presence (local, sigma=radius) : badge PUBLIC "sur la plage".
    """
    from beaches import presence_label, display_badge
    conn = get_db()
    # Tendance île (approche/éloignement) pour le filet de sécurité du badge
    trend_state = compute_drift_trend(conn).get('state', 'unknown')
    cur = conn.execute("""
        SELECT s.beach_name,
               COALESCE(bc.lat, s.beach_lat) AS beach_lat,
               COALESCE(bc.lon, s.beach_lon) AS beach_lon,
               s.day_offset, s.risk_level,
               ROUND(s.regional_score, 1) AS regional_score,
               ROUND(s.local_score, 2) AS local_score,
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
        presence = presence_label(row['local_score'])
        badge = display_badge(presence, row['risk_level'], trend_state)
        beaches[name]['forecast'].append({
            'day_offset': row['day_offset'],
            # Régional (approche) — conservé pour le tag "masse au large" + alertes/timeline
            'risk_level': row['risk_level'],
            'color': risk_to_color(row['risk_level']),
            'label': risk_to_fr(row['risk_level']),
            'regional_score': row['regional_score'],
            'closest_km': row['closest_km'],
            'local_score': row['local_score'],
            # Présence brute (sur la plage)
            'presence': presence,
            # Badge public FINAL = présence + filet de sécurité (watch si masse qui approche)
            'badge': badge,
            'badge_color': risk_to_color(badge),
            'badge_label': risk_to_fr(badge),
        })

    # Pire sur 3 jours : régional (approche -> tag "masse au large")
    # ET présence (badge public "sur la plage").
    for beach in beaches.values():
        rank = {'none': 0, 'low': 1, 'watch': 2, 'medium': 2, 'high': 3}
        worst = max(
            (f['risk_level'] for f in beach['forecast'][:3]),
            key=lambda l: rank.get(l, 0),
        )
        beach['worst_3d'] = worst
        beach['worst_3d_color'] = risk_to_color(worst)
        beach['worst_3d_label'] = risk_to_fr(worst)
        # Badge public final sur 3 jours (présence + filet de sécurité)
        worst_badge = max(
            (f['badge'] for f in beach['forecast'][:3]),
            key=lambda l: rank.get(l, 0),
        )
        beach['worst_3d_badge'] = worst_badge
        beach['worst_3d_badge_color'] = risk_to_color(worst_badge)
        beach['worst_3d_badge_label'] = risk_to_fr(worst_badge)

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
    """Positions des particules OpenDrift (dernier sim).

    ?day=N   → snapshot journalier j+N (par défaut, rétrocompatible)
    ?hour=H  → snapshot à H heures depuis t0 (résolution 3h : 0,3,…,120)
    """
    conn = get_db()

    hour_arg = request.args.get('hour')
    if hour_arg is not None:
        try:
            hour_offset = int(hour_arg)
        except ValueError:
            hour_offset = 0
        cur = conn.execute("""
            SELECT positions_json, positions_viz_json, n_particles, active_fraction,
                   simulated_at, sim_start, current_source, day_offset, hour_offset
            FROM drift_predictions
            WHERE hour_offset = ?
            ORDER BY id DESC LIMIT 1
        """, (hour_offset,))
    else:
        try:
            day_offset = int(request.args.get('day', '0'))
        except ValueError:
            day_offset = 0
        # Snapshot journalier = bord de journée (hour_offset % 24 == 0).
        # hour_offset IS NULL = sim antérieures à la résolution 3h.
        cur = conn.execute("""
            SELECT positions_json, positions_viz_json, n_particles, active_fraction,
                   simulated_at, sim_start, current_source, day_offset, hour_offset
            FROM drift_predictions
            WHERE day_offset = ?
              AND (hour_offset IS NULL OR hour_offset % 24 = 0)
            ORDER BY id DESC LIMIT 1
        """, (day_offset,))

    row = cur.fetchone()
    conn.close()

    if not row:
        return jsonify({'error': 'No drift data'}), 404

    # La carte préfère l'échantillon dense régional (positions_viz_json) ;
    # repli sur l'échantillon uniforme pour les sim antérieures à ce champ.
    raw = None
    try:
        raw = row['positions_viz_json']
    except (IndexError, KeyError):
        raw = None
    if not raw:
        raw = row['positions_json']
    try:
        positions = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        positions = []

    return jsonify({
        'day_offset': row['day_offset'],
        'hour_offset': row['hour_offset'],
        'simulated_at': row['simulated_at'],
        'sim_start': row['sim_start'],
        'source': row['current_source'],
        'n_particles': row['n_particles'],
        'active_fraction': row['active_fraction'],
        'positions': positions,
    })


@app.route('/api/timeline')
def api_timeline():
    """Timeline horaire (résolution 3h) du risque par plage — dernier run.

    Sans paramètre : toutes les plages SBH avec leur série 3h et l'heure
    d'arrivée prévue (1er pas où risk_level >= medium).
    ?beach=Nom  → restreint à une plage.
    ?hours=N    → horizon en heures (défaut 48).

    L'heure d'arrivée = 1er pas où la PRÉSENCE (local) atteint la plage, et non
    plus l'entrée d'une masse dans la zone régionale (cohérent avec le badge).
    """
    from beaches import presence_label
    try:
        horizon = int(request.args.get('hours', '48'))
    except ValueError:
        horizon = 48
    beach_filter = request.args.get('beach')

    conn = get_db()
    last = conn.execute(
        "SELECT MAX(computed_at) AS c FROM beach_timeline WHERE island = ?",
        (ISLAND,),
    ).fetchone()
    if not last or not last['c']:
        conn.close()
        return jsonify({'island': ISLAND, 'computed_at': None, 'beaches': []})
    computed_at = last['c']

    params = [ISLAND, computed_at, horizon]
    beach_clause = ""
    if beach_filter:
        beach_clause = " AND beach_name = ?"
        params.append(beach_filter)

    rows = conn.execute(f"""
        SELECT beach_name, beach_lat, beach_lon, hour_offset, day_offset,
               valid_time, risk_level,
               ROUND(regional_score, 1) AS regional_score,
               ROUND(local_score, 2) AS local_score,
               ROUND(closest_km, 1) AS closest_km
        FROM beach_timeline
        WHERE island = ? AND computed_at = ? AND hour_offset <= ?{beach_clause}
        ORDER BY beach_name, hour_offset
    """, params).fetchall()
    conn.close()

    rank = {'none': 0, 'low': 1, 'medium': 2, 'high': 3}
    beaches = {}
    for r in rows:
        name = r['beach_name']
        b = beaches.get(name)
        if b is None:
            b = beaches[name] = {
                'name': name,
                'lat': r['beach_lat'],
                'lon': r['beach_lon'],
                'computed_at': computed_at,
                'arrival_hour': None,
                'arrival_time': None,
                'series': [],
            }
        presence = presence_label(r['local_score'])
        b['series'].append({
            'hour_offset': r['hour_offset'],
            'day_offset': r['day_offset'],
            'valid_time': r['valid_time'],
            'risk_level': r['risk_level'],
            'color': risk_to_color(r['risk_level']),
            'label': risk_to_fr(r['risk_level']),
            'regional_score': r['regional_score'],
            'local_score': r['local_score'],
            'presence': presence,
            'presence_color': risk_to_color(presence),
            'closest_km': r['closest_km'],
        })
        # Arrivée SUR la plage = 1er pas où la présence (local) atteint au moins "faible"
        if b['arrival_hour'] is None and rank.get(presence, 0) >= 1:
            b['arrival_hour'] = r['hour_offset']
            b['arrival_time'] = r['valid_time']

    return jsonify({
        'island': ISLAND,
        'computed_at': computed_at,
        'horizon_hours': horizon,
        'count': len(beaches),
        'beaches': list(beaches.values()),
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


@app.route('/api/observations')
def api_observations():
    """Dernières observations terrain (bénévoles) APPROUVÉES, < 24 h, par plage SBH.

    Anonyme — complète la prévision du modèle sur la carte publique (le
    « dernier kilomètre » : ce qu'un humain a vraiment vu sur place).
    """
    obs = contributors_db.latest_public_observations(ISLAND, within_hours=24)
    # Construit les URLs de photos côté serveur (le front n'a pas à connaître
    # le schéma d'URL ni le nombre de photos à l'avance).
    for b in obs.values():
        n = b.pop('n_photos', 0)
        b['photos'] = [f"/api/observation-photo/{b['obs_id']}/{i}" for i in range(n)]
    return jsonify({'island': ISLAND, 'window_hours': 24, 'beaches': obs})


@app.route('/api/observation-photo/<int:obs_id>/<int:idx>')
def api_observation_photo(obs_id, idx):
    """Sert une photo (par index) d'une observation APPROUVÉE par Sam.

    Les photos en attente ou rejetées ne sont jamais servies (404).
    """
    photos = contributors_db.get_approved_photos(obs_id)
    if idx < 0 or idx >= len(photos):
        abort(404)
    # send_from_directory borne l'accès au dossier photos (anti path-traversal)
    return send_from_directory(PHOTOS_DIR, Path(photos[idx]).name, max_age=3600)


# ── Tendance de la dérive (la masse arrive-t-elle ou repart-elle ?) ─────────────
# Signal NIVEAU ÎLE : Saint-Barth (~10 km) est petit devant le rayon de 70 km, donc
# toutes les plages "voient" le même nuage. On regarde la fenêtre fiable 0→24 h.
ISLAND_CENTER = (17.9, -62.83)
TREND_RADIUS_KM = 70.0


def _hav(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1); dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def _bearing_idx(lat1, lon1, lat2, lon2):
    """Index 0-7 (N,NE,E,SE,S,SO,O,NO) de la direction île → centre de masse."""
    dlon = math.radians(lon2 - lon1)
    y = math.sin(dlon) * math.cos(math.radians(lat2))
    x = (math.cos(math.radians(lat1)) * math.sin(math.radians(lat2))
         - math.sin(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.cos(dlon))
    brng = (math.degrees(math.atan2(y, x)) + 360) % 360
    return int((brng + 22.5) // 45) % 8


def compute_drift_trend(conn) -> dict:
    """La masse proche se rapproche-t-elle (↗) ou s'éloigne-t-elle (↘) ?

    state : 'approach' | 'recede' | 'stable' | 'calm' | 'unknown'
    compass_idx : où se trouve la masse (0-7), closest_km : distance actuelle.
    """
    row = conn.execute("SELECT MAX(simulated_at) m FROM drift_predictions").fetchone()
    if not row or not row["m"]:
        return {"state": "unknown"}
    sim = row["m"]
    samples = []
    for h in (0, 12, 24):
        r = conn.execute(
            "SELECT positions_json, positions_viz_json FROM drift_predictions "
            "WHERE simulated_at=? AND hour_offset=? ORDER BY id DESC LIMIT 1", (sim, h)
        ).fetchone()
        if not r:
            continue
        raw = r["positions_viz_json"] or r["positions_json"]
        try:
            pts = json.loads(raw) if raw else []
        except (TypeError, json.JSONDecodeError):
            pts = []
        near = [(_hav(ISLAND_CENTER[0], ISLAND_CENTER[1], lat, lon), lon, lat)
                for lon, lat in pts]
        near = [x for x in near if x[0] <= TREND_RADIUS_KM]
        if near:
            samples.append({
                "n": len(near),
                "nearest": min(x[0] for x in near),
                "clat": sum(x[2] for x in near) / len(near),
                "clon": sum(x[1] for x in near) / len(near),
            })
    if not samples or samples[0]["n"] < 3:
        return {"state": "calm"}
    first, last = samples[0], samples[-1]
    delta = last["nearest"] - first["nearest"]
    state = "recede" if delta > 5 else ("approach" if delta < -5 else "stable")
    return {
        "state": state,
        "compass_idx": _bearing_idx(ISLAND_CENTER[0], ISLAND_CENTER[1],
                                    first["clat"], first["clon"]),
        "closest_km": round(first["nearest"], 1),
    }


@app.route('/api/trend')
def api_trend():
    """Tendance île de la masse de sargasses (informatif ; ne change pas le risque)."""
    conn = get_db()
    try:
        return jsonify(compute_drift_trend(conn))
    finally:
        conn.close()


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
