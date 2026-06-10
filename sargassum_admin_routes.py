"""
Routes admin a inclure dans sargassum_web.py
============================================
Ajoute les endpoints :
  GET  /admin                  Redirige vers /admin/beaches
  GET  /admin/beaches          UI gestion des plages
  GET  /admin/api/beaches      JSON liste complete
  POST /admin/api/beaches      Cree une nouvelle plage
  PUT  /admin/api/beaches/<id> Met a jour une plage
  POST /admin/api/beaches/<id>/delete  Desactive une plage
  GET  /admin/stats            Page de stats publiques

Auth : HTTP Basic Auth, user = 'sam', password depuis env ADMIN_PASSWORD.
Durci (audit 2026-06-10) : comparaison en temps constant (compare_digest)
+ limitation des tentatives par IP, alignées sur le portail contributeurs.
"""

import os
import secrets
import time
from functools import wraps

from flask import request, jsonify, redirect, render_template, Response

import beaches_db


# ── Auth ───────────────────────────────────────────────────────────────────────

ADMIN_USER = 'sam'

# Anti force-brute : 10 echecs / 15 min / IP, puis 429.
# En memoire process (suffisant : un seul utilisateur legitime).
AUTH_FAIL_MAX = 10
AUTH_FAIL_WINDOW_S = 15 * 60
_auth_failures: dict[str, list[float]] = {}


def _load_admin_password() -> str:
    """Charge le mot de passe admin depuis .env ou variable d'env."""
    # Cherche d'abord dans .env
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith('ADMIN_PASSWORD='):
                    return line.split('=', 1)[1].strip()
    return os.environ.get('ADMIN_PASSWORD', 'changeme')


ADMIN_PASSWORD = _load_admin_password()


def _client_ip() -> str:
    """Vraie IP du client derriere nginx/Cloudflare (pour le rate-limit)."""
    cf = request.headers.get('CF-Connecting-IP')
    if cf:
        return cf.strip()
    xff = request.headers.get('X-Forwarded-For')
    if xff:
        return xff.split(',')[0].strip()
    return request.remote_addr or '?'


def _too_many_failures(ip: str) -> bool:
    now = time.time()
    fails = [t for t in _auth_failures.get(ip, []) if now - t < AUTH_FAIL_WINDOW_S]
    _auth_failures[ip] = fails
    return len(fails) >= AUTH_FAIL_MAX


def _record_failure(ip: str) -> None:
    _auth_failures.setdefault(ip, []).append(time.time())


def check_auth(auth) -> bool:
    if not auth or auth.username is None or auth.password is None:
        return False
    # compare_digest : duree de comparaison independante du contenu,
    # evite de deviner le mot de passe caractere par caractere (timing attack)
    user_ok = secrets.compare_digest(auth.username, ADMIN_USER)
    pass_ok = secrets.compare_digest(auth.password, ADMIN_PASSWORD)
    return user_ok and pass_ok


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        ip = _client_ip()
        if _too_many_failures(ip):
            return Response('Trop de tentatives. Reessaie dans 15 minutes.\n', 429)
        auth = request.authorization
        if not check_auth(auth):
            # On ne compte que les vraies tentatives (identifiants fournis),
            # pas le 1er passage du navigateur qui demande le formulaire.
            if auth:
                _record_failure(ip)
            return Response(
                'Acces protege\n', 401,
                {'WWW-Authenticate': 'Basic realm="Sargassum Admin"'},
            )
        return f(*args, **kwargs)
    return decorated


# ── Routes ─────────────────────────────────────────────────────────────────────

def register_admin_routes(app):
    """A appeler depuis sargassum_web.py : register_admin_routes(app)"""

    @app.route('/admin')
    @require_auth
    def admin_home():
        return redirect('/admin/beaches')

    @app.route('/admin/beaches')
    @require_auth
    def admin_beaches():
        return render_template('admin_beaches.html')

    @app.route('/admin/api/beaches', methods=['GET'])
    @require_auth
    def admin_api_list():
        beaches = beaches_db.list_all(only_active=False)
        return jsonify({'count': len(beaches), 'beaches': beaches})

    @app.route('/admin/api/beaches', methods=['POST'])
    @require_auth
    def admin_api_create():
        data = request.get_json() or {}
        required = ['island', 'name', 'lat', 'lon']
        missing = [k for k in required if k not in data]
        if missing:
            return jsonify({'error': f'Champs manquants : {missing}'}), 400
        try:
            beach_id = beaches_db.create_beach(
                island=data['island'],
                name=data['name'],
                lat=float(data['lat']),
                lon=float(data['lon']),
                radius_km=float(data.get('radius_km', 2.0)),
                exposure=data.get('exposure', 'moderate'),
                orientation=data.get('orientation', ''),
                description=data.get('description', ''),
            )
            if beach_id is None:
                return jsonify({'error': 'Plage deja existante (island+name)'}), 409
            return jsonify({'id': beach_id, 'success': True})
        except (ValueError, TypeError) as e:
            return jsonify({'error': str(e)}), 400

    @app.route('/admin/api/beaches/<int:beach_id>', methods=['PUT'])
    @require_auth
    def admin_api_update(beach_id):
        data = request.get_json() or {}
        # Convertit les types numeriques
        for k in ('lat', 'lon', 'radius_km'):
            if k in data:
                try:
                    data[k] = float(data[k])
                except (ValueError, TypeError):
                    return jsonify({'error': f'{k} invalide'}), 400
        if 'active' in data:
            data['active'] = 1 if data['active'] else 0
        ok = beaches_db.update_beach(beach_id, **data)
        if not ok:
            return jsonify({'error': 'Plage introuvable ou aucun champ a mettre a jour'}), 404
        return jsonify({'success': True, 'updated': beach_id})

    @app.route('/admin/api/beaches/<int:beach_id>/delete', methods=['POST'])
    @require_auth
    def admin_api_delete(beach_id):
        ok = beaches_db.delete_beach(beach_id)
        if not ok:
            return jsonify({'error': 'Plage introuvable'}), 404
        return jsonify({'success': True, 'deactivated': beach_id})

    # ─── Stats publiques ───────────────────────────────────────────────────────

    @app.route('/stats')
    def public_stats():
        return render_template('stats.html')

    @app.route('/api/stats')
    def api_stats():
        import sqlite3
        from datetime import datetime, timezone, timedelta

        DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sargassum_data.db')
        conn = sqlite3.connect(DB)
        conn.row_factory = sqlite3.Row

        # Compteurs simples
        n_subs = conn.execute("SELECT COUNT(*) FROM telegram_subscriptions").fetchone()[0]
        n_unique_users = conn.execute("SELECT COUNT(DISTINCT chat_id) FROM telegram_subscriptions").fetchone()[0]
        n_alerts_total = conn.execute("SELECT COUNT(*) FROM alert_state WHERE sent_at IS NOT NULL").fetchone()[0]
        n_drift_sims = conn.execute("SELECT COUNT(DISTINCT simulated_at) FROM drift_predictions").fetchone()[0]
        n_observations = conn.execute("SELECT COUNT(*) FROM beach_observations").fetchone()[0]
        n_beaches = conn.execute("SELECT COUNT(*) FROM beaches_config WHERE active = 1").fetchone()[0] if conn.execute("SELECT name FROM sqlite_master WHERE name='beaches_config'").fetchone() else 0

        # Derniere collecte / scoring
        last_collect = conn.execute("SELECT MAX(collected_at) FROM copernicus_currents").fetchone()[0]
        last_scoring = conn.execute("SELECT MAX(computed_at) FROM beach_risk_scores").fetchone()[0]
        last_alert = conn.execute("SELECT MAX(sent_at) FROM alert_state WHERE sent_at IS NOT NULL").fetchone()[0]

        # Alertes dernieres 7 jours
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        n_alerts_7d = conn.execute("SELECT COUNT(*) FROM alert_state WHERE sent_at > ?", (cutoff,)).fetchone()[0]

        # Top abonnements par plage
        top_beaches_sub = conn.execute("""
            SELECT beach_name, COUNT(*) as n
            FROM telegram_subscriptions
            GROUP BY beach_name
            ORDER BY n DESC
            LIMIT 5
        """).fetchall()

        # Distribution risk_level actuelle (par ile)
        risk_dist = conn.execute("""
            SELECT island, risk_level, COUNT(*) as n
            FROM beach_risk_scores
            WHERE computed_at = (SELECT MAX(computed_at) FROM beach_risk_scores)
              AND day_offset = 0
            GROUP BY island, risk_level
        """).fetchall()

        # Tendance alertes 30 jours
        alerts_history = conn.execute("""
            SELECT DATE(sent_at) as day, COUNT(*) as n
            FROM alert_state
            WHERE sent_at > date('now', '-30 days')
            GROUP BY DATE(sent_at)
            ORDER BY day
        """).fetchall()

        conn.close()

        return jsonify({
            'counters': {
                'subscriptions':     n_subs,
                'unique_users':      n_unique_users,
                'alerts_total':      n_alerts_total,
                'alerts_7d':         n_alerts_7d,
                'drift_simulations': n_drift_sims,
                'observations':      n_observations,
                'active_beaches':    n_beaches,
            },
            'timestamps': {
                'last_collect': last_collect,
                'last_scoring': last_scoring,
                'last_alert':   last_alert,
            },
            'top_beaches_subscribed': [{'beach': r['beach_name'], 'count': r['n']} for r in top_beaches_sub],
            'risk_distribution': [{'island': r['island'], 'level': r['risk_level'], 'count': r['n']} for r in risk_dist],
            'alerts_history_30d': [{'day': r['day'], 'count': r['n']} for r in alerts_history],
        })
