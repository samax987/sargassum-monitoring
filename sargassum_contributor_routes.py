#!/usr/bin/env python3
"""
sargassum_contributor_routes.py
===============================
Portail public « contributeurs » : permet à des bénévoles de Saint-Barth de
créer un compte, puis (une fois validés par Sam) de signaler l'état des plages.

Bilingue FR / EN : la langue vient de ?lang= (mémorisée en session), repli FR.
Les chaînes affichées sont dans contrib_i18n.py.

Sécurité (reprend les patterns éprouvés de villa-suite / sailtracker sur ce VPS) :
  - mots de passe hachés (werkzeug.security)
  - sessions signées (cookie Flask), validées à chaque requête
  - jeton CSRF sur tous les POST du portail (et UNIQUEMENT du portail, pour ne
    pas casser /api/subscribe qui est du JSON sans CSRF)
  - rate-limiting par IP (inscription, connexion) et par compte (signalements)

Un signalement est créé en statut 'pending' dans `contributor_observations`.
Il n'alimente la calibration qu'APRÈS approbation de Sam (cf. contributors_db).
"""

from __future__ import annotations

import logging
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import (
    Blueprint, abort, redirect, render_template, request, send_from_directory,
    session, url_for,
)
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.security import check_password_hash, generate_password_hash

import beaches_db
import contributors_db
from contrib_i18n import get_strings, SUPPORTED_LANGS, current_lang

logger = logging.getLogger(__name__)

ISLAND = "Saint-Barth"

# Clés de risque valides (indépendantes de la langue ; libellés dans contrib_i18n)
RISK_KEYS = {"none", "low", "medium", "high"}

# Validation des identifiants
USERNAME_RE = re.compile(r"^[A-Za-z0-9._-]{3,30}$")
MIN_PASSWORD_LEN = 8
MAX_NOTES_LEN = 500

# Photos jointes : ré-encodées en JPEG via Pillow (supprime EXIF/GPS — vie
# privée), bord max 1600 px. Stockées hors de templates/, jamais committées.
PHOTOS_DIR = Path(__file__).parent / "contrib_photos"
PHOTO_MAX_EDGE = 1600
PHOTO_JPEG_QUALITY = 85

# Rate-limiting (en mémoire process ; suffisant pour ce volume)
LOGIN_MAX, LOGIN_WINDOW_S = 5, 15 * 60          # 5 tentatives / 15 min / IP
REGISTER_MAX, REGISTER_WINDOW_S = 5, 60 * 60    # 5 inscriptions / h / IP
SUBMIT_MAX, SUBMIT_WINDOW_S = 20, 60 * 60       # 20 signalements / h / compte

_rate_buckets: dict[str, list[float]] = {}

contrib_bp = Blueprint("contrib", __name__, url_prefix="/contribuer")


# ── Helpers : langue, IP, rate-limit, CSRF, session ──────────────────────────

def _lang() -> str:
    """Langue courante (logique centralisée dans contrib_i18n.current_lang)."""
    return current_lang()


def _client_ip() -> str:
    """Vraie IP du client derrière nginx/Cloudflare (audit + rate-limit)."""
    cf = request.headers.get("CF-Connecting-IP")
    if cf:
        return cf.strip()
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "?"


def _rate_limit_ok(key: str, max_events: int, window_s: int) -> bool:
    """True si l'évènement est autorisé ; enregistre l'évènement au passage."""
    now = time.time()
    events = [t for t in _rate_buckets.get(key, []) if now - t < window_s]
    if len(events) >= max_events:
        _rate_buckets[key] = events
        return False
    events.append(now)
    _rate_buckets[key] = events
    return True


def _ensure_csrf() -> str:
    if "_csrf" not in session:
        session["_csrf"] = secrets.token_urlsafe(32)
    return session["_csrf"]


def _csrf_valid() -> bool:
    token_session = session.get("_csrf")
    token_form = request.form.get("_csrf") or request.headers.get("X-CSRF-Token")
    return bool(
        token_session and token_form
        and secrets.compare_digest(token_session, token_form)
    )


@contrib_bp.before_request
def _csrf_protect():
    # CSRF uniquement sur les POST du portail contributeurs.
    if request.method == "POST" and not _csrf_valid():
        abort(403)


def _current_contributor() -> dict | None:
    """Contributeur connecté ET actif, sinon None (session invalidée si banni)."""
    cid = session.get("contrib_id")
    if not cid:
        return None
    c = contributors_db.get_by_id(cid)
    if not c or c["status"] != contributors_db.ACCOUNT_ACTIVE:
        return None
    return c


def _start_session(contributor: dict) -> None:
    """Ouvre une session propre (anti-fixation) après une connexion réussie.

    Conserve la langue choisie avant connexion.
    """
    lang = session.get("lang", "fr")
    session.clear()
    session["lang"] = lang
    session["contrib_id"] = contributor["id"]
    session.permanent = True
    session["_csrf"] = secrets.token_urlsafe(32)


def _sbh_beaches() -> list[str]:
    """Plages de Saint-Barth pour le menu déroulant (source beaches_config)."""
    try:
        rows = beaches_db.list_all(only_active=True)
        names = [b["name"] for b in rows if b.get("island") == ISLAND]
        if names:
            return sorted(names)
    except Exception:
        logger.exception("Lecture beaches_config échouée — fallback BEACHES")
    try:
        from beaches import BEACHES
        return sorted(b["name"] for b in BEACHES if b["island"] == ISLAND)
    except Exception:
        return []


def _parse_observed_at(raw: str) -> str | None:
    """Parse un champ datetime-local. Retourne ISO (secondes) ou None si invalide.

    Tolère un large créneau [maintenant-30j, maintenant+1j] : assez souple pour
    le décalage horaire SBH (UTC-4) et de petites dérives d'horloge, assez strict
    pour rejeter les fautes de frappe évidentes (année suivante, etc.).
    """
    raw = (raw or "").strip()
    now = datetime.now(timezone.utc)
    if not raw:
        return now.strftime("%Y-%m-%dT%H:%M:%S")
    dt = None
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            break
        except ValueError:
            continue
    if dt is None:
        return None
    if dt > now + timedelta(days=1) or dt < now - timedelta(days=30):
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _now_local() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")


def _process_photo(file_storage) -> str | None:
    """Valide et ré-encode la photo jointe. Retourne le chemin relatif stocké.

    - Pillow ouvre le fichier : si ce n'est pas une vraie image (peu importe
      l'extension annoncée), ça lève → ValueError("invalid").
    - exif_transpose AVANT suppression des métadonnées : on garde l'orientation
      visuelle correcte, puis le ré-encodage JPEG élimine EXIF/GPS (vie privée).
    - thumbnail borne le plus grand côté à PHOTO_MAX_EDGE (économie disque).
    Nom de fichier aléatoire (jamais le nom client) → pas de path traversal.
    """
    from PIL import Image, ImageOps, UnidentifiedImageError

    if not file_storage or not file_storage.filename:
        return None
    try:
        img = Image.open(file_storage.stream)
        img = ImageOps.exif_transpose(img)
        img.thumbnail((PHOTO_MAX_EDGE, PHOTO_MAX_EDGE))
        img = img.convert("RGB")
    except (UnidentifiedImageError, OSError, ValueError) as e:
        logger.info("Photo rejetée (illisible) : %s", e)
        raise ValueError("invalid")

    PHOTOS_DIR.mkdir(exist_ok=True)
    name = f"{secrets.token_hex(8)}.jpg"
    img.save(PHOTOS_DIR / name, "JPEG", quality=PHOTO_JPEG_QUALITY)
    return f"contrib_photos/{name}"


# ── Routes ───────────────────────────────────────────────────────────────────

@contrib_bp.route("/")
def home():
    return render_template(
        "contrib_home.html",
        t=get_strings(_lang()), lang=_lang(),
        contributor=_current_contributor(),
        csrf_token=_ensure_csrf(),
    )


@contrib_bp.route("/inscription", methods=["GET", "POST"])
def register():
    lang = _lang()
    t = get_strings(lang)
    if _current_contributor():
        return redirect(url_for("contrib.observer"))

    if request.method == "GET":
        return render_template("contrib_register.html", t=t, lang=lang,
                               csrf_token=_ensure_csrf())

    # POST — création de compte
    # Honeypot : champ caché que seuls les bots remplissent.
    if request.form.get("website"):
        logger.info("Inscription honeypot déclenchée (IP %s)", _client_ip())
        return render_template("contrib_register.html", t=t, lang=lang,
                               csrf_token=_ensure_csrf(), registered=True)

    if not _rate_limit_ok(f"reg:{_client_ip()}", REGISTER_MAX, REGISTER_WINDOW_S):
        return render_template("contrib_register.html", t=t, lang=lang,
                               csrf_token=_ensure_csrf(),
                               error=t["err_rate_register"]), 429

    username = (request.form.get("username") or "").strip()
    display_name = (request.form.get("display_name") or "").strip()
    password = request.form.get("password") or ""
    password2 = request.form.get("password2") or ""

    error = None
    if not USERNAME_RE.match(username):
        error = t["err_username"]
    elif not (1 <= len(display_name) <= 40):
        error = t["err_name"]
    elif len(password) < MIN_PASSWORD_LEN:
        error = t["err_pw_len"]
    elif password != password2:
        error = t["err_pw_match"]

    if error:
        return render_template("contrib_register.html", t=t, lang=lang,
                               csrf_token=_ensure_csrf(), error=error,
                               username=username, display_name=display_name)

    new_id = contributors_db.create_contributor(
        username=username,
        display_name=display_name,
        password_hash=generate_password_hash(password),
    )
    if new_id is None:
        return render_template("contrib_register.html", t=t, lang=lang,
                               csrf_token=_ensure_csrf(),
                               error=t["err_username_taken"],
                               display_name=display_name)

    logger.info("Nouveau contributeur #%s (%s) — en attente de validation",
                new_id, username)
    return render_template("contrib_register.html", t=t, lang=lang,
                           csrf_token=_ensure_csrf(), registered=True)


@contrib_bp.route("/connexion", methods=["GET", "POST"])
def login():
    lang = _lang()
    t = get_strings(lang)
    if _current_contributor():
        return redirect(url_for("contrib.observer"))

    if request.method == "GET":
        return render_template("contrib_login.html", t=t, lang=lang,
                               csrf_token=_ensure_csrf())

    # POST — connexion
    if not _rate_limit_ok(f"login:{_client_ip()}", LOGIN_MAX, LOGIN_WINDOW_S):
        return render_template("contrib_login.html", t=t, lang=lang,
                               csrf_token=_ensure_csrf(),
                               error=t["err_rate_login"]), 429

    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    c = contributors_db.get_by_username(username)

    # Anti-énumération : message générique tant que les identifiants ne sont pas
    # valides. Le statut (en attente / refusé) n'est révélé qu'avec le bon
    # mot de passe.
    if not c or not check_password_hash(c["password_hash"], password):
        return render_template("contrib_login.html", t=t, lang=lang,
                               csrf_token=_ensure_csrf(),
                               error=t["err_bad_creds"], username=username), 401

    status = c["status"]
    if status == contributors_db.ACCOUNT_ACTIVE:
        _start_session(c)
        contributors_db.record_login(c["id"])
        logger.info("Connexion contributeur #%s (%s)", c["id"], username)
        return redirect(url_for("contrib.observer"))

    msg = t["msg_pending"] if status == contributors_db.ACCOUNT_PENDING else t["msg_not_allowed"]
    return render_template("contrib_login.html", t=t, lang=lang,
                           csrf_token=_ensure_csrf(), error=msg,
                           username=username), 403


@contrib_bp.route("/deconnexion", methods=["POST"])
def logout():
    lang = session.get("lang", "fr")
    session.clear()
    session["lang"] = lang  # conserve la langue après déconnexion
    return redirect(url_for("contrib.home"))


@contrib_bp.route("/observer")
def observer():
    lang = _lang()
    c = _current_contributor()
    if not c:
        return redirect(url_for("contrib.login"))
    return render_template(
        "contrib_observer.html",
        t=get_strings(lang), lang=lang,
        contributor=c,
        beaches=_sbh_beaches(),
        csrf_token=_ensure_csrf(),
        ok=request.args.get("ok") == "1",
        now_local=_now_local(),
    )


@contrib_bp.route("/observation", methods=["POST"])
def submit_observation():
    lang = _lang()
    t = get_strings(lang)
    c = _current_contributor()
    if not c:
        return redirect(url_for("contrib.login"))

    if not _rate_limit_ok(f"submit:{c['id']}", SUBMIT_MAX, SUBMIT_WINDOW_S):
        return render_template("contrib_observer.html", t=t, lang=lang,
                               contributor=c, beaches=_sbh_beaches(),
                               csrf_token=_ensure_csrf(),
                               error=t["err_rate_submit"],
                               now_local=_now_local()), 429

    beaches = _sbh_beaches()
    beach_name = (request.form.get("beach_name") or "").strip()
    observed_risk = (request.form.get("observed_risk") or "").strip()
    observed_at = _parse_observed_at(request.form.get("observed_at"))
    notes = (request.form.get("notes") or "").strip()[:MAX_NOTES_LEN] or None

    coverage_raw = (request.form.get("coverage_pct") or "").strip()
    coverage = None
    if coverage_raw:
        try:
            coverage = max(0, min(100, int(float(coverage_raw))))
        except ValueError:
            coverage = None

    # Validation serveur (ne jamais faire confiance au formulaire)
    error = None
    if beach_name not in beaches:
        error = t["err_beach"]
    elif observed_risk not in RISK_KEYS:
        error = t["err_risk"]
    elif observed_at is None:
        error = t["err_date"]

    # Photo optionnelle : validée/ré-encodée seulement si le reste est valide
    photo_path = None
    if error is None:
        try:
            photo_path = _process_photo(request.files.get("photo"))
        except ValueError:
            error = t["err_photo"]

    if error:
        return render_template("contrib_observer.html", t=t, lang=lang,
                               contributor=c, beaches=beaches,
                               csrf_token=_ensure_csrf(), error=error,
                               now_local=_now_local()), 400

    contributors_db.add_observation(
        contributor_id=c["id"],
        observed_at=observed_at,
        island=ISLAND,
        beach_name=beach_name,
        observed_risk=observed_risk,
        coverage_pct=coverage,
        notes=notes,
        client_ip=_client_ip(),
        photo_path=photo_path,
    )
    logger.info("Signalement contributeur #%s : %s / %s (risque %s%s)",
                c["id"], ISLAND, beach_name, observed_risk,
                ", photo" if photo_path else "")
    return redirect(url_for("contrib.observer", ok=1))


@contrib_bp.route("/photo/<int:obs_id>")
def photo(obs_id: int):
    """Sert la photo d'un signalement — uniquement à son auteur connecté.

    (Sam, lui, voit les photos directement dans le dashboard Streamlit qui lit
    les fichiers en local sur le serveur.)
    """
    c = _current_contributor()
    if not c:
        abort(404)  # 404 plutôt que 403 : ne révèle pas l'existence de la photo
    obs = contributors_db.get_observation(obs_id)
    if not obs or obs["contributor_id"] != c["id"] or not obs.get("photo_path"):
        abort(404)
    # send_from_directory borne l'accès au dossier photos (anti-traversal)
    return send_from_directory(PHOTOS_DIR, Path(obs["photo_path"]).name,
                               max_age=3600)


@contrib_bp.app_errorhandler(RequestEntityTooLarge)
def _too_large(_e):
    """Fichier au-delà de MAX_CONTENT_LENGTH : message clair au lieu d'une 413 brute."""
    lang = current_lang()
    t = get_strings(lang)
    c = _current_contributor()
    if not c:
        return redirect(url_for("contrib.home"))
    return render_template("contrib_observer.html", t=t, lang=lang,
                           contributor=c, beaches=_sbh_beaches(),
                           csrf_token=_ensure_csrf(),
                           error=t["err_photo_size"],
                           now_local=_now_local()), 413


@contrib_bp.route("/mes-signalements")
def my_observations():
    lang = _lang()
    c = _current_contributor()
    if not c:
        return redirect(url_for("contrib.login"))
    return render_template(
        "contrib_mes_obs.html",
        t=get_strings(lang), lang=lang,
        contributor=c,
        observations=contributors_db.list_observations_for(c["id"]),
        csrf_token=_ensure_csrf(),
    )


def register_contributor_routes(app) -> None:
    """À appeler depuis sargassum_web.py : register_contributor_routes(app)."""
    app.register_blueprint(contrib_bp)
