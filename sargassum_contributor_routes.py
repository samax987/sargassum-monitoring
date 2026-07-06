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
from zoneinfo import ZoneInfo

# Fuseau de Saint-Barthélemy (UTC-4 toute l'année, pas d'heure d'été).
# Les formulaires saisissent l'heure locale ; la DB reste en UTC.
TZ_SBH = ZoneInfo("America/Guadeloupe")

from flask import (
    Blueprint, abort, g, redirect, render_template, request,
    send_from_directory, session, url_for,
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

# Où sont les sargasses (indépendant de la langue ; libellés dans contrib_i18n).
# Pertinent seulement s'il y a des sargasses (observed_risk != "none").
LOCATION_KEYS = {"water", "beach", "both"}

# Validation des identifiants
USERNAME_RE = re.compile(r"^[A-Za-z0-9._-]{3,30}$")
MIN_PASSWORD_LEN = 8
MAX_NOTES_LEN = 500

# Photos jointes : ré-encodées en JPEG via Pillow (supprime EXIF/GPS — vie
# privée), bord max 1600 px. Stockées hors de templates/, jamais committées.
PHOTOS_DIR = Path(__file__).parent / "contrib_photos"
PHOTO_MAX_EDGE = 1600
PHOTO_JPEG_QUALITY = 85
MAX_PHOTOS = 3   # une plage ne tient pas dans un cadre + échouage localisé

# Rate-limiting (en mémoire process ; suffisant pour ce volume)
LOGIN_MAX, LOGIN_WINDOW_S = 8, 15 * 60          # 8 tentatives / 15 min / IP
REGISTER_MAX, REGISTER_WINDOW_S = 5, 60 * 60    # 5 inscriptions / h / IP
SUBMIT_MAX, SUBMIT_WINDOW_S = 20, 60 * 60       # 20 signalements / h / compte
FORGOT_MAX, FORGOT_WINDOW_S = 5, 60 * 60        # 5 demandes de reset / h / IP
CHPW_MAX, CHPW_WINDOW_S = 10, 15 * 60           # 10 changements / 15 min / compte
ANON_SUBMIT_MAX, ANON_SUBMIT_WINDOW_S = 10, 60 * 60  # 10 signalements directs / h / IP

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


def _env_value(key: str, default: str = "") -> str:
    """Lit une clé du .env (lecture manuelle, comme le reste du projet)."""
    import os
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip()
    return os.environ.get(key, default)


def _notify_admin_telegram(text: str) -> None:
    """Prévient Sam sur Telegram (canal admin). Best-effort : toute erreur est
    avalée — une panne Telegram ne doit JAMAIS bloquer le bénévole ni révéler
    quoi que ce soit côté web.

    Texte BRUT (pas de parse_mode) : le message contient des données saisies par
    l'utilisateur (identifiant, nom) ; un caractère spécial ne doit pas faire
    échouer l'envoi ni permettre d'injecter du formatage.
    """
    token = _env_value("TELEGRAM_TOKEN")
    chat = _env_value("TELEGRAM_CHAT")
    if not token or not chat:
        return
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat, "text": text},
            timeout=8,
        )
    except Exception:
        logger.warning("Notification Telegram (reset) échouée", exc_info=False)


def _turnstile_sitekey() -> str:
    """Clé publique Turnstile (à mettre dans le widget). Vide si non configuré."""
    return _env_value("TURNSTILE_SITEKEY")


def _verify_turnstile() -> bool:
    """Vérifie le captcha Cloudflare Turnstile du formulaire.

    - Pas de secret configuré → True (on s'appuie sur honeypot + rate-limit +
      modération ; permet de livrer la fonctionnalité avant que Sam ait les clés).
    - Secret configuré mais jeton absent/invalide → False (probable bot).
    - Cloudflare injoignable → True (fail-open : ne pas bloquer un vrai bénévole
      sur une panne réseau ; la modération reste le filet de sécurité).
    """
    secret = _env_value("TURNSTILE_SECRET")
    if not secret:
        return True
    token = request.form.get("cf-turnstile-response", "")
    if not token:
        # Diagnostic : aucun jeton = le widget ne s'est pas chargé/résolu côté
        # client (souvent : domaine non autorisé pour la sitekey, ou CSP/réseau).
        logger.warning("Turnstile : aucun jeton reçu (widget non résolu côté client)")
        return False
    try:
        import requests
        r = requests.post(
            "https://challenges.cloudflare.com/turnstile/v0/siteverify",
            data={"secret": secret, "response": token, "remoteip": _client_ip()},
            timeout=8,
        )
        data = r.json()
        if not data.get("success"):
            # error-codes Cloudflare : la cause exacte du refus (ex.
            # "invalid-input-secret", "hostname-mismatch", "timeout-or-duplicate").
            logger.warning("Turnstile refusé par Cloudflare : %s", data.get("error-codes"))
        return bool(data.get("success"))
    except Exception:
        logger.warning("Vérification Turnstile injoignable — on laisse passer")
        return True


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
        # Cas réel terrain (juillet 2026) : certains navigateurs mobiles (Brave…)
        # ne renvoient pas le cookie de session → pas de jeton CSRF possible →
        # 403 sur le signalement anonyme. Pour CE formulaire-là uniquement, et
        # seulement quand il n'y a AUCUNE session (donc rien à détourner), on
        # laisse passer : il reste couvert par honeypot + rate-limit + Turnstile
        # + modération humaine systématique. Les routes de compte gardent le
        # CSRF strict.
        if request.endpoint == "contrib.report_anon" and "_csrf" not in session:
            return
        abort(403)


@contrib_bp.before_request
def _force_password_change():
    """Si un mdp temporaire est en cours (reset admin), on bloque tout tant que
    le bénévole n'a pas choisi son propre mot de passe — sauf la page de
    changement elle-même et la déconnexion (sinon boucle de redirection)."""
    c = _current_contributor()
    if not c or not c.get("must_change_password"):
        return
    if request.endpoint in ("contrib.change_password", "contrib.logout"):
        return
    return redirect(url_for("contrib.change_password"))


def _current_contributor() -> dict | None:
    """Contributeur connecté ET actif, sinon None. Mis en cache sur `g` pour la
    durée de la requête (plusieurs appels par requête : before_request + vue).

    Invalidation de session renforcée :
      - compte banni / supprimé  → None
      - mot de passe changé depuis l'ouverture de la session (pwd_changed_at ≠
        empreinte stockée à la connexion) → None. C'est le « déconnexion partout
        sur changement de mot de passe » : un cookie volé devient inutile dès que
        le mot de passe est modifié.
    """
    if "contrib_cached" in g:
        return g.contrib_cached
    result = None
    cid = session.get("contrib_id")
    if cid:
        c = contributors_db.get_by_id(cid)
        if c and c["status"] == contributors_db.ACCOUNT_ACTIVE:
            if session.get("pwd_stamp") == c.get("pwd_changed_at"):
                result = c
    g.contrib_cached = result
    return result


def _start_session(contributor: dict) -> None:
    """Ouvre une session propre (anti-fixation) après une connexion réussie.

    Conserve la langue choisie avant connexion. Mémorise l'empreinte du mot de
    passe (pwd_changed_at) : toute modification ultérieure invalide ce cookie.
    """
    lang = session.get("lang", "fr")
    session.clear()
    session["lang"] = lang
    session["contrib_id"] = contributor["id"]
    session["pwd_stamp"] = contributor.get("pwd_changed_at")
    session.permanent = True
    session["_csrf"] = secrets.token_urlsafe(32)
    g.pop("contrib_cached", None)  # force un recalcul propre


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
    """Parse un champ datetime-local (heure LOCALE Saint-Barth, UTC-4).

    Le formulaire affiche et saisit l'heure de Saint-Barth (celle du téléphone) ;
    on convertit ici en UTC avant stockage, car toute la chaîne (DB, dérive,
    « il y a Xh » sur la carte) travaille en UTC.
    Retourne ISO UTC (secondes) ou None si invalide.
    Tolère [maintenant-30j, maintenant+1j] : souple pour de petites dérives
    d'horloge, strict pour rejeter les fautes de frappe évidentes.
    """
    raw = (raw or "").strip()
    now = datetime.now(timezone.utc)
    if not raw:
        return now.strftime("%Y-%m-%dT%H:%M:%S")
    dt = None
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=TZ_SBH).astimezone(timezone.utc)
            break
        except ValueError:
            continue
    if dt is None:
        return None
    if dt > now + timedelta(days=1) or dt < now - timedelta(days=30):
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _now_local() -> str:
    # Pré-remplissage du champ date/heure : heure de Saint-Barth (pas UTC),
    # pour correspondre à ce qu'affiche le téléphone du contributeur.
    return datetime.now(TZ_SBH).strftime("%Y-%m-%dT%H:%M")


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


@contrib_bp.route("/signaler", methods=["GET", "POST"])
def report_anon():
    """Signalement DIRECT, sans compte — le chemin par défaut, zéro friction.

    Entre dans la MÊME file de modération que les signalements des comptes
    (statut 'pending') et n'influence le modèle qu'après validation de Sam.
    Anti-abus : honeypot + rate-limit par IP + Turnstile (si configuré) +
    modération humaine systématique (rien de public sans le feu vert de Sam).
    """
    lang = _lang()
    t = get_strings(lang)

    def _render(**kw):
        return render_template(
            "contrib_report_anon.html", t=t, lang=lang,
            contributor=_current_contributor(), beaches=_sbh_beaches(),
            csrf_token=_ensure_csrf(), now_local=_now_local(),
            # Plage présélectionnée quand on arrive depuis la bulle de la carte (?beach=…)
            preselected=request.args.get("beach"),
            turnstile_sitekey=_turnstile_sitekey(), **kw)

    if request.method == "GET":
        return _render()

    # Honeypot : on répond « ok » sans rien enregistrer (ne pas renseigner le bot).
    if request.form.get("website"):
        logger.info("Signalement direct honeypot (IP %s)", _client_ip())
        return _render(done=True)

    if not _rate_limit_ok(f"anon:{_client_ip()}", ANON_SUBMIT_MAX, ANON_SUBMIT_WINDOW_S):
        return _render(error=t["err_rate_submit"]), 429

    if not _verify_turnstile():
        return _render(error=t["err_captcha"]), 400

    beaches = _sbh_beaches()
    beach_name = (request.form.get("beach_name") or "").strip()
    observed_risk = (request.form.get("observed_risk") or "").strip()
    sarg_location = (request.form.get("sarg_location") or "").strip() or None
    observed_at = _parse_observed_at(request.form.get("observed_at"))
    # Pas de champ « remarques » sur le formulaire anonyme : on ignore toute
    # valeur éventuellement bricolée dans la requête (réduction de surface).
    reporter_name = (request.form.get("reporter_name") or "").strip()[:40] or None

    coverage_raw = (request.form.get("coverage_pct") or "").strip()
    coverage = None
    if coverage_raw:
        try:
            coverage = max(0, min(100, int(float(coverage_raw))))
        except ValueError:
            coverage = None

    error = None
    if beach_name not in beaches:
        error = t["err_beach"]
    elif observed_risk not in RISK_KEYS:
        error = t["err_risk"]
    elif observed_at is None:
        error = t["err_date"]

    # « Où » : pertinent seulement s'il y a des sargasses. Optionnel côté serveur
    # (le JS le rend obligatoire à l'écran) ; on neutralise toute valeur hors
    # liste — jamais bloquer un signalement valide pour ce seul champ.
    if observed_risk == "none" or sarg_location not in LOCATION_KEYS:
        sarg_location = None

    # Photos optionnelles (validées seulement si le reste est valide).
    photos: list[str] = []
    if error is None:
        try:
            for fs in request.files.getlist("photos")[:MAX_PHOTOS]:
                p = _process_photo(fs)
                if p:
                    photos.append(p)
        except ValueError:
            error = t["err_photo"]

    if error:
        return _render(error=error), 400

    contributors_db.add_observation(
        contributor_id=contributors_db.anonymous_contributor_id(),
        observed_at=observed_at, island=ISLAND, beach_name=beach_name,
        observed_risk=observed_risk, coverage_pct=coverage,
        sarg_location=sarg_location, notes=None,
        client_ip=_client_ip(), photos=photos, reporter_name=reporter_name,
    )
    logger.info("Signalement DIRECT (anonyme) : %s / %s (risque %s, lieu %s, %d photo(s))",
                ISLAND, beach_name, observed_risk, sarg_location or "?", len(photos))
    return _render(done=True)


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
                           csrf_token=_ensure_csrf(), registered=True,
                           username=username)


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
    # Insensible à la casse : « samuel », « Samuel », « SAMUEL » → même compte.
    # C'était le piège n°1 qui bloquait les bénévoles.
    c = contributors_db.get_by_username_ci(username)

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


@contrib_bp.route("/mot-de-passe-oublie", methods=["GET", "POST"])
def forgot_password():
    """Mot de passe oublié — DEMANDE, jamais reset automatique.

    Sans canal de vérification d'identité (pas d'e-mail/SMS des bénévoles), un
    reset self-service serait une faille de vol de compte. Ici, la demande :
      1) prévient Sam sur Telegram + s'ajoute à la file du dashboard,
      2) affiche TOUJOURS le même message (anti-énumération).
    Sam réinitialise ensuite manuellement (mot de passe temporaire).
    """
    lang = _lang()
    t = get_strings(lang)
    if _current_contributor():
        return redirect(url_for("contrib.observer"))

    if request.method == "GET":
        return render_template("contrib_forgot.html", t=t, lang=lang,
                               csrf_token=_ensure_csrf())

    if not _rate_limit_ok(f"forgot:{_client_ip()}", FORGOT_MAX, FORGOT_WINDOW_S):
        return render_template("contrib_forgot.html", t=t, lang=lang,
                               csrf_token=_ensure_csrf(),
                               error=t["err_rate_forgot"]), 429

    identifier = (request.form.get("identifier") or "").strip()[:60]
    if identifier:
        accounts = contributors_db.find_accounts_for_reset(identifier)
        for acc in accounts:
            contributors_db.add_reset_request(acc["id"], client_ip=_client_ip())
        if accounts:
            who = ", ".join(f"{a['display_name']} ({a['username']})" for a in accounts)
            _notify_admin_telegram(
                "🔑 Sargasses — demande de mot de passe oublié\n"
                f"Compte(s) : {who}\n"
                "→ Réinitialise-le dans le dashboard, page « Contributeurs »."
            )
            logger.info("Demande reset : %d compte(s) pour identifiant saisi",
                        len(accounts))
        else:
            # Canal privé de Sam (pas d'énumération côté web) : on l'aide à
            # retrouver la personne qui s'est trompée d'identifiant.
            _notify_admin_telegram(
                "🔑 Sargasses — mot de passe oublié\n"
                f"Aucun compte ne correspond à « {identifier} ».\n"
                "Identifiant mal orthographié ? À vérifier avec la personne."
            )
            logger.info("Demande reset sans correspondance")

    # Même réponse dans tous les cas.
    return render_template("contrib_forgot.html", t=t, lang=lang,
                           csrf_token=_ensure_csrf(), done=True)


@contrib_bp.route("/changer-mot-de-passe", methods=["GET", "POST"])
def change_password():
    """Changement de mot de passe par le bénévole connecté.

    Sert aussi d'étape obligatoire après un reset admin (must_change_password) :
    le before_request _force_password_change redirige ici tant que ce n'est pas
    fait. Demande le mot de passe actuel (preuve de possession), puis applique le
    nouveau — ce qui invalide les autres sessions (cf. set_password_hash).
    """
    lang = _lang()
    t = get_strings(lang)
    c = _current_contributor()
    if not c:
        return redirect(url_for("contrib.login"))

    forced = bool(c.get("must_change_password"))

    if request.method == "GET":
        return render_template("contrib_change_pw.html", t=t, lang=lang,
                               contributor=c, csrf_token=_ensure_csrf(),
                               forced=forced)

    if not _rate_limit_ok(f"chpw:{c['id']}", CHPW_MAX, CHPW_WINDOW_S):
        return render_template("contrib_change_pw.html", t=t, lang=lang,
                               contributor=c, csrf_token=_ensure_csrf(),
                               forced=forced, error=t["err_rate_login"]), 429

    current = request.form.get("current_password") or ""
    new = request.form.get("new_password") or ""
    new2 = request.form.get("new_password2") or ""

    error = None
    if not check_password_hash(c["password_hash"], current):
        error = t["err_chpw_current"]
    elif len(new) < MIN_PASSWORD_LEN:
        error = t["err_pw_len"]
    elif new != new2:
        error = t["err_pw_match"]
    elif check_password_hash(c["password_hash"], new):
        error = t["err_pw_same"]

    if error:
        return render_template("contrib_change_pw.html", t=t, lang=lang,
                               contributor=c, csrf_token=_ensure_csrf(),
                               forced=forced, error=error), 400

    contributors_db.set_password_hash(
        c["id"], generate_password_hash(new), must_change=False)
    # La session courante reste valide : on resynchronise son empreinte (les
    # AUTRES sessions, elles, sont désormais invalides).
    _start_session(contributors_db.get_by_id(c["id"]))
    logger.info("Mot de passe changé pour le contributeur #%s", c["id"])
    return render_template("contrib_change_pw.html", t=t, lang=lang,
                           contributor=_current_contributor(),
                           csrf_token=_ensure_csrf(), forced=False, done=True)


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

    # Photos optionnelles (jusqu'à MAX_PHOTOS) : une plage ne tient pas dans un
    # seul cadre, et l'échouage est souvent localisé (cf. retours bénévoles).
    # Validées/ré-encodées seulement si le reste du formulaire est valide.
    photos: list[str] = []
    if error is None:
        try:
            for fs in request.files.getlist("photos")[:MAX_PHOTOS]:
                p = _process_photo(fs)
                if p:
                    photos.append(p)
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
        photos=photos,
    )
    logger.info("Signalement contributeur #%s : %s / %s (risque %s, %d photo(s))",
                c["id"], ISLAND, beach_name, observed_risk, len(photos))
    return redirect(url_for("contrib.observer", ok=1))


@contrib_bp.route("/photo/<int:obs_id>/<int:idx>")
def photo(obs_id: int, idx: int):
    """Sert une photo (par index) d'un signalement — à son auteur connecté.

    (Sam, lui, voit les photos directement dans le dashboard Streamlit qui lit
    les fichiers en local sur le serveur.)
    """
    c = _current_contributor()
    if not c:
        abort(404)  # 404 plutôt que 403 : ne révèle pas l'existence de la photo
    photos = contributors_db.get_owner_photos(obs_id, c["id"])
    if idx < 0 or idx >= len(photos):
        abort(404)
    # send_from_directory borne l'accès au dossier photos (anti-traversal)
    return send_from_directory(PHOTOS_DIR, Path(photos[idx]).name, max_age=3600)


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
