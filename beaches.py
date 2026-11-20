#!/usr/bin/env python3
"""
beaches.py
==========
Coordonnées GPS des principales plages des Antilles françaises
(Saint-Barthélemy, Martinique, Guadeloupe, Marie-Galante, Saint-Martin)
et calcul du risque d'échouage de sargasses par plage.

Le calcul s'appuie sur les snapshots de dérive (drift_predictions) produits
par sargassum_collector.py --simulate, qui utilisent les courants AVISO+ DUACS.

Scoring (deux échelles gaussiennes)
------------------------------------
  local_score    : score gaussien σ = radius_km  → arrivées imminentes
  regional_score : score gaussien σ = 50 km      → masses qui approchent
  closest_km     : distance à la particule la plus proche
  density_km2    : particules estimées par km² de la zone de catchment

  risk_level est dérivé du regional_score extrapolé à la population entière,
  ce qui le rend indépendant de la taille de l'échantillon (≤ 500 pts).

Usage
-----
  python beaches.py              # calcule et affiche les scores (dernière sim.)
  python beaches.py --report     # affiche uniquement le dernier rapport stocké
  python beaches.py --help
"""

import json
import math
import sqlite3
import sys
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

DB_PATH          = Path(__file__).parent / "sargassum_data.db"
DAY_OFFSETS      = [0, 1, 2, 3, 4, 5]
REGIONAL_SIGMA   = 50.0   # km — bandwidth pour le score d'approche

# Seuils sur regional_score (population extrapolée, σ = 50 km)
RISK_THRESHOLDS = {"low": 5.0, "medium": 25.0, "high": 75.0}

# Seuils sur local_score (présence réelle/imminente SUR la plage, σ = radius_km).
# Sert au badge public (« sur la plage »). Provisoires — à affiner avec le terrain.
LOCAL_THRESHOLDS = {"low": 0.5, "medium": 3.0, "high": 10.0}

# Correction des biais issus de calibration_spatial_bias
# Appliquée seulement si n_obs >= MIN_BIAS_NOBS (sinon biais trop bruité)
APPLY_BIAS_CORRECTION = True
MIN_BIAS_NOBS         = 3
MAX_BIAS_KM           = 60.0  # garde-fou : ignore biais > 60 km (vraisemblablement aberrants)

# Fenêtre d'exposition : une particule ÉCHOUÉE à moins de cette distance de la
# plage compte sans test de visibilité (OpenDrift la pose jusqu'à 2-3 km à
# l'intérieur des terres au pas de 3 h). Au-delà, c'est la côte d'une autre
# île : on teste comme pour une particule en mer.
STRANDED_EXEMPT_KM    = 3.0


# ── Plages — Antilles françaises ───────────────────────────────────────────────

BEACHES = [
    # ── Saint-Barthélemy ──────────────────────────────────────────────────────
    {"island": "Saint-Barth", "name": "Flamands",          "lat": 17.9067, "lon": -62.8467, "radius_km": 3.0},
    {"island": "Saint-Barth", "name": "Colombier",         "lat": 17.9033, "lon": -62.8600, "radius_km": 2.0},
    {"island": "Saint-Barth", "name": "Saint-Jean",        "lat": 17.9000, "lon": -62.8267, "radius_km": 4.0},
    {"island": "Saint-Barth", "name": "Lorient",           "lat": 17.9000, "lon": -62.8100, "radius_km": 3.0},
    {"island": "Saint-Barth", "name": "Grand_Cul-de-Sac",  "lat": 17.9117, "lon": -62.7917, "radius_km": 3.0},
    {"island": "Saint-Barth", "name": "Petit_Cul-de-Sac",  "lat": 17.9067, "lon": -62.7967, "radius_km": 2.0},
    {"island": "Saint-Barth", "name": "Toiny",             "lat": 17.8933, "lon": -62.7817, "radius_km": 2.0},
    {"island": "Saint-Barth", "name": "Gouverneur",        "lat": 17.8717, "lon": -62.8433, "radius_km": 3.0},
    {"island": "Saint-Barth", "name": "Grande_Saline",     "lat": 17.8717, "lon": -62.8267, "radius_km": 3.0},
    {"island": "Saint-Barth", "name": "Marigot",           "lat": 17.9033, "lon": -62.8067, "radius_km": 2.0},

    # ── Saint-Martin ──────────────────────────────────────────────────────────
    {"island": "Saint-Martin", "name": "Orient_Bay",         "lat": 18.0817, "lon": -63.0233, "radius_km": 4.0},
    {"island": "Saint-Martin", "name": "Grand_Case",         "lat": 18.1000, "lon": -63.0567, "radius_km": 3.0},
    {"island": "Saint-Martin", "name": "Friar_s_Bay",        "lat": 18.0733, "lon": -63.0767, "radius_km": 2.5},
    {"island": "Saint-Martin", "name": "Cupecoy",            "lat": 18.0433, "lon": -63.1233, "radius_km": 2.0},
    {"island": "Saint-Martin", "name": "Mullet_Bay",         "lat": 18.0483, "lon": -63.1183, "radius_km": 2.5},
    {"island": "Saint-Martin", "name": "Baie_Longue",        "lat": 18.0500, "lon": -63.1100, "radius_km": 3.0},
    {"island": "Saint-Martin", "name": "Anse_Marcel",        "lat": 18.1117, "lon": -63.0483, "radius_km": 2.0},
    {"island": "Saint-Martin", "name": "Cul_de_Sac",         "lat": 18.0967, "lon": -63.0100, "radius_km": 2.5},

    # ── Martinique ────────────────────────────────────────────────────────────
    {"island": "Martinique", "name": "Les_Salines",        "lat": 14.3917, "lon": -60.8617, "radius_km": 3.0},
    {"island": "Martinique", "name": "Grande_Anse",        "lat": 14.4867, "lon": -61.0867, "radius_km": 3.0},
    {"island": "Martinique", "name": "Anse_Noire",         "lat": 14.4817, "lon": -61.0783, "radius_km": 2.0},
    {"island": "Martinique", "name": "Anse_Mitan",         "lat": 14.5483, "lon": -61.0567, "radius_km": 2.5},
    {"island": "Martinique", "name": "Anse_a_l_Ane",       "lat": 14.5333, "lon": -61.0683, "radius_km": 2.0},
    {"island": "Martinique", "name": "Diamant",            "lat": 14.4667, "lon": -61.0233, "radius_km": 3.5},
    {"island": "Martinique", "name": "Tartane",            "lat": 14.7533, "lon": -60.8750, "radius_km": 2.5},
    {"island": "Martinique", "name": "Trinite",            "lat": 14.7383, "lon": -60.9700, "radius_km": 2.0},
    {"island": "Martinique", "name": "Le_Vauclin",         "lat": 14.5550, "lon": -60.8383, "radius_km": 2.5},
    {"island": "Martinique", "name": "Sainte-Luce",        "lat": 14.4717, "lon": -60.9217, "radius_km": 2.5},
    {"island": "Martinique", "name": "Le_Marin",           "lat": 14.4700, "lon": -60.8733, "radius_km": 2.0},
    {"island": "Martinique", "name": "Cap_Chevalier",      "lat": 14.4900, "lon": -60.8567, "radius_km": 2.0},
    {"island": "Martinique", "name": "Anse_Cafard",        "lat": 14.4433, "lon": -61.0533, "radius_km": 2.0},
    # Côte atlantique — zones les plus fréquentes en observations sargasses
    {"island": "Martinique", "name": "Le_Francois",        "lat": 14.6170, "lon": -60.9010, "radius_km": 3.5},
    {"island": "Martinique", "name": "Le_Robert",          "lat": 14.6817, "lon": -60.9433, "radius_km": 3.0},
    {"island": "Martinique", "name": "Tombolo_Ste-Marie",  "lat": 14.7780, "lon": -61.0000, "radius_km": 2.0},
    {"island": "Martinique", "name": "Anse_Michel",        "lat": 14.4600, "lon": -60.8800, "radius_km": 2.0},

    # ── Guadeloupe ────────────────────────────────────────────────────────────
    {"island": "Guadeloupe", "name": "Grande_Anse_Deshaies",  "lat": 16.3050, "lon": -61.7950, "radius_km": 2.5},
    {"island": "Guadeloupe", "name": "Malendure",             "lat": 16.1900, "lon": -61.7400, "radius_km": 2.0},
    {"island": "Guadeloupe", "name": "Caravelle_Ste-Anne",    "lat": 16.2233, "lon": -61.3817, "radius_km": 3.0},
    {"island": "Guadeloupe", "name": "Souffleur_Port-Louis",  "lat": 16.4183, "lon": -61.5400, "radius_km": 2.0},
    {"island": "Guadeloupe", "name": "Trois-Rivieres",        "lat": 15.9800, "lon": -61.6617, "radius_km": 2.0},
    {"island": "Guadeloupe", "name": "Saint-Francois",        "lat": 16.2533, "lon": -61.2767, "radius_km": 3.0},
    {"island": "Guadeloupe", "name": "Gosier",                "lat": 16.2067, "lon": -61.4967, "radius_km": 2.5},
    {"island": "Guadeloupe", "name": "Anse_Bertrand",         "lat": 16.4717, "lon": -61.5267, "radius_km": 2.0},
    {"island": "Guadeloupe", "name": "Anse_Bourg_Deshaies",   "lat": 16.3017, "lon": -61.7967, "radius_km": 1.5},
    {"island": "Guadeloupe", "name": "Plage_de_Viard",        "lat": 16.1083, "lon": -61.5933, "radius_km": 2.0},
    {"island": "Guadeloupe", "name": "Le_Moule",              "lat": 16.3300, "lon": -61.3533, "radius_km": 3.0},
    # Côte atlantique / nord — zones les plus fréquentes en observations sargasses
    {"island": "Guadeloupe", "name": "Porte_d_Enfer",         "lat": 16.5167, "lon": -61.4667, "radius_km": 2.5},
    {"island": "Guadeloupe", "name": "Souffleur_Desirade",    "lat": 16.3333, "lon": -61.0833, "radius_km": 2.5},
    {"island": "Guadeloupe", "name": "Anse_des_Rochers",      "lat": 16.2350, "lon": -61.2750, "radius_km": 2.0},
    {"island": "Guadeloupe", "name": "Raisins_Clairs",        "lat": 16.2533, "lon": -61.2750, "radius_km": 2.0},
    {"island": "Guadeloupe", "name": "Sainte-Anne_Galbas",    "lat": 16.2250, "lon": -61.4050, "radius_km": 2.5},
    {"island": "Guadeloupe", "name": "Bois_Jolan",            "lat": 16.2300, "lon": -61.3700, "radius_km": 2.0},

    # ── Marie-Galante ─────────────────────────────────────────────────────────
    {"island": "Marie-Galante", "name": "Capesterre",   "lat": 15.9217, "lon": -61.2033, "radius_km": 2.0},
    {"island": "Marie-Galante", "name": "Saint-Louis",  "lat": 15.9633, "lon": -61.2983, "radius_km": 2.0},
    {"island": "Marie-Galante", "name": "Grand-Bourg",  "lat": 15.8833, "lon": -61.3133, "radius_km": 2.5},
    {"island": "Marie-Galante", "name": "Anse_Ballet",  "lat": 15.9367, "lon": -61.3200, "radius_km": 2.0},
    {"island": "Marie-Galante", "name": "Anse_Canot",   "lat": 15.9683, "lon": -61.2733, "radius_km": 2.0},
]

# ── Override BEACHES depuis la DB (table beaches_config) ──────────────────────
# Si la table existe et n'est pas vide, on charge les plages depuis la DB
# au lieu de la liste hardcodee ci-dessus. La liste reste en fallback.
def _load_beaches_from_db() -> list | None:
    """Plages actives depuis beaches_config, avec leur fenêtre d'exposition
    (`exposure` : dict décodé de exposure_json, None si pas encore calculée).
    None si la table est absente/vide → l'appelant garde la liste hardcodée."""
    import beaches_db as _beaches_db
    if _beaches_db.is_table_empty():
        return None
    _db_beaches = _beaches_db.list_all(only_active=True)
    if not _db_beaches:
        return None
    out = []
    for b in _db_beaches:
        exposure = None
        if b.get('exposure_json'):
            try:
                exposure = json.loads(b['exposure_json'])
            except (json.JSONDecodeError, TypeError):
                exposure = None
        # Garde le format attendu par le code (island, name, lat, lon, radius_km)
        out.append({
            'island':    b['island'],
            'name':      b['name'],
            'lat':       b['lat'],
            'lon':       b['lon'],
            'radius_km': b['radius_km'],
            'exposure':  exposure,
        })
    return out


try:
    BEACHES = _load_beaches_from_db() or BEACHES
except Exception as _e:
    # En cas d'erreur (table absente, etc.), garde la liste hardcodee
    import sys
    print(f'  [WARN] Fallback liste hardcodee : {_e}', file=sys.stderr)



# ── Géographie ────────────────────────────────────────────────────────────────

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance en km entre deux points GPS (formule haversine)."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def _deg_per_km_at(lat: float) -> tuple[float, float]:
    """Conversion km → degrés à la latitude donnée (lat, lon)."""
    deg_lat_per_km = 1.0 / 111.0
    deg_lon_per_km = 1.0 / (111.0 * math.cos(math.radians(lat)))
    return deg_lat_per_km, deg_lon_per_km


# ── Calibration : biais directionnels ─────────────────────────────────────────

def _load_latest_biases(conn: sqlite3.Connection) -> dict:
    """Charge les biais (Δlat_km, Δlon_km) les plus récents par
    (island, month, day_offset). Retourne un dict pour lookup rapide."""
    biases: dict[tuple[str, int, int], tuple[float, float, int]] = {}
    try:
        rows = conn.execute("""
            SELECT b.island, b.month, b.day_offset,
                   b.mean_delta_lat_km, b.mean_delta_lon_km, b.n_obs
            FROM calibration_spatial_bias b
            INNER JOIN (
                SELECT island, month, day_offset, MAX(computed_at) AS max_at
                FROM calibration_spatial_bias
                GROUP BY island, month, day_offset
            ) latest
              ON b.island = latest.island
             AND b.month = latest.month
             AND b.day_offset = latest.day_offset
             AND b.computed_at = latest.max_at
        """).fetchall()
    except sqlite3.OperationalError:
        return biases  # table pas encore créée

    for r in rows:
        if r["mean_delta_lat_km"] is None or r["mean_delta_lon_km"] is None:
            continue
        biases[(r["island"], r["month"], r["day_offset"])] = (
            r["mean_delta_lat_km"], r["mean_delta_lon_km"], r["n_obs"]
        )
    return biases


def _bias_for(biases: dict, island: str, month: int, day_offset: int):
    """Retourne (Δlat_km, Δlon_km) si applicable, sinon None.
    Filtre par MIN_BIAS_NOBS et MAX_BIAS_KM. Fallback : mois courant,
    sinon M-1, sinon M-2 (les régimes alizés sont quasi-stables sur 2-3 mois)."""
    for m_try in (month, ((month - 2) % 12) + 1, ((month - 3) % 12) + 1):
        key = (island, m_try, day_offset)
        if key not in biases:
            continue
        dlat_km, dlon_km, n_obs = biases[key]
        if n_obs < MIN_BIAS_NOBS:
            continue
        if abs(dlat_km) > MAX_BIAS_KM or abs(dlon_km) > MAX_BIAS_KM:
            continue
        return (dlat_km, dlon_km)
    return None


# ── Scoring ────────────────────────────────────────────────────────────────────

def _score_beach(
    positions: list,
    beach_lat: float,
    beach_lon: float,
    radius_km: float,
    ratio: float,
    positions_dense: list | None = None,
    exposure: dict | None = None,
) -> dict:
    """Scores d'une plage pour un jeu de positions.

    exposure : fenêtre d'exposition (sarga_exposure.py). Si fournie, le score
    LOCAL et est_count ne comptent, parmi les particules EN MER, que celles
    « visibles par la mer » depuis la plage (pas de terre entre elles et la
    plage) ; le score brut sans fenêtre est conservé dans local_score_raw.
    Les particules ÉCHOUÉES (flag pt[2] == 1) à moins de STRANDED_EXEMPT_KM ne
    sont pas testées : OpenDrift les pose au premier pas de 3 h où elles sont à
    terre, donc jusqu'à 2-3 km à l'intérieur de l'île — un test de visibilité
    les cacherait toujours. Au-delà (côte d'une autre île), elles sont testées.
    Le score RÉGIONAL (approche à 50 km, alertes) n'est PAS filtré ; on calcule
    en plus regional_exposed = la part de cette masse régionale qui fait FACE à
    la plage par la mer, et exposed_frac = regional_exposed / regional_score.
    C'est ce qui distingue enfin deux plages d'une même île."""
    sample_count    = 0
    sample_hidden   = 0      # en mer, dans radius_km, mais cachées par la terre
    n_stranded      = 0      # particules ÉCHOUÉES (flag pt[2] == 1) dans radius_km
    local_gauss_sum = 0.0    # pondéré par la fenêtre
    local_raw_sum   = 0.0    # sans fenêtre
    reg_gauss_sum   = 0.0
    reg_exposed_sum = 0.0    # régional restreint aux particules visibles par la mer
    min_dist        = math.inf

    # Origine des rayons de la fenêtre : l'« œil » 300 m au large, sinon le point
    # de côte, sinon le point catalogue.
    if exposure and exposure.get("eye_lat") is not None:
        eye_lat, eye_lon = exposure["eye_lat"], exposure["eye_lon"]
    elif exposure and exposure.get("snap_lat") is not None:
        eye_lat, eye_lon = exposure["snap_lat"], exposure["snap_lon"]
    else:
        eye_lat, eye_lon = beach_lat, beach_lon

    for pt in positions:
        if len(pt) < 2:
            continue
        plon, plat = float(pt[0]), float(pt[1])
        stranded = len(pt) >= 3 and bool(pt[2])
        d = haversine_km(beach_lat, beach_lon, plat, plon)

        if d < min_dist:
            min_dist = d

        g_local = math.exp(-0.5 * (d / radius_km) ** 2)
        g_reg   = math.exp(-0.5 * (d / REGIONAL_SIGMA) ** 2)
        local_raw_sum += g_local
        reg_gauss_sum += g_reg

        visible = True
        if exposure and g_reg > 1e-4 and not (stranded and d <= STRANDED_EXEMPT_KM):
            d_eye = haversine_km(eye_lat, eye_lon, plat, plon)
            visible = _visible_by_sea(exposure, _bearing_deg(eye_lat, eye_lon, plat, plon), d_eye)
        if visible:
            local_gauss_sum += g_local
            reg_exposed_sum += g_reg

        if d <= radius_km:
            if visible:
                sample_count += 1
                if stranded:
                    n_stranded += 1
            else:
                sample_hidden += 1

    est_count        = round(sample_count * ratio, 2)
    local_score      = round(local_gauss_sum * ratio, 3)
    local_score_raw  = round(local_raw_sum   * ratio, 3)
    regional_score   = round(reg_gauss_sum   * ratio, 3)
    regional_exposed = round(reg_exposed_sum * ratio, 3)
    exposed_frac     = (round(reg_exposed_sum / reg_gauss_sum, 3)
                        if exposure and reg_gauss_sum > 1e-9 else None)
    # closest_km : distance à la particule la plus proche. Le jeu UNIFORME
    # (~500 pts sur tout le bassin) place rarement un point près d'une petite
    # île → ce min est fortement surestimé (toutes les plages ressortaient à
    # ~95 km simultanément). On mesure donc la distance sur le jeu DENSE
    # régional quand il est fourni ; sinon on retombe sur l'uniforme.
    # Les scores gaussiens, eux, restent sur l'uniforme : leur calibration par
    # `ratio = n_active/n_sample` suppose un échantillonnage uniforme.
    if positions_dense:
        dmin = math.inf
        for pt in positions_dense:
            if len(pt) < 2:
                continue
            d = haversine_km(beach_lat, beach_lon, float(pt[1]), float(pt[0]))
            if d < dmin:
                dmin = d
        if math.isfinite(dmin):
            min_dist = dmin
    closest_km     = round(min_dist, 2) if math.isfinite(min_dist) else None
    catchment_area = math.pi * radius_km ** 2
    density_km2    = round(est_count / catchment_area, 6) if catchment_area > 0 else 0.0

    return {
        "sample_count":    sample_count,
        "sample_hidden":   sample_hidden,
        "n_stranded":      n_stranded,
        "est_count":       est_count,
        "local_score":     local_score,
        "local_score_raw": local_score_raw,
        "regional_score":  regional_score,
        "regional_exposed": regional_exposed,
        "exposed_frac":    exposed_frac,
        "closest_km":      closest_km,
        "density_km2":     density_km2,
    }


# ── Fenêtre d'exposition (géométrie du dernier km) ────────────────────────────
# Copies locales minimales de sarga_exposure (pour ne pas en dépendre à l'import).

def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Azimut géographique de 1 vers 2 : 0 = nord, 90 = est."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def _visible_by_sea(exposure: dict | None, bearing: float, dist_km: float) -> bool:
    """True si une particule à (bearing, dist_km) atteint la plage par la mer.

    Lit la fenêtre calculée par sarga_exposure.py (eau libre par secteur de 5°,
    plafond max_km). Tolérance ±1 secteur. Sans fenêtre → True (pas de filtre)."""
    if not exposure or not exposure.get("open_km"):
        return True
    open_km = exposure["open_km"]
    step = exposure.get("step_deg", 5)
    n = len(open_km)
    i = int(round(bearing / step)) % n
    reach = max(open_km[(i - 1) % n], open_km[i], open_km[(i + 1) % n])
    cap = exposure.get("max_km", 40.0)
    return reach >= min(dist_km, cap - 1e-6)


def _decode_region(blob) -> list | None:
    """Décode le jeu régional complet stocké par le collecteur
    (positions_region_blob : JSON [[lon,lat,flag],…] compressé zlib).

    Retourne None si la colonne est absente/vide (anciennes simulations) :
    l'appelant retombe alors sur l'échantillon uniforme extrapolé."""
    if not blob:
        return None
    try:
        return json.loads(zlib.decompress(blob).decode())
    except (zlib.error, json.JSONDecodeError, UnicodeDecodeError, TypeError):
        return None


def risk_label(regional_score: float) -> str:
    if regional_score >= RISK_THRESHOLDS["high"]:
        return "high"
    if regional_score >= RISK_THRESHOLDS["medium"]:
        return "medium"
    if regional_score >= RISK_THRESHOLDS["low"]:
        return "low"
    return "none"


def presence_label(local_score: float) -> str:
    """Badge « sur la plage » dérivé du local_score (présence réelle/imminente).

    Distinct de risk_label (régional, σ=50 km) qui reste le signal d'approche
    utilisé par les alertes Telegram, la timeline et la calibration.
    """
    if local_score is None:
        return "none"
    if local_score >= LOCAL_THRESHOLDS["high"]:
        return "high"
    if local_score >= LOCAL_THRESHOLDS["medium"]:
        return "medium"
    if local_score >= LOCAL_THRESHOLDS["low"]:
        return "low"
    return "none"


def display_badge(presence: str, regional: str, trend_state: str) -> str:
    """Badge public FINAL = présence, avec un filet de sécurité.

    Si une masse est dans la zone (régional >= moyen) sans être encore sur la
    plage (présence <= faible) ET qu'elle se RAPPROCHE → 'watch' (Surveiller),
    pour ne jamais afficher un vert rassurant juste avant une arrivée.
    Si elle s'éloigne/est stable, on garde la présence (vert quand c'est propre).
    """
    rank = {"none": 0, "low": 1, "medium": 2, "high": 3}
    offshore = rank.get(regional, 0) >= 2 and rank.get(presence, 0) <= 1
    if offshore and trend_state == "approach":
        return "watch"
    return presence


def _shift_positions(positions: list, dlat_km: float, dlon_km: float,
                     ref_lat: float) -> list:
    """Applique la correction de biais (sim − obs) à un jeu de positions."""
    deg_lat_per_km, deg_lon_per_km = _deg_per_km_at(ref_lat)
    shift_lon = -dlon_km * deg_lon_per_km
    shift_lat = -dlat_km * deg_lat_per_km
    # pt[2:] conserve le flag échouage éventuel (jeu régional complet)
    return [
        [pt[0] + shift_lon, pt[1] + shift_lat, *pt[2:]]
        for pt in positions if len(pt) >= 2
    ]


def _score_all_beaches(
    positions: list,
    n_active: int,
    n_sample: int,
    biases: dict,
    pred_month: int,
    day_offset: int,
    positions_dense: list | None = None,
    ratio_override: float | None = None,
):
    """Score toutes les plages pour un snapshot donné, biais appliqué par île.

    Retourne [(beach_dict, island, score_dict), …]. Mutualisé entre le scoring
    journalier (beach_risk_scores) et la timeline 3h (beach_timeline).

    positions_dense : échantillon dense régional, utilisé UNIQUEMENT pour le
    closest_km (voir _score_beach). Corrigé du même biais que le jeu uniforme.
    ratio_override  : 1.0 quand `positions` est le jeu régional COMPLET (toutes
    les particules, pas d'extrapolation) ; None = ancien mode échantillon."""
    if ratio_override is not None:
        ratio = ratio_override
    else:
        ratio = n_active / n_sample if n_sample > 0 else 0.0
    corrected_cache: dict[str, list] = {}
    dense_cache: dict[str, list] = {}
    out = []
    for beach in BEACHES:
        island = beach.get("island", "")
        bias = _bias_for(biases, island, pred_month, day_offset) if biases else None
        if bias and positions:
            dlat_km, dlon_km = bias
            if island not in corrected_cache:
                corrected_cache[island] = _shift_positions(
                    positions, dlat_km, dlon_km, beach["lat"])
                if positions_dense:
                    dense_cache[island] = _shift_positions(
                        positions_dense, dlat_km, dlon_km, beach["lat"])
            positions_to_use = corrected_cache[island]
            dense_to_use = dense_cache.get(island) if positions_dense else None
        else:
            positions_to_use = positions
            dense_to_use = positions_dense
        s = _score_beach(positions_to_use, beach["lat"], beach["lon"],
                         beach["radius_km"], ratio, dense_to_use,
                         beach.get("exposure"))
        out.append((beach, island, s))
    return out


# ── Base de données ───────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS beach_risk_scores (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at    TEXT    NOT NULL,
    simulated_at   TEXT    NOT NULL,
    island         TEXT,
    beach_name     TEXT    NOT NULL,
    beach_lat      REAL    NOT NULL,
    beach_lon      REAL    NOT NULL,
    radius_km      REAL    NOT NULL,
    day_offset     INTEGER NOT NULL,
    sample_count   INTEGER NOT NULL,
    n_sample       INTEGER,
    n_active       INTEGER,
    n_particles    INTEGER,
    est_count      REAL,
    local_score    REAL,
    regional_score REAL,
    closest_km     REAL,
    density_km2    REAL,
    risk_level     TEXT    NOT NULL
);
"""

_NEW_COLUMNS = [
    ("island",         "TEXT"),
    ("local_score",    "REAL"),
    ("regional_score", "REAL"),
    ("closest_km",     "REAL"),
    ("density_km2",    "REAL"),
    ("n_stranded",     "INTEGER"),   # particules échouées dans radius_km (2026-11)
    ("local_score_raw", "REAL"),     # score local SANS fenêtre d'exposition (2026-11)
    ("sample_hidden",  "INTEGER"),   # particules dans le rayon mais cachées par la terre
    ("regional_exposed", "REAL"),    # part du régional qui fait face à la plage par la mer
    ("exposed_frac",   "REAL"),      # regional_exposed / regional_score (0..1)
]

# Colonnes ajoutées à forecast_archive après sa création (migration douce)
_ARCHIVE_NEW_COLUMNS = [
    ("local_score_raw", "REAL"),
    ("presence_raw",    "TEXT"),     # badge qu'on aurait affiché SANS la fenêtre
    ("regional_exposed", "REAL"),
    ("exposed_frac",    "REAL"),
    # Masque côtier des points de départ de la simulation utilisée (km, 0 = aucun,
    # NULL = inconnu). Sert à ne pas mélanger les régimes dans la vérification.
    ("seed_coast_km",   "REAL"),
    ("seed_dated",      "INTEGER"),
]

# Archive des prévisions, JAMAIS purgée : c'est la matière première de la
# vérification (sarga_verification.py). Sans elle, on ne pouvait pas comparer
# ce qu'on avait prévu à ce que les bénévoles ont observé (beach_risk_scores
# est purgé à 60 runs, soit ~2 semaines). ~1 400 lignes/jour, léger.
_ARCHIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS forecast_archive (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at    TEXT    NOT NULL,   -- quand la prévision a été calculée
    simulated_at   TEXT    NOT NULL,
    island         TEXT,
    beach_name     TEXT    NOT NULL,
    day_offset     INTEGER NOT NULL,   -- échéance (j+0 … j+5)
    target_date    TEXT    NOT NULL,   -- jour visé (YYYY-MM-DD, UTC)
    est_count      REAL,
    local_score    REAL,
    regional_score REAL,
    closest_km     REAL,
    n_stranded     INTEGER,
    risk_level     TEXT,               -- régional (approche)
    presence       TEXT,               -- local (sur la plage)
    scoring_mode   TEXT                -- 'region' (jeu complet) | 'sample' (ancien)
);
CREATE INDEX IF NOT EXISTS idx_archive_lookup
    ON forecast_archive (beach_name, target_date, day_offset);
"""


_TIMELINE_SCHEMA = """
CREATE TABLE IF NOT EXISTS beach_timeline (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at    TEXT    NOT NULL,
    simulated_at   TEXT    NOT NULL,
    island         TEXT,
    beach_name     TEXT    NOT NULL,
    beach_lat      REAL    NOT NULL,
    beach_lon      REAL    NOT NULL,
    radius_km      REAL,
    hour_offset    INTEGER NOT NULL,   -- heures depuis t0 (0,3,…,120)
    day_offset     INTEGER NOT NULL,   -- hour_offset // 24
    valid_time     TEXT,               -- t0 + hour_offset (UTC ISO)
    est_count      REAL,
    local_score    REAL,
    regional_score REAL,
    closest_km     REAL,
    risk_level     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_timeline_lookup
    ON beach_timeline (island, beach_name, computed_at, hour_offset);
"""

# Nb de runs (computed_at) de timeline conservés (41 pas × ~58 plages / run)
TIMELINE_KEEP_RUNS = 30


def _get_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(_SCHEMA)
    conn.executescript(_TIMELINE_SCHEMA)
    conn.executescript(_ARCHIVE_SCHEMA)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(beach_risk_scores)")}
    for col, typedef in _NEW_COLUMNS:
        if col not in existing:
            conn.execute(f"ALTER TABLE beach_risk_scores ADD COLUMN {col} {typedef}")
    existing_tl = {row[1] for row in conn.execute("PRAGMA table_info(beach_timeline)")}
    if "n_stranded" not in existing_tl:
        conn.execute("ALTER TABLE beach_timeline ADD COLUMN n_stranded INTEGER")
    existing_ar = {row[1] for row in conn.execute("PRAGMA table_info(forecast_archive)")}
    for col, typedef in _ARCHIVE_NEW_COLUMNS:
        if col not in existing_ar:
            conn.execute(f"ALTER TABLE forecast_archive ADD COLUMN {col} {typedef}")
    conn.commit()
    conn.row_factory = sqlite3.Row
    return conn


# ── Calcul des scores ─────────────────────────────────────────────────────────

def _seed_coast_km(raw_metadata: str | None) -> float | None:
    """Masque côtier noté par le collecteur dans raw_metadata (None si absent ou illisible)."""
    try:
        v = json.loads(raw_metadata or "{}").get("seed_coast_km")
        return float(v) if v is not None else None
    except (ValueError, TypeError, AttributeError):
        return None


def _seed_dated(raw_metadata: str | None) -> int | None:
    """Semis daté noté par le collecteur dans raw_metadata : 1, 0, ou None si absent ou illisible."""
    try:
        v = json.loads(raw_metadata or "{}").get("seed_dated")
        return int(bool(v)) if isinstance(v, bool) else None
    except (ValueError, TypeError, AttributeError):
        return None


def compute_beach_scores(db_path: Path = DB_PATH) -> int:
    conn = _get_conn(db_path)

    row = conn.execute(
        "SELECT MAX(simulated_at) AS max_sim FROM drift_predictions"
    ).fetchone()
    if not row or not row["max_sim"]:
        print("  ⚠️  Aucune simulation de dérive dans la base.")
        print("       Lancez : python sargassum_collector.py --simulate")
        conn.close()
        return 0
    simulated_at = row["max_sim"]

    placeholders = ",".join("?" * len(DAY_OFFSETS))
    # Scoring journalier : ne prendre que les snapshots de bord de journée
    # (hour_offset % 24 == 0). hour_offset IS NULL = anciennes sim avant la 3h.
    has_region = "positions_region_blob" in {
        row[1] for row in conn.execute("PRAGMA table_info(drift_predictions)")
    }
    region_col = "positions_region_blob" if has_region else "NULL AS positions_region_blob"
    snaps = conn.execute(
        f"""SELECT day_offset, positions_json, positions_viz_json,
                   n_particles, active_fraction, {region_col}, raw_metadata
            FROM drift_predictions
            WHERE simulated_at = ? AND day_offset IN ({placeholders})
              AND (hour_offset IS NULL OR hour_offset % 24 = 0)
            ORDER BY day_offset""",
        (simulated_at, *DAY_OFFSETS),
    ).fetchall()

    if not snaps:
        print(f"  ⚠️  Aucun snapshot pour j+{DAY_OFFSETS} dans la simulation {simulated_at}.")
        conn.close()
        return 0

    computed_at    = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows_to_insert = []
    archive_rows   = []
    seed_coast_km  = _seed_coast_km(snaps[0]["raw_metadata"])
    if seed_coast_km is not None:
        print(f"  🧹 Masque côtier des points de départ : "
              + (f"{seed_coast_km:g} km" if seed_coast_km > 0 else "aucun"))
    seed_dated     = _seed_dated(snaps[0]["raw_metadata"])
    if seed_dated:
        print("  🕒 Semis daté : points de départ transportés jusqu'à l'heure de la simulation")

    biases = _load_latest_biases(conn) if APPLY_BIAS_CORRECTION else {}
    sim_dt = datetime.fromisoformat(simulated_at.replace("Z", "+00:00"))
    bias_hits = 0  # combien de (snap × beach) ont reçu la correction
    modes_used: set[str] = set()

    for snap in snaps:
        day         = snap["day_offset"]
        n_particles = snap["n_particles"] or 0
        act_frac    = snap["active_fraction"] or 0.0
        n_active    = int(round(n_particles * act_frac))

        # Jeu RÉGIONAL COMPLET (depuis 2026-11-09) : toutes les particules de la
        # boîte Antilles, actives et échouées → scoring exact, ratio = 1.
        # Avant : 500 pts uniformes sur TOUT le bassin ×12 → presque jamais un
        # point à < 2 km d'une plage, local_score structurellement ~0.
        region = _decode_region(snap["positions_region_blob"])
        if region is not None:
            positions       = region
            positions_dense = None          # le jeu complet EST le plus dense
            n_sample        = len(region)
            ratio           = 1.0
            scoring_mode    = "region"
        else:
            try:
                positions = json.loads(snap["positions_json"] or "[]")
            except (json.JSONDecodeError, TypeError):
                positions = []

            # Jeu dense régional : sert UNIQUEMENT au closest_km (l'uniforme est
            # trop clairsemé près des petites îles). Retombe sur l'uniforme si absent.
            try:
                positions_dense = json.loads(snap["positions_viz_json"] or "[]")
            except (json.JSONDecodeError, TypeError):
                positions_dense = []
            if not positions_dense:
                positions_dense = positions

            n_sample     = len(positions)
            ratio        = n_active / n_sample if n_sample > 0 else 0.0
            scoring_mode = "sample"
        modes_used.add(scoring_mode)
        target_date = (sim_dt + timedelta(days=day)).strftime("%Y-%m-%d")

        # Mois de la date prédite (utilisé pour lookup du biais)
        pred_month = (sim_dt + timedelta(days=day)).month

        # Cache des positions corrigées par île (1 calcul par île × snap)
        corrected_cache: dict[str, list] = {}
        dense_cache: dict[str, list] = {}

        for beach in BEACHES:
            island = beach.get("island", "")
            bias = _bias_for(biases, island, pred_month, day) if biases else None

            if bias and positions:
                dlat_km, dlon_km = bias
                if island not in corrected_cache:
                    # delta_*_km = sim - obs ; corriger = soustraire delta des positions
                    corrected_cache[island] = _shift_positions(
                        positions, dlat_km, dlon_km, beach["lat"])
                    if positions_dense:
                        dense_cache[island] = _shift_positions(
                            positions_dense, dlat_km, dlon_km, beach["lat"])
                positions_to_use = corrected_cache[island]
                dense_to_use = dense_cache.get(island) if positions_dense else None
                bias_hits += 1
            else:
                positions_to_use = positions
                dense_to_use = positions_dense

            s = _score_beach(positions_to_use, beach["lat"], beach["lon"],
                             beach["radius_km"], ratio, dense_to_use,
                             beach.get("exposure"))
            level = risk_label(s["regional_score"])
            rows_to_insert.append((
                computed_at, simulated_at,
                island,
                beach["name"], beach["lat"], beach["lon"], beach["radius_km"],
                day,
                s["sample_count"], n_sample, n_active, n_particles,
                s["est_count"], s["local_score"], s["regional_score"],
                s["closest_km"], s["density_km2"],
                level, s["n_stranded"], s["local_score_raw"], s["sample_hidden"],
                s["regional_exposed"], s["exposed_frac"],
            ))
            archive_rows.append((
                computed_at, simulated_at, island, beach["name"], day, target_date,
                s["est_count"], s["local_score"], s["regional_score"],
                s["closest_km"], s["n_stranded"],
                level, presence_label(s["local_score"]), scoring_mode,
                s["local_score_raw"], presence_label(s["local_score_raw"]),
                s["regional_exposed"], s["exposed_frac"], seed_coast_km, seed_dated,
            ))

    if APPLY_BIAS_CORRECTION:
        total = len(rows_to_insert)
        print(f"  🎯 Correction biais appliquée à {bias_hits}/{total} scores "
              f"(seuil n_obs ≥ {MIN_BIAS_NOBS}, |Δ| ≤ {MAX_BIAS_KM} km)")
    print(f"  📐 Mode de scoring : {', '.join(sorted(modes_used))}"
          + ("  (jeu régional complet, échouées incluses)" if "region" in modes_used else
             "  (échantillon uniforme extrapolé — ancienne simulation)"))

    conn.executemany(
        """INSERT INTO beach_risk_scores
           (computed_at, simulated_at, island,
            beach_name, beach_lat, beach_lon,
            radius_km, day_offset, sample_count, n_sample, n_active,
            n_particles, est_count, local_score, regional_score,
            closest_km, density_km2, risk_level, n_stranded, local_score_raw, sample_hidden,
            regional_exposed, exposed_frac)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows_to_insert,
    )
    # Archive permanente (pas de purge) pour la vérification a posteriori
    conn.executemany(
        """INSERT INTO forecast_archive
           (computed_at, simulated_at, island, beach_name, day_offset, target_date,
            est_count, local_score, regional_score, closest_km, n_stranded,
            risk_level, presence, scoring_mode, local_score_raw, presence_raw,
            regional_exposed, exposed_frac, seed_coast_km, seed_dated)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        archive_rows,
    )
    conn.commit()

    # Purge : conserver uniquement les 60 derniers computed_at distincts
    conn.execute("""
        DELETE FROM beach_risk_scores
        WHERE computed_at NOT IN (
            SELECT DISTINCT computed_at FROM beach_risk_scores
            ORDER BY computed_at DESC LIMIT 60
        )
    """)
    conn.commit()

    conn.close()
    return len(rows_to_insert)


def compute_beach_timeline(db_path: Path = DB_PATH) -> int:
    """Scoring fin (toutes les 3h) → table beach_timeline.

    Lit TOUS les snapshots de la dernière simulation (hour_offset 0,3,…,120)
    et calcule le risque par plage à chaque pas, pour alimenter la timeline
    horaire du site ("heure d'arrivée prévue par plage"). N'altère pas
    beach_risk_scores (chemin journalier des alertes/calibration)."""
    conn = _get_conn(db_path)

    row = conn.execute(
        "SELECT MAX(simulated_at) AS max_sim FROM drift_predictions"
    ).fetchone()
    if not row or not row["max_sim"]:
        conn.close()
        return 0
    simulated_at = row["max_sim"]

    has_region = "positions_region_blob" in {
        row[1] for row in conn.execute("PRAGMA table_info(drift_predictions)")
    }
    region_col = "positions_region_blob" if has_region else "NULL AS positions_region_blob"
    snaps = conn.execute(
        f"""SELECT hour_offset, day_offset, sim_start, positions_json,
                  positions_viz_json, n_particles, active_fraction, {region_col}
           FROM drift_predictions
           WHERE simulated_at = ? AND hour_offset IS NOT NULL
           ORDER BY hour_offset""",
        (simulated_at,),
    ).fetchall()

    if not snaps:
        # Aucune sim 3h disponible (base antérieure à la migration) — pas d'erreur.
        conn.close()
        return 0

    computed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    biases = _load_latest_biases(conn) if APPLY_BIAS_CORRECTION else {}
    sim_dt = datetime.fromisoformat(simulated_at.replace("Z", "+00:00"))

    rows_to_insert = []
    for snap in snaps:
        hour        = snap["hour_offset"]
        day         = snap["day_offset"] if snap["day_offset"] is not None else hour // 24
        n_particles = snap["n_particles"] or 0
        act_frac    = snap["active_fraction"] or 0.0
        n_active    = int(round(n_particles * act_frac))

        try:
            positions = json.loads(snap["positions_json"] or "[]")
        except (json.JSONDecodeError, TypeError):
            positions = []
        n_sample = len(positions)

        # Jeu dense régional pour le closest_km (cf. compute_beach_scores).
        try:
            positions_dense = json.loads(snap["positions_viz_json"] or "[]")
        except (json.JSONDecodeError, TypeError):
            positions_dense = []
        if not positions_dense:
            positions_dense = positions

        # Jeu régional COMPLET si disponible (cf. compute_beach_scores) :
        # il remplace l'échantillon ET le jeu dense, ratio = 1.
        ratio_override = None
        region = _decode_region(snap["positions_region_blob"]) if has_region else None
        if region is not None:
            positions, positions_dense = region, None
            n_sample, ratio_override = len(region), 1.0

        pred_month = (sim_dt + timedelta(hours=hour)).month

        # valid_time = t0 (sim_start) + hour_offset
        valid_time = None
        if snap["sim_start"]:
            try:
                t0 = datetime.fromisoformat(snap["sim_start"].replace("Z", "+00:00"))
                if t0.tzinfo is None:
                    t0 = t0.replace(tzinfo=timezone.utc)
                valid_time = (t0 + timedelta(hours=hour)).strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                valid_time = None

        for beach, island, s in _score_all_beaches(
            positions, n_active, n_sample, biases, pred_month, day,
            positions_dense, ratio_override,
        ):
            rows_to_insert.append((
                computed_at, simulated_at, island,
                beach["name"], beach["lat"], beach["lon"], beach["radius_km"],
                hour, day, valid_time,
                s["est_count"], s["local_score"], s["regional_score"],
                s["closest_km"], risk_label(s["regional_score"]), s["n_stranded"],
            ))

    conn.executemany(
        """INSERT INTO beach_timeline
           (computed_at, simulated_at, island, beach_name, beach_lat, beach_lon,
            radius_km, hour_offset, day_offset, valid_time,
            est_count, local_score, regional_score, closest_km, risk_level, n_stranded)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows_to_insert,
    )
    conn.commit()

    # Purge : conserver les TIMELINE_KEEP_RUNS derniers computed_at
    conn.execute(
        """DELETE FROM beach_timeline
           WHERE computed_at NOT IN (
               SELECT DISTINCT computed_at FROM beach_timeline
               ORDER BY computed_at DESC LIMIT ?
           )""",
        (TIMELINE_KEEP_RUNS,),
    )
    conn.commit()

    conn.close()
    return len(rows_to_insert)


# ── Rapport ───────────────────────────────────────────────────────────────────

def print_report(db_path: Path = DB_PATH) -> None:
    conn = _get_conn(db_path)

    row = conn.execute(
        "SELECT MAX(computed_at) AS last FROM beach_risk_scores"
    ).fetchone()
    if not row or not row["last"]:
        print("Aucun score disponible. Lancez : python beaches.py")
        conn.close()
        return
    last = row["last"]

    scores = conn.execute(
        """SELECT island, beach_name, day_offset, sample_count, est_count,
                  local_score, regional_score, closest_km,
                  density_km2, risk_level, radius_km, n_active, n_particles, n_sample,
                  n_stranded, exposed_frac
           FROM beach_risk_scores
           WHERE computed_at = ?
           ORDER BY island, beach_name, day_offset""",
        (last,),
    ).fetchall()
    mode_row = conn.execute(
        "SELECT scoring_mode FROM forecast_archive WHERE computed_at = ? LIMIT 1", (last,)
    ).fetchone()
    scoring_mode = mode_row["scoring_mode"] if mode_row else "sample"
    conn.close()

    if not scores:
        print("Aucun score à afficher.")
        return

    ICONS = {"none": "🟢", "low": "🟡", "medium": "🟠", "high": "🔴"}
    days  = sorted({r["day_offset"] for r in scores})

    # Grouper par île
    by_island: dict[str, dict[str, list]] = {}
    for r in scores:
        island = r["island"] or "?"
        by_island.setdefault(island, {})
        by_island[island].setdefault(r["beach_name"], []).append(r)

    r0 = scores[0]
    ratio = (r0["n_active"] or 0) / (r0["n_sample"] or 1) if r0["n_sample"] else 0

    for island, beaches in by_island.items():
        print(f"\n{'═'*72}")
        print(f"  🏖️  Risque sargasses — {island}  (calculé {last})")
        print(f"{'═'*72}")

        header = f"{'Plage':<22}" + "".join(f"  {'j+'+str(d):<26}" for d in days)
        sub    = f"{'':22}" + "".join(f"  {'rég face / loc / prox ⚓':26}" for _ in days)
        print(header)
        print(sub)
        print("─" * len(header))

        for beach_name, beach_scores in beaches.items():
            line = f"{beach_name:<22}"
            for d in days:
                s = next((x for x in beach_scores if x["day_offset"] == d), None)
                if s:
                    icon  = ICONS.get(s["risk_level"], "?")
                    prox  = f"{s['closest_km']:.0f}km" if s["closest_km"] is not None else "—"
                    # face = part de la masse régionale qui fait face à la plage par la mer
                    ef    = s["exposed_frac"]
                    face  = f"{int(round(ef * 100)):3d}%" if ef is not None else "   —"
                    # ⚓ = particules échouées dans le rayon de la plage
                    ns    = s["n_stranded"] or 0
                    anc   = f"⚓{ns}" if ns else ""
                    line += (f"  {icon} {s['regional_score']:5.1f}{face}"
                             f" /{s['local_score']:5.1f}"
                             f" /{prox:>5} {anc:<3}")
                else:
                    line += "  " + "—" * 26
            print(line)

    print()
    # Depuis 2026-11-09 le scoring se fait sur le jeu régional COMPLET
    # (n_sample = nb de particules Antilles, actives + échouées, ratio 1).
    # Les anciennes simulations gardent l'échantillon uniforme extrapolé.
    if scoring_mode == "region":
        jeu = f"jeu régional complet {r0['n_sample']} pts Antilles (échouées incluses, ×1)"
    else:
        jeu = f"échantillon {r0['n_sample']} pts (×{ratio:.1f})"
    print(f"  Simulation  : {r0['n_particles']} particules | "
          f"{r0['n_active']} actives en mer | {jeu}")
    print(f"  Colonnes    : risque | regional_score (σ=50km) + % de cette masse FACE à la plage par la mer | "
          f"local_score (σ=radius, visibles par la mer) | closest_km | ⚓ échouées dans le rayon")
    print()
    print(f"  Seuils risk_level (regional_score extrapolé) :")
    print(f"    🟢 < {RISK_THRESHOLDS['low']}   "
          f"🟡 ≥ {RISK_THRESHOLDS['low']}   "
          f"🟠 ≥ {RISK_THRESHOLDS['medium']}   "
          f"🔴 ≥ {RISK_THRESHOLDS['high']}")
    print()


# ── Point d'entrée ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        sys.exit(0)

    db = DB_PATH

    if "--report" in sys.argv:
        print_report(db)
        sys.exit(0)

    # Fenêtres d'exposition manquantes (plage ajoutée via /admin) : on les
    # calcule ici, APRÈS la simulation (RAM libre), puis on recharge le catalogue.
    try:
        import sarga_exposure as _expo
        if _expo.ensure_exposures(db) > 0:
            BEACHES = _load_beaches_from_db() or BEACHES
    except Exception as _e:   # noqa: BLE001 — jamais bloquant pour le scoring
        print(f"  [warn] fenêtres d'exposition : {_e}", file=sys.stderr)

    n_islands = len({b["island"] for b in BEACHES})
    n_beaches = len(BEACHES)
    n_expo    = sum(1 for b in BEACHES if b.get("exposure"))
    print(f"\n🏖️  Calcul des scores de risque — {n_islands} îles, {n_beaches} plages"
          f" ({n_expo} avec fenêtre d'exposition)")
    n = compute_beach_scores(db)
    if n > 0:
        print(f"  ✅ {n} scores insérés dans beach_risk_scores")
        nt = compute_beach_timeline(db)
        if nt > 0:
            print(f"  ✅ {nt} scores 3h insérés dans beach_timeline (timeline horaire)")
        print_report(db)
    else:
        print("  Aucun score inséré.")
