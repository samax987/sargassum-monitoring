#!/usr/bin/env python3
"""
beaches.py
==========
Coordonnées GPS des principales plages de Saint-Barthélemy
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
from datetime import datetime, timezone
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

DB_PATH          = Path("./sargassum_data.db")
DAY_OFFSETS      = [0, 1, 2, 3]
REGIONAL_SIGMA   = 50.0   # km — bandwidth pour le score d'approche

# Seuils sur regional_score (population extrapolée, σ = 50 km)
# Calibration :
#   1 particule à 50 km  → regional_score ≈ 8.7
#   1 particule à 36 km  → regional_score ≈ 11
#   5 particules à 50 km → regional_score ≈ 43
#  10 particules à 50 km → regional_score ≈ 87
RISK_THRESHOLDS = {"low": 5.0, "medium": 25.0, "high": 75.0}


# ── Plages de Saint-Barthélemy ─────────────────────────────────────────────────

BEACHES = [
    {"name": "Flamands",         "lat": 17.9067, "lon": -62.8467, "radius_km": 3.0},
    {"name": "Colombier",        "lat": 17.9033, "lon": -62.8600, "radius_km": 2.0},
    {"name": "Saint-Jean",       "lat": 17.9000, "lon": -62.8267, "radius_km": 4.0},
    {"name": "Lorient",          "lat": 17.9000, "lon": -62.8100, "radius_km": 3.0},
    {"name": "Grand_Cul-de-Sac", "lat": 17.9117, "lon": -62.7917, "radius_km": 3.0},
    {"name": "Petit_Cul-de-Sac", "lat": 17.9067, "lon": -62.7967, "radius_km": 2.0},
    {"name": "Toiny",            "lat": 17.8933, "lon": -62.7817, "radius_km": 2.0},
    {"name": "Gouverneur",       "lat": 17.8717, "lon": -62.8433, "radius_km": 3.0},
    {"name": "Grande_Saline",    "lat": 17.8717, "lon": -62.8267, "radius_km": 3.0},
    {"name": "Marigot",          "lat": 17.9033, "lon": -62.8067, "radius_km": 2.0},
]


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


# ── Scoring ────────────────────────────────────────────────────────────────────

def _score_beach(
    positions: list,
    beach_lat: float,
    beach_lon: float,
    radius_km: float,
    ratio: float,          # n_active / n_sample — facteur d'extrapolation
) -> dict:
    """
    Calcule tous les indicateurs de risque pour une plage.

    Paramètres
    ----------
    positions  : liste [[lon, lat], …] — échantillon ≤ 500 pts
    ratio      : n_active / n_sample  — chaque pt représente `ratio` particules réelles

    Retourne
    --------
    sample_count   : nb de particules (échantillon) dans radius_km
    est_count      : extrapolé = sample_count × ratio
    local_score    : Σ Gauss(d, σ=radius_km) × ratio  — champ proche
    regional_score : Σ Gauss(d, σ=50 km)    × ratio  — approche régionale
    closest_km     : distance à la particule la plus proche (None si aucune)
    density_km2    : est_count / (π × radius_km²)
    """
    sample_count    = 0
    local_gauss_sum = 0.0
    reg_gauss_sum   = 0.0
    min_dist        = math.inf

    for pt in positions:
        if len(pt) < 2:
            continue
        d = haversine_km(beach_lat, beach_lon, float(pt[1]), float(pt[0]))

        if d < min_dist:
            min_dist = d
        if d <= radius_km:
            sample_count += 1

        # Gaussiennes : exp(-d²/(2σ²))
        local_gauss_sum += math.exp(-0.5 * (d / radius_km)    ** 2)
        reg_gauss_sum   += math.exp(-0.5 * (d / REGIONAL_SIGMA) ** 2)

    est_count      = round(sample_count * ratio, 2)
    local_score    = round(local_gauss_sum  * ratio, 3)
    regional_score = round(reg_gauss_sum    * ratio, 3)
    closest_km     = round(min_dist, 2) if math.isfinite(min_dist) else None
    catchment_area = math.pi * radius_km ** 2
    density_km2    = round(est_count / catchment_area, 6) if catchment_area > 0 else 0.0

    return {
        "sample_count":   sample_count,
        "est_count":      est_count,
        "local_score":    local_score,
        "regional_score": regional_score,
        "closest_km":     closest_km,
        "density_km2":    density_km2,
    }


def risk_label(regional_score: float) -> str:
    """Niveau de risque basé sur le regional_score (population extrapolée)."""
    if regional_score >= RISK_THRESHOLDS["high"]:
        return "high"
    if regional_score >= RISK_THRESHOLDS["medium"]:
        return "medium"
    if regional_score >= RISK_THRESHOLDS["low"]:
        return "low"
    return "none"


# ── Base de données ───────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS beach_risk_scores (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at    TEXT    NOT NULL,
    simulated_at   TEXT    NOT NULL,
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

# Colonnes ajoutées après la version initiale (migration idempotente)
_NEW_COLUMNS = [
    ("local_score",    "REAL"),
    ("regional_score", "REAL"),
    ("closest_km",     "REAL"),
    ("density_km2",    "REAL"),
]


def _get_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(_SCHEMA)
    # Migration : ajoute les nouvelles colonnes si absentes
    existing = {row[1] for row in conn.execute("PRAGMA table_info(beach_risk_scores)")}
    for col, typedef in _NEW_COLUMNS:
        if col not in existing:
            conn.execute(f"ALTER TABLE beach_risk_scores ADD COLUMN {col} {typedef}")
    conn.commit()
    conn.row_factory = sqlite3.Row
    return conn


# ── Calcul des scores ─────────────────────────────────────────────────────────

def compute_beach_scores(db_path: Path = DB_PATH) -> int:
    """
    Charge la simulation la plus récente, calcule les scores densité-aware
    pour chaque plage × j+0…j+3, et stocke dans beach_risk_scores.
    """
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
    snaps = conn.execute(
        f"""SELECT day_offset, positions_json, n_particles, active_fraction
            FROM drift_predictions
            WHERE simulated_at = ? AND day_offset IN ({placeholders})
            ORDER BY day_offset""",
        (simulated_at, *DAY_OFFSETS),
    ).fetchall()

    if not snaps:
        print(f"  ⚠️  Aucun snapshot pour j+{DAY_OFFSETS} dans la simulation {simulated_at}.")
        conn.close()
        return 0

    computed_at    = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows_to_insert = []

    for snap in snaps:
        day         = snap["day_offset"]
        n_particles = snap["n_particles"] or 0
        act_frac    = snap["active_fraction"] or 0.0
        n_active    = int(round(n_particles * act_frac))

        try:
            positions = json.loads(snap["positions_json"] or "[]")
        except (json.JSONDecodeError, TypeError):
            positions = []

        n_sample = len(positions)
        ratio    = n_active / n_sample if n_sample > 0 else 0.0

        for beach in BEACHES:
            s = _score_beach(positions, beach["lat"], beach["lon"],
                             beach["radius_km"], ratio)
            rows_to_insert.append((
                computed_at, simulated_at,
                beach["name"], beach["lat"], beach["lon"], beach["radius_km"],
                day,
                s["sample_count"], n_sample, n_active, n_particles,
                s["est_count"], s["local_score"], s["regional_score"],
                s["closest_km"], s["density_km2"],
                risk_label(s["regional_score"]),
            ))

    conn.executemany(
        """INSERT INTO beach_risk_scores
           (computed_at, simulated_at, beach_name, beach_lat, beach_lon,
            radius_km, day_offset, sample_count, n_sample, n_active,
            n_particles, est_count, local_score, regional_score,
            closest_km, density_km2, risk_level)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows_to_insert,
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
        """SELECT beach_name, day_offset, sample_count, est_count,
                  local_score, regional_score, closest_km,
                  density_km2, risk_level, radius_km, n_active, n_particles, n_sample
           FROM beach_risk_scores
           WHERE computed_at = ?
           ORDER BY beach_name, day_offset""",
        (last,),
    ).fetchall()
    conn.close()

    if not scores:
        print("Aucun score à afficher.")
        return

    ICONS = {"none": "🟢", "low": "🟡", "medium": "🟠", "high": "🔴"}
    days  = sorted({r["day_offset"] for r in scores})
    by_beach: dict[str, list] = {}
    for r in scores:
        by_beach.setdefault(r["beach_name"], []).append(r)

    print(f"\n{'═'*72}")
    print(f"  🏖️  Risque sargasses — Saint-Barthélemy  (calculé {last})")
    print(f"{'═'*72}")

    # En-tête
    header = f"{'Plage':<20}" + "".join(
        f"  {'j+'+str(d):<18}" for d in days
    )
    print(header)
    sub = f"{'':20}" + "".join(
        f"  {'rég / loc / prox':18}" for _ in days
    )
    print(sub)
    print("─" * len(header))

    for beach_name, beach_scores in by_beach.items():
        line = f"{beach_name:<20}"
        for d in days:
            s = next((x for x in beach_scores if x["day_offset"] == d), None)
            if s:
                icon  = ICONS.get(s["risk_level"], "?")
                prox  = f"{s['closest_km']:.0f}km" if s["closest_km"] is not None else "—"
                line += (f"  {icon} {s['regional_score']:5.1f}"
                         f" /{s['local_score']:5.1f}"
                         f" /{prox:>5}")
            else:
                line += "  " + "—" * 18
        print(line)

    print()
    r0 = scores[0]
    ratio = (r0["n_active"] or 0) / (r0["n_sample"] or 1) if r0["n_sample"] else 0
    print(f"  Simulation  : {r0['n_particles']} particules | "
          f"{r0['n_active']} actives | "
          f"échantillon {r0['n_sample']} pts (×{ratio:.1f})")
    print(f"  Colonnes    : risque | regional_score (σ=50km) | "
          f"local_score (σ=radius) | closest_km")
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

    print("\n🏖️  Calcul des scores de risque — plages de Saint-Barthélemy")
    n = compute_beach_scores(db)
    if n > 0:
        print(f"  ✅ {n} scores insérés dans beach_risk_scores")
        print_report(db)
    else:
        print("  Aucun score inséré.")
