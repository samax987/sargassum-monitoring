#!/usr/bin/env python3
"""
beaches.py
==========
Coordonnées GPS des principales plages de Saint-Barthélemy
et calcul du risque d'échouage de sargasses par plage.

Le calcul s'appuie sur les snapshots de dérive (drift_predictions) produits
par sargassum_collector.py --simulate, qui utilisent les courants AVISO+ DUACS.

Usage
-----
  python beaches.py              # calcule et affiche les scores (dernière sim.)
  python beaches.py --report     # affiche uniquement le dernier rapport stocké
  python beaches.py --help

La table beach_risk_scores est créée automatiquement dans sargassum_data.db.
"""

import json
import math
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

DB_PATH = Path("./sargassum_data.db")

# Jours de prévision à évaluer (j+0 à j+3)
DAY_OFFSETS = [0, 1, 2, 3]

# Seuils de risque basés sur le nombre de particules (échantillon ≤ 500)
RISK_THRESHOLDS = {"low": 1, "medium": 5, "high": 15}


# ── Plages de Saint-Barthélemy ─────────────────────────────────────────────────
#
# Coordonnées : centroïde de la plage (WGS-84).
# radius_km   : zone de catchment — distance à partir de laquelle une
#               particule est considérée comme susceptible d'échouer.

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


def count_particles_in_radius(
    positions: list,
    beach_lat: float,
    beach_lon: float,
    radius_km: float,
) -> int:
    """
    Compte les particules de dérive [[lon, lat], …] dans un rayon donné.
    positions_json stocke [lon, lat] (ordre du collecteur OpenDrift).
    """
    count = 0
    for pt in positions:
        if len(pt) < 2:
            continue
        lon, lat = float(pt[0]), float(pt[1])
        if haversine_km(beach_lat, beach_lon, lat, lon) <= radius_km:
            count += 1
    return count


def risk_label(sample_count: int) -> str:
    """Retourne 'none' | 'low' | 'medium' | 'high' selon le comptage échantillon."""
    if sample_count >= RISK_THRESHOLDS["high"]:
        return "high"
    if sample_count >= RISK_THRESHOLDS["medium"]:
        return "medium"
    if sample_count >= RISK_THRESHOLDS["low"]:
        return "low"
    return "none"


# ── Base de données ───────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS beach_risk_scores (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at  TEXT    NOT NULL,  -- horodatage du calcul (UTC ISO-8601)
    simulated_at TEXT    NOT NULL,  -- référence à drift_predictions.simulated_at
    beach_name   TEXT    NOT NULL,
    beach_lat    REAL    NOT NULL,
    beach_lon    REAL    NOT NULL,
    radius_km    REAL    NOT NULL,
    day_offset   INTEGER NOT NULL,  -- 0=j+0, 1=j+1, 2=j+2, 3=j+3
    sample_count INTEGER NOT NULL,  -- particules (échantillon ≤ 500) dans la zone
    n_sample     INTEGER,           -- taille de l'échantillon pour ce jour
    n_active     INTEGER,           -- nb de particules actives (total simulation)
    n_particles  INTEGER,           -- nb de particules semées au t0
    est_count    REAL,              -- comptage extrapolé à la population entière
    risk_level   TEXT    NOT NULL   -- 'none' | 'low' | 'medium' | 'high'
);
"""


def _get_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(_SCHEMA)
    conn.row_factory = sqlite3.Row
    return conn


# ── Calcul des scores ─────────────────────────────────────────────────────────

def compute_beach_scores(db_path: Path = DB_PATH) -> int:
    """
    Charge la simulation de dérive la plus récente depuis drift_predictions,
    calcule pour chaque plage et pour j+0…j+3 le nombre de particules dans
    la zone de catchment, puis stocke les résultats dans beach_risk_scores.

    Retourne le nombre de lignes insérées (0 si aucune simulation disponible).
    """
    conn = _get_conn(db_path)

    # Dernière simulation disponible
    row = conn.execute(
        "SELECT MAX(simulated_at) AS max_sim FROM drift_predictions"
    ).fetchone()
    if not row or not row["max_sim"]:
        print("  ⚠️  Aucune simulation de dérive dans la base.")
        print("       Lancez : python sargassum_collector.py --simulate")
        conn.close()
        return 0
    simulated_at = row["max_sim"]

    # Snapshots des jours demandés
    placeholders = ",".join("?" * len(DAY_OFFSETS))
    snaps = conn.execute(
        f"""SELECT day_offset, positions_json, n_particles, active_fraction
            FROM drift_predictions
            WHERE simulated_at = ? AND day_offset IN ({placeholders})
            ORDER BY day_offset""",
        (simulated_at, *DAY_OFFSETS),
    ).fetchall()

    if not snaps:
        print(f"  ⚠️  Aucun snapshot trouvé pour j+{DAY_OFFSETS} dans la simulation {simulated_at}.")
        conn.close()
        return 0

    computed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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

        for beach in BEACHES:
            sc = count_particles_in_radius(
                positions, beach["lat"], beach["lon"], beach["radius_km"]
            )
            # Extrapolation linéaire : sc / n_sample × n_active
            est = round(sc / n_sample * n_active, 2) if n_sample > 0 else 0.0

            rows_to_insert.append((
                computed_at, simulated_at,
                beach["name"], beach["lat"], beach["lon"], beach["radius_km"],
                day, sc, n_sample, n_active, n_particles,
                est, risk_label(sc),
            ))

    conn.executemany(
        """INSERT INTO beach_risk_scores
           (computed_at, simulated_at, beach_name, beach_lat, beach_lon,
            radius_km, day_offset, sample_count, n_sample, n_active,
            n_particles, est_count, risk_level)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows_to_insert,
    )
    conn.commit()
    conn.close()
    return len(rows_to_insert)


# ── Rapport ───────────────────────────────────────────────────────────────────

def print_report(db_path: Path = DB_PATH) -> None:
    """Affiche le dernier rapport de risque sous forme de tableau."""
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
                  risk_level, radius_km, n_active, n_particles
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
    days = sorted({r["day_offset"] for r in scores})

    # Regrouper par plage
    by_beach: dict[str, list] = {}
    for r in scores:
        by_beach.setdefault(r["beach_name"], []).append(r)

    print(f"\n{'═'*62}")
    print(f"  🏖️  Risque sargasses — Saint-Barthélemy")
    print(f"  Calculé : {last}")
    print(f"{'═'*62}")

    header = f"{'Plage':<22}" + "".join(f"   j+{d}  " for d in days)
    print(header)
    print("─" * len(header))

    for beach_name, beach_scores in by_beach.items():
        line = f"{beach_name:<22}"
        for d in days:
            s = next((x for x in beach_scores if x["day_offset"] == d), None)
            if s:
                icon = ICONS.get(s["risk_level"], "?")
                line += f"  {icon}{s['sample_count']:>3}pt  "
            else:
                line += "    —    "
        print(line)

    print()

    # Détail d'une plage représentative (n_active / n_particles)
    sample_row = scores[0]
    print(f"  Simulation  : {sample_row['n_particles']} particules semées "
          f"| {sample_row['n_active']} actives à j+{sample_row['day_offset']}")
    print(f"  Échantillon : ≤ 500 pts stockés (extrapolation = est_count)")
    print()
    print(f"  Légende :")
    print(f"    🟢 aucune  🟡 faible (≥{RISK_THRESHOLDS['low']}pt)  "
          f"🟠 moyen (≥{RISK_THRESHOLDS['medium']}pt)  "
          f"🔴 élevé (≥{RISK_THRESHOLDS['high']}pt)")
    print(f"    pt = particules dans l'échantillon dans la zone de catchment")
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
