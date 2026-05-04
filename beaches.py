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
from datetime import datetime, timezone
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

DB_PATH          = Path(__file__).parent / "sargassum_data.db"
DAY_OFFSETS      = [0, 1, 2, 3, 4, 5]
REGIONAL_SIGMA   = 50.0   # km — bandwidth pour le score d'approche

# Seuils sur regional_score (population extrapolée, σ = 50 km)
RISK_THRESHOLDS = {"low": 5.0, "medium": 25.0, "high": 75.0}


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
    ratio: float,
) -> dict:
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
]


def _get_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(_SCHEMA)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(beach_risk_scores)")}
    for col, typedef in _NEW_COLUMNS:
        if col not in existing:
            conn.execute(f"ALTER TABLE beach_risk_scores ADD COLUMN {col} {typedef}")
    conn.commit()
    conn.row_factory = sqlite3.Row
    return conn


# ── Calcul des scores ─────────────────────────────────────────────────────────

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
                beach.get("island", ""),
                beach["name"], beach["lat"], beach["lon"], beach["radius_km"],
                day,
                s["sample_count"], n_sample, n_active, n_particles,
                s["est_count"], s["local_score"], s["regional_score"],
                s["closest_km"], s["density_km2"],
                risk_label(s["regional_score"]),
            ))

    conn.executemany(
        """INSERT INTO beach_risk_scores
           (computed_at, simulated_at, island,
            beach_name, beach_lat, beach_lon,
            radius_km, day_offset, sample_count, n_sample, n_active,
            n_particles, est_count, local_score, regional_score,
            closest_km, density_km2, risk_level)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows_to_insert,
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
                  density_km2, risk_level, radius_km, n_active, n_particles, n_sample
           FROM beach_risk_scores
           WHERE computed_at = ?
           ORDER BY island, beach_name, day_offset""",
        (last,),
    ).fetchall()
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

        header = f"{'Plage':<22}" + "".join(f"  {'j+'+str(d):<18}" for d in days)
        sub    = f"{'':22}" + "".join(f"  {'rég / loc / prox':18}" for _ in days)
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
                    line += (f"  {icon} {s['regional_score']:5.1f}"
                             f" /{s['local_score']:5.1f}"
                             f" /{prox:>5}")
                else:
                    line += "  " + "—" * 18
            print(line)

    print()
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

    n_islands = len({b["island"] for b in BEACHES})
    n_beaches = len(BEACHES)
    print(f"\n🏖️  Calcul des scores de risque — {n_islands} îles, {n_beaches} plages")
    n = compute_beach_scores(db)
    if n > 0:
        print(f"  ✅ {n} scores insérés dans beach_risk_scores")
        print_report(db)
    else:
        print("  Aucun score inséré.")
