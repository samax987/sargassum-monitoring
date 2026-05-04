#!/usr/bin/env python3
"""
sarga_calibration_spatial.py
============================
Calibration spatiale du modèle de drift : pour chaque observation terrain
géocodée, mesure l'erreur de positionnement de la masse de sargasses prédite.

Différence avec sarga_calibration.py (ancien) :
  - Ancien : compare le risk_level (catégoriel) sur des plages au nom fuzzy.
  - Nouveau : compare la POSITION GÉOGRAPHIQUE (km) des particules vs
    le lieu réel de l'échouement → utile pour le « dernier kilomètre ».

Pour chaque obs O à (lat, lon, date_obs) et chaque day_offset 1..5 :
  - cible_sim = date_obs - day_offset jours
  - cherche les drift_predictions avec sim_start ≈ cible_sim (±1 jour)
  - lit positions_json
  - calcule :
      min_dist_km     : distance à la particule la plus proche
      n_within_25km   : nombre de particules dans 25 km
      n_within_50km   : nombre de particules dans 50 km
      centroid_lon/lat : centroïde des particules dans 100 km
      delta_lon_km    : écart est-ouest entre centroïde et obs (négatif = trop à l'ouest)
      delta_lat_km    : écart nord-sud
  - stocke 1 ligne par (obs, day_offset)

Aggrège ensuite par île × mois × day_offset → calibration_spatial_bias
"""

import argparse
import json
import math
import re
import sqlite3
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "sargassum_data.db"


# ── Géocodage des plages observées ──────────────────────────────────────────
# Dict {(island, normalized_name_fragment): (lat, lon)} — couvre les ~30 noms
# les plus fréquents dans beach_observations. Le fragment doit apparaître dans
# le nom normalisé de l'observation pour matcher (substring match).

OBS_GEOCODE = {
    # Saint-Martin
    ("Saint-Martin", "cul de sac"):           (18.0967, -63.0100),
    ("Saint-Martin", "orientale"):            (18.0817, -63.0233),
    ("Saint-Martin", "orient bay"):           (18.0817, -63.0233),
    ("Saint-Martin", "grand case"):           (18.1000, -63.0567),
    ("Saint-Martin", "marigot"):              (18.0700, -63.0850),

    # Saint-Barth
    ("Saint-Barth", "flamands"):              (17.9067, -62.8467),
    ("Saint-Barth", "saint jean"):            (17.9000, -62.8267),
    ("Saint-Barth", "lorient"):               (17.9000, -62.8100),
    ("Saint-Barth", "salines"):               (17.8717, -62.8267),
    ("Saint-Barth", "gouverneur"):            (17.8717, -62.8433),
    ("Saint-Barth", "cul de sac"):            (17.9117, -62.7917),

    # Guadeloupe
    ("Guadeloupe", "porte d'enfer"):          (16.5167, -61.4667),
    ("Guadeloupe", "porte d enfer"):          (16.5167, -61.4667),
    ("Guadeloupe", "anse-bertrand"):          (16.4833, -61.5000),
    ("Guadeloupe", "saint francois"):         (16.2517, -61.2697),
    ("Guadeloupe", "rochers"):                (16.2350, -61.2750),
    ("Guadeloupe", "raisins clairs"):         (16.2533, -61.2750),
    ("Guadeloupe", "anse du mancenillier"):   (16.2400, -61.2600),
    ("Guadeloupe", "sainte anne"):            (16.2270, -61.3819),
    ("Guadeloupe", "galbas"):                 (16.2250, -61.4050),
    ("Guadeloupe", "bois jolan"):             (16.2300, -61.3700),
    ("Guadeloupe", "anse belley"):            (16.2240, -61.3850),
    ("Guadeloupe", "le moule"):               (16.3333, -61.3500),
    ("Guadeloupe", "le gosier"):              (16.2030, -61.4940),
    ("Guadeloupe", "datcha"):                 (16.2050, -61.5100),
    ("Guadeloupe", "petit-bourg"):            (16.1900, -61.5867),
    ("Guadeloupe", "petit canal"):            (16.3833, -61.4667),
    ("Guadeloupe", "anse maurice"):           (16.3833, -61.4500),
    ("Guadeloupe", "petit bourg"):            (16.1900, -61.5867),
    ("Guadeloupe", "vinaigrerie"):            (16.1850, -61.5833),
    ("Guadeloupe", "malendure"):              (16.1900, -61.7400),
    ("Guadeloupe", "bouillante"):             (16.1483, -61.7700),
    ("Guadeloupe", "desirade"):               (16.3000, -61.0500),
    ("Guadeloupe", "souffleur"):              (16.3333, -61.0833),
    ("Guadeloupe", "marie galante"):          (15.9000, -61.2333),
    ("Guadeloupe", "capesterre de marie"):    (15.9000, -61.2333),
    ("Guadeloupe", "terre-de-haut"):          (15.8667, -61.5833),

    # Martinique
    ("Martinique", "littoral du francois"):   (14.6170, -60.9010),
    ("Martinique", "le francois"):            (14.6170, -60.9010),
    ("Martinique", "côte atlantique"):        (14.5550, -60.8383),  # zone Vauclin
    ("Martinique", "cote atlantique"):        (14.5550, -60.8383),
    ("Martinique", "diamant"):                (14.4667, -61.0233),
    ("Martinique", "grande anse du diamant"): (14.4667, -61.0233),
    ("Martinique", "cap chevalier"):          (14.4900, -60.8567),
    ("Martinique", "le robert"):              (14.6817, -60.9433),
    ("Martinique", "baie de cayol"):          (14.6800, -60.9100),
    ("Martinique", "le marin"):               (14.4700, -60.8733),
    ("Martinique", "tombolo"):                (14.7780, -61.0000),
    ("Martinique", "sainte-marie"):           (14.7780, -61.0000),
    ("Martinique", "anse michel"):            (14.4600, -60.8800),
    ("Martinique", "les salines"):            (14.3917, -60.8617),
    ("Martinique", "tartane"):                (14.7533, -60.8750),
    ("Martinique", "trinite"):                (14.7383, -60.9700),
}


def normalize(name: str) -> str:
    nfkd = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in nfkd if not unicodedata.combining(c))
    name = name.lower().replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", name).strip()


def geocode(island: str, beach_name: str) -> tuple[float, float] | None:
    """Retourne (lat, lon) ou None si introuvable."""
    norm = normalize(beach_name)
    # match par fragment — chaque entrée du dict est un fragment qu'on cherche dans norm
    best = None
    best_len = 0
    for (isl, frag), coords in OBS_GEOCODE.items():
        if isl != island:
            continue
        if normalize(frag) in norm and len(frag) > best_len:
            best = coords
            best_len = len(frag)
    return best


# ── Distance ─────────────────────────────────────────────────────────────────

def haversine_km(lon1, lat1, lon2, lat2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    dp = p2 - p1
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def deg_per_km_at(lat: float) -> tuple[float, float]:
    """Retourne (deg_lat_per_km, deg_lon_per_km) à une latitude donnée."""
    deg_lat_per_km = 1.0 / 110.574
    deg_lon_per_km = 1.0 / (111.320 * math.cos(math.radians(lat)))
    return deg_lat_per_km, deg_lon_per_km


# ── Schéma DB ────────────────────────────────────────────────────────────────

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS calibration_spatial (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        computed_at     TEXT    NOT NULL,
        obs_id          INTEGER NOT NULL,
        island          TEXT    NOT NULL,
        obs_beach       TEXT    NOT NULL,
        obs_date        TEXT    NOT NULL,
        obs_lat         REAL    NOT NULL,
        obs_lon         REAL    NOT NULL,
        observed_risk   TEXT,
        sim_id          INTEGER,
        sim_start       TEXT,
        day_offset      INTEGER,
        n_particles     INTEGER,
        n_within_25km   INTEGER,
        n_within_50km   INTEGER,
        min_dist_km     REAL,
        centroid_lat    REAL,
        centroid_lon    REAL,
        delta_lat_km    REAL,
        delta_lon_km    REAL
    );

    CREATE INDEX IF NOT EXISTS idx_calib_spatial_obs ON calibration_spatial(obs_id);
    CREATE INDEX IF NOT EXISTS idx_calib_spatial_island_day ON calibration_spatial(island, day_offset);

    CREATE TABLE IF NOT EXISTS calibration_spatial_bias (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        computed_at     TEXT    NOT NULL,
        island          TEXT    NOT NULL,
        month           INTEGER,
        day_offset      INTEGER NOT NULL,
        n_obs           INTEGER NOT NULL,
        mean_min_dist_km    REAL,
        median_min_dist_km  REAL,
        mean_delta_lat_km   REAL,
        mean_delta_lon_km   REAL,
        rmse_km             REAL,
        recommendation      TEXT
    );
    """)
    conn.commit()


# ── Recherche des sims candidates ───────────────────────────────────────────

def find_matching_sims(conn: sqlite3.Connection, obs_date: str, day_offset: int,
                       tolerance_days: int = 1) -> list[dict]:
    """Retourne les sims dont sim_start ≈ obs_date - day_offset jours."""
    obs_dt = datetime.fromisoformat(obs_date.replace("Z", ""))
    target = obs_dt - timedelta(days=day_offset)
    lo = (target - timedelta(days=tolerance_days)).strftime("%Y-%m-%dT%H:%M:%S")
    hi = (target + timedelta(days=tolerance_days)).strftime("%Y-%m-%dT%H:%M:%S")

    rows = conn.execute("""
        SELECT id, sim_start, day_offset, n_particles, positions_json
        FROM drift_predictions
        WHERE day_offset = ?
          AND substr(sim_start,1,19) BETWEEN ? AND ?
        ORDER BY sim_start
    """, (day_offset, lo, hi)).fetchall()
    return [dict(zip(["id","sim_start","day_offset","n_particles","positions_json"], r))
            for r in rows]


# ── Métriques ────────────────────────────────────────────────────────────────

def compute_metrics(positions: list, obs_lat: float, obs_lon: float) -> dict:
    """positions = [[lon, lat], ...] sub-échantillon ≤500."""
    if not positions:
        return {}
    dists = []
    for lon, lat in positions:
        d = haversine_km(obs_lon, obs_lat, lon, lat)
        dists.append((d, lon, lat))
    dists.sort(key=lambda x: x[0])

    min_dist = dists[0][0]
    n25 = sum(1 for d, _, _ in dists if d <= 25)
    n50 = sum(1 for d, _, _ in dists if d <= 50)

    # Centroïde des particules dans 100 km
    near = [(lon, lat) for d, lon, lat in dists if d <= 100]
    if near:
        c_lon = sum(p[0] for p in near) / len(near)
        c_lat = sum(p[1] for p in near) / len(near)
        deg_lat_per_km, deg_lon_per_km = deg_per_km_at(obs_lat)
        delta_lon_km = (c_lon - obs_lon) / deg_lon_per_km
        delta_lat_km = (c_lat - obs_lat) / deg_lat_per_km
    else:
        c_lon = c_lat = delta_lon_km = delta_lat_km = None

    return {
        "n_particles": len(positions),
        "n_within_25km": n25,
        "n_within_50km": n50,
        "min_dist_km": round(min_dist, 2),
        "centroid_lon": round(c_lon, 4) if c_lon is not None else None,
        "centroid_lat": round(c_lat, 4) if c_lat is not None else None,
        "delta_lon_km": round(delta_lon_km, 2) if delta_lon_km is not None else None,
        "delta_lat_km": round(delta_lat_km, 2) if delta_lat_km is not None else None,
    }


# ── Agrégation ──────────────────────────────────────────────────────────────

def compute_bias(rows: list[dict]) -> list[dict]:
    """Aggrège par île × mois × day_offset."""
    from collections import defaultdict
    import statistics
    groups = defaultdict(list)
    for r in rows:
        if r.get("min_dist_km") is None:
            continue
        month = int(r["obs_date"][5:7])
        groups[(r["island"], month, r["day_offset"])].append(r)

    biases = []
    for (island, month, day_off), group in sorted(groups.items()):
        dists = [g["min_dist_km"] for g in group]
        dlon  = [g["delta_lon_km"] for g in group if g["delta_lon_km"] is not None]
        dlat  = [g["delta_lat_km"] for g in group if g["delta_lat_km"] is not None]
        rmse = math.sqrt(sum(d*d for d in dists) / len(dists)) if dists else None

        reco_parts = []
        if dlon and abs(statistics.mean(dlon)) > 5:
            sense = "à l'est" if statistics.mean(dlon) > 0 else "à l'ouest"
            reco_parts.append(f"décalage moyen {abs(statistics.mean(dlon)):.0f} km {sense}")
        if dlat and abs(statistics.mean(dlat)) > 5:
            sense = "au nord" if statistics.mean(dlat) > 0 else "au sud"
            reco_parts.append(f"{abs(statistics.mean(dlat)):.0f} km {sense}")
        reco = "; ".join(reco_parts) if reco_parts else "biais < 5 km, modèle calibré"

        biases.append({
            "island": island,
            "month": month,
            "day_offset": day_off,
            "n_obs": len(group),
            "mean_min_dist_km":   round(statistics.mean(dists), 2),
            "median_min_dist_km": round(statistics.median(dists), 2),
            "mean_delta_lat_km":  round(statistics.mean(dlat), 2) if dlat else None,
            "mean_delta_lon_km":  round(statistics.mean(dlon), 2) if dlon else None,
            "rmse_km":            round(rmse, 2) if rmse else None,
            "recommendation":     reco,
        })
    return biases


# ── Stockage ─────────────────────────────────────────────────────────────────

def store(conn: sqlite3.Connection, rows: list[dict], biases: list[dict], now: str):
    conn.execute("DELETE FROM calibration_spatial")
    for r in rows:
        conn.execute("""
            INSERT INTO calibration_spatial
            (computed_at, obs_id, island, obs_beach, obs_date, obs_lat, obs_lon,
             observed_risk, sim_id, sim_start, day_offset, n_particles,
             n_within_25km, n_within_50km, min_dist_km, centroid_lat, centroid_lon,
             delta_lat_km, delta_lon_km)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now, r["obs_id"], r["island"], r["obs_beach"], r["obs_date"],
            r["obs_lat"], r["obs_lon"], r.get("observed_risk"),
            r.get("sim_id"), r.get("sim_start"), r.get("day_offset"),
            r.get("n_particles"), r.get("n_within_25km"), r.get("n_within_50km"),
            r.get("min_dist_km"), r.get("centroid_lat"), r.get("centroid_lon"),
            r.get("delta_lat_km"), r.get("delta_lon_km"),
        ))
    conn.execute("DELETE FROM calibration_spatial_bias")
    for b in biases:
        conn.execute("""
            INSERT INTO calibration_spatial_bias
            (computed_at, island, month, day_offset, n_obs,
             mean_min_dist_km, median_min_dist_km, mean_delta_lat_km,
             mean_delta_lon_km, rmse_km, recommendation)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now, b["island"], b["month"], b["day_offset"], b["n_obs"],
            b["mean_min_dist_km"], b["median_min_dist_km"],
            b["mean_delta_lat_km"], b["mean_delta_lon_km"],
            b["rmse_km"], b["recommendation"],
        ))
    conn.commit()


# ── Rapport ──────────────────────────────────────────────────────────────────

def print_report(rows: list[dict], biases: list[dict], n_obs_total: int, n_obs_geocoded: int):
    print(f"\n{'='*72}")
    print(f"  CALIBRATION SPATIALE — Sargasses")
    print(f"{'='*72}")
    print(f"  Observations totales       : {n_obs_total}")
    print(f"  Géocodées                  : {n_obs_geocoded}")
    print(f"  Matches (obs × day_offset) : {len(rows)}")
    if rows:
        dists = [r["min_dist_km"] for r in rows if r.get("min_dist_km") is not None]
        if dists:
            mean_d = sum(dists) / len(dists)
            med_d  = sorted(dists)[len(dists)//2]
            print(f"  Distance min moyenne       : {mean_d:>6.1f} km")
            print(f"  Distance min médiane       : {med_d:>6.1f} km")

    if biases:
        print(f"\n  {'Île':<14} {'Mois':<5} {'j+':<4} {'N':>3}  {'min_d_km':>9}  "
              f"{'Δlon_km':>9}  {'Δlat_km':>9}  Diagnostic")
        print(f"  {'-'*100}")
        for b in biases:
            dlon = f"{b['mean_delta_lon_km']:+6.1f}" if b['mean_delta_lon_km'] is not None else "  —  "
            dlat = f"{b['mean_delta_lat_km']:+6.1f}" if b['mean_delta_lat_km'] is not None else "  —  "
            print(f"  {b['island']:<14} {b['month']:<5} {'j+'+str(b['day_offset']):<4} "
                  f"{b['n_obs']:>3}  {b['mean_min_dist_km']:>8.1f}  "
                  f"{dlon:>9}  {dlat:>9}  {b['recommendation']}")
    print(f"{'='*72}\n")


# ── Main ─────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False, verbose: bool = False, day_offsets: tuple = (1,2,3,4,5)) -> dict:
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)

    obs_rows = conn.execute("""
        SELECT id, observed_at, island, beach_name, observed_risk
        FROM beach_observations
        WHERE island IN ('Martinique','Guadeloupe','Saint-Barth','Saint-Martin','Marie-Galante')
        ORDER BY observed_at DESC
    """).fetchall()
    n_total = len(obs_rows)

    rows_out = []
    n_geo = 0
    for obs_id, observed_at, island, beach_name, observed_risk in obs_rows:
        coords = geocode(island, beach_name)
        if not coords:
            if verbose:
                print(f"  [skip] non géocodable : {island} / {beach_name}")
            continue
        n_geo += 1
        obs_lat, obs_lon = coords

        for day_off in day_offsets:
            sims = find_matching_sims(conn, observed_at, day_off)
            if not sims:
                continue
            # Prendre la sim la plus proche en temps (1ère)
            sim = sims[0]
            try:
                positions = json.loads(sim["positions_json"])
            except Exception:
                continue
            metrics = compute_metrics(positions, obs_lat, obs_lon)
            if not metrics:
                continue
            row = {
                "obs_id": obs_id,
                "island": island,
                "obs_beach": beach_name,
                "obs_date": observed_at[:10],
                "obs_lat": obs_lat,
                "obs_lon": obs_lon,
                "observed_risk": observed_risk,
                "sim_id": sim["id"],
                "sim_start": sim["sim_start"],
                "day_offset": day_off,
                **metrics,
            }
            rows_out.append(row)

    biases = compute_bias(rows_out)
    print_report(rows_out, biases, n_total, n_geo)

    if not dry_run:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        store(conn, rows_out, biases, now)
        print(f"[CALIB-SPATIAL] {len(rows_out)} matches, {len(biases)} biais stockés en DB.")
    conn.close()
    return {"matches": len(rows_out), "biases": len(biases),
            "n_geocoded": n_geo, "n_total": n_total}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report",  action="store_true", help="affiche sans écrire en DB")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.report, verbose=args.verbose)


if __name__ == "__main__":
    main()
