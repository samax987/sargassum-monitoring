#!/usr/bin/env python3
"""
sarga_exposure.py
=================
Fenêtre d'exposition de chaque plage : depuis quelles directions la mer
ouverte l'atteint-elle sans croiser la terre, et jusqu'à quelle distance ?

Pourquoi
--------
Jusqu'au 2026-11-09 toutes les plages d'une même île recevaient le même score :
une masse à 15 km au nord de Saint-Barth comptait autant pour Gouverneur (côte
sud) que pour Flamands (côte nord). Or le « dernier kilomètre » est d'abord une
question de géométrie : une plage ne peut recevoir que ce qui lui arrive par la
mer. On calcule donc, une fois pour toutes, pour chaque plage et chaque
direction (pas de 5°), la distance d'eau libre avant la première terre. Le
scoring (beaches.py) ne compte ensuite dans le score LOCAL que les particules
« visibles par la mer » depuis la plage.

Méthode
-------
1. Trait de côte GSHHG pleine résolution (~100-200 m), déjà présent dans le venv
   via roaring_landmask (le masque utilisé par OpenDrift pour l'échouage).
2. Le point du catalogue est souvent posé sur le sable, voire en retrait : on le
   PROJETTE sur le point de côte le plus proche (distance mémorisée).
3. 72 rayons (0°, 5°, …, 355°, azimut géographique, 0 = nord) de 40 km max,
   lancés depuis un « œil » posé 300 m au large du point de côte (sinon un rayon
   le long du rivage rase la terre) : la fenêtre vaut, par direction, la
   longueur du premier segment d'eau (0 = terre ; 40 = mer ouverte au plafond).

Côté scoring (beaches.py) : le test « visible par la mer » ne s'applique qu'aux
particules EN MER. Une particule échouée est posée par OpenDrift au premier pas
(3 h) où elle est à terre, donc jusqu'à 2-3 km à l'intérieur : la tester en
visibilité la cacherait toujours. Dans le rayon de la plage, elle compte.

Stocké dans beaches_config.exposure_json :
  {"step_deg": 5, "max_km": 40, "open_km": [72 valeurs],
   "snap_km": distance point→côte, "snap_lat", "snap_lon",
   "eye_lat", "eye_lon", "eye_km": origine réelle des rayons,
   "facing_deg": direction moyenne d'ouverture, "open_frac": part d'horizon
   ouverte à ≥ 5 km, "computed_at": ...}

Usage
-----
  python sarga_exposure.py --all              # (re)calcule les 58 plages
  python sarga_exposure.py --missing          # seulement celles sans fenêtre
  python sarga_exposure.py --show Flamands    # affiche la rose d'une plage
  python sarga_exposure.py --all --dry-run    # calcule et affiche sans écrire

Coût : ~5 s et ~470 Mo de RAM pour charger GSHHG, puis quelques ms par plage.
beaches.py appelle ensure_exposures() et ne charge GSHHG que s'il manque
une fenêtre (plage ajoutée via /admin).
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "sargassum_data.db"

STEP_DEG = 5
MAX_KM   = 40.0
TOL_KM   = 0.15          # tolérance au départ du rayon (on part du trait de côte)
EYE_KM   = 0.3           # l'« œil » d'où partent les rayons : 300 m au large du point de côte
OPEN_MIN_KM = 5.0        # une direction est « ouverte » si ≥ 5 km d'eau libre
# Boîte des Antilles françaises + marge : on ne garde que ces polygones GSHHG
ANTILLES_BBOX = (-64.5, 13.5, -59.5, 19.0)   # lon_min, lat_min, lon_max, lat_max


# ── Géométrie ────────────────────────────────────────────────────────────────

def _km_per_deg(lat: float) -> tuple[float, float]:
    """(km par degré de latitude, km par degré de longitude) à cette latitude."""
    return 110.574, 111.32 * math.cos(math.radians(lat))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Azimut géographique de (lat1,lon1) vers (lat2,lon2), 0 = nord, 90 = est."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def window_open_km(window: dict | None, bearing: float) -> float | None:
    """Distance d'eau libre dans la direction `bearing` d'après la fenêtre.

    Tolérant à la quantification : on prend le MAX du secteur visé et de ses
    deux voisins (±5°), pour ne pas éliminer une particule pile sur une arête.
    None si pas de fenêtre (plage non calculée) → l'appelant ne filtre pas."""
    if not window or not window.get("open_km"):
        return None
    open_km = window["open_km"]
    step = window.get("step_deg", STEP_DEG)
    n = len(open_km)
    i = int(round(bearing / step)) % n
    return max(open_km[(i - 1) % n], open_km[i], open_km[(i + 1) % n])


def visible_by_sea(window: dict | None, bearing: float, dist_km: float) -> bool:
    """True si une particule à (bearing, dist_km) peut atteindre la plage par la
    mer sans croiser la terre. Sans fenêtre → True (pas de filtrage)."""
    open_km = window_open_km(window, bearing)
    if open_km is None:
        return True
    cap = window.get("max_km", MAX_KM)
    # au-delà du plafond, visible si la direction est ouverte jusqu'au plafond
    return open_km >= min(dist_km, cap - 1e-6)


# ── Calcul des fenêtres (nécessite shapely + roaring_landmask) ──────────────

def load_land(bbox: tuple = ANTILLES_BBOX):
    """Polygones de terre GSHHG pleine résolution, découpés à la boîte."""
    from shapely import wkb
    from shapely.geometry import box
    import roaring_landmask as rl
    land = wkb.loads(rl.Shapes.wkb(rl.LandmaskProvider.Gshhg))
    return land.intersection(box(*bbox))


def snap_to_coast(lat: float, lon: float, land) -> tuple[float, float, float]:
    """Projette le point sur le trait de côte le plus proche → (lat, lon, km)."""
    from shapely.geometry import Point
    from shapely.ops import nearest_points
    p = Point(lon, lat)
    q = nearest_points(land.boundary, p)[0]
    klat, klon = _km_per_deg(lat)
    d = math.hypot((q.x - lon) * klon, (q.y - lat) * klat)
    return q.y, q.x, round(d, 3)


def ray_open_km(lat: float, lon: float, bearing: float, land,
                max_km: float = MAX_KM, tol_km: float = TOL_KM) -> float:
    """Longueur (km) du premier segment d'eau le long du rayon, 0 si terre."""
    from shapely.geometry import LineString
    klat, klon = _km_per_deg(lat)
    dx, dy = math.sin(math.radians(bearing)), math.cos(math.radians(bearing))
    end = (lon + dx * max_km / klon, lat + dy * max_km / klat)
    water = LineString([(lon, lat), end]).difference(land)
    if water.is_empty:
        return 0.0
    parts = list(getattr(water, "geoms", [water]))
    best = 0.0
    for part in parts:
        c = list(part.coords)
        d0 = math.hypot((c[0][0] - lon) * klon, (c[0][1] - lat) * klat)
        d1 = math.hypot((c[-1][0] - lon) * klon, (c[-1][1] - lat) * klat)
        if min(d0, d1) <= tol_km:          # segment qui part de la plage
            best = max(best, max(d0, d1))
    return round(min(best, max_km), 1)


def _facing(bearings: list, open_km: list) -> float | None:
    """Direction moyenne d'ouverture (moyenne circulaire pondérée par la distance)."""
    sx = sum(d * math.sin(math.radians(b)) for b, d in zip(bearings, open_km))
    sy = sum(d * math.cos(math.radians(b)) for b, d in zip(bearings, open_km))
    return (math.degrees(math.atan2(sx, sy)) + 360.0) % 360.0 if (sx or sy) else None


def compute_window(lat: float, lon: float, land, step_deg: int = STEP_DEG,
                   max_km: float = MAX_KM) -> dict:
    """Fenêtre d'une plage. Les rayons partent d'un « œil » posé EYE_KM au large
    du point de côte, dans la direction d'ouverture : depuis le trait de côte
    lui-même, un rayon le long du rivage rase la terre et déclare cachée une
    particule qui flotte pourtant dans la même baie."""
    from shapely.geometry import Point
    s_lat, s_lon, snap_km = snap_to_coast(lat, lon, land)
    bearings = list(range(0, 360, step_deg))
    # 1er passage depuis la côte : sert uniquement à trouver la direction du large
    open_km = [ray_open_km(s_lat, s_lon, b, land, max_km) for b in bearings]
    facing = _facing(bearings, open_km)
    e_lat, e_lon = s_lat, s_lon
    if facing is not None:
        klat, klon = _km_per_deg(s_lat)
        c_lat = s_lat + math.cos(math.radians(facing)) * EYE_KM / klat
        c_lon = s_lon + math.sin(math.radians(facing)) * EYE_KM / klon
        if not land.contains(Point(c_lon, c_lat)):      # l'œil doit être dans l'eau
            e_lat, e_lon = c_lat, c_lon
            open_km = [ray_open_km(e_lat, e_lon, b, land, max_km, tol_km=0.05) for b in bearings]
            facing = _facing(bearings, open_km)
    return {
        "step_deg": step_deg, "max_km": max_km, "open_km": open_km,
        "snap_km": snap_km, "snap_lat": round(s_lat, 5), "snap_lon": round(s_lon, 5),
        "eye_lat": round(e_lat, 5), "eye_lon": round(e_lon, 5), "eye_km": EYE_KM,
        "facing_deg": round(facing) if facing is not None else None,
        "open_frac": round(sum(1 for d in open_km if d >= OPEN_MIN_KM) / len(open_km), 2),
        "computed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ── Base ─────────────────────────────────────────────────────────────────────

def ensure_column(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(beaches_config)")}
    if "exposure_json" not in cols:
        conn.execute("ALTER TABLE beaches_config ADD COLUMN exposure_json TEXT")
        conn.commit()


def compass(deg: float | None) -> str:
    if deg is None:
        return "—"
    pts = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
           "S", "SSO", "SO", "OSO", "O", "ONO", "NO", "NNO"]
    return pts[int(round(deg / 22.5)) % 16]


def describe(window: dict) -> str:
    """Secteurs ouverts (≥ 5 km) en texte : « 300°–60° (NO→NE) »."""
    open_km, step = window["open_km"], window["step_deg"]
    n = len(open_km)
    flags = [d >= OPEN_MIN_KM for d in open_km]
    if not any(flags):
        return "aucune ouverture ≥ 5 km"
    if all(flags):
        return "ouvert tout horizon"
    # trouver les plages contiguës (circulaires)
    start = next(i for i in range(n) if flags[i] and not flags[(i - 1) % n])
    sectors, i, seen = [], start, 0
    while seen < n:
        if flags[i]:
            j = i
            while flags[(j + 1) % n] and (j + 1 - i) < n:
                j += 1
            sectors.append(((i % n) * step, ((j + 1) % n) * step))
            seen += j - i + 1
            i = (j + 1) % n
        else:
            seen += 1
            i = (i + 1) % n
    return ", ".join(f"{a:03d}°–{b:03d}° ({compass(a)}→{compass(b)})" for a, b in sectors)


def run(mode: str, dry_run: bool = False, name_filter: str | None = None) -> int:
    conn = sqlite3.connect(DB_PATH)
    ensure_column(conn)
    sql = "SELECT id, island, name, lat, lon, exposure_json FROM beaches_config WHERE active = 1"
    rows = conn.execute(sql).fetchall()
    if mode == "missing":
        rows = [r for r in rows if not r[5]]
    if name_filter:
        rows = [r for r in rows if name_filter.lower() in r[2].lower()]
    if not rows:
        print("Rien à calculer.")
        conn.close()
        return 0

    print(f"→ chargement du trait de côte GSHHG…", flush=True)
    land = load_land()
    n_done = 0
    print(f"{'Île':<14}{'Plage':<22}{'côte':>6} {'ouvert':>7} {'face':>6}  secteurs ouverts (≥ 5 km)")
    for bid, island, name, lat, lon, _old in rows:
        w = compute_window(lat, lon, land)
        print(f"{island:<14}{name:<22}{w['snap_km']:5.2f}k {100 * w['open_frac']:5.0f}% "
              f"{compass(w['facing_deg']):>6}  {describe(w)}")
        if w["snap_km"] > 1.0:
            print(f"{'':36}⚠️  point du catalogue à {w['snap_km']:.1f} km de la côte — vérifier lat/lon")
        if not dry_run:
            conn.execute("UPDATE beaches_config SET exposure_json = ? WHERE id = ?",
                         (json.dumps(w), bid))
        n_done += 1
    if not dry_run:
        conn.commit()
        print(f"\n✅ {n_done} fenêtres écrites dans beaches_config.exposure_json")
    conn.close()
    return n_done


def ensure_exposures(db_path: Path = DB_PATH, verbose: bool = True) -> int:
    """Calcule les fenêtres manquantes (plages ajoutées via /admin). Appelé par
    beaches.py ; ne charge GSHHG que s'il manque quelque chose. Jamais bloquant."""
    try:
        conn = sqlite3.connect(db_path)
        ensure_column(conn)
        missing = conn.execute(
            "SELECT COUNT(*) FROM beaches_config WHERE active = 1 "
            "AND (exposure_json IS NULL OR exposure_json = '')").fetchone()[0]
        conn.close()
        if not missing:
            return 0
        if verbose:
            print(f"  🧭 {missing} plage(s) sans fenêtre d'exposition — calcul…")
        global DB_PATH
        DB_PATH = db_path
        return run("missing")
    except Exception as e:          # noqa: BLE001 — ne doit jamais casser le scoring
        if verbose:
            print(f"  [warn] fenêtres d'exposition non calculées : {e}", file=sys.stderr)
        return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true", help="(re)calcule toutes les plages actives")
    g.add_argument("--missing", action="store_true", help="seulement les plages sans fenêtre")
    g.add_argument("--show", metavar="NOM", help="affiche la rose stockée d'une plage")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.show:
        conn = sqlite3.connect(DB_PATH)
        ensure_column(conn)
        rows = conn.execute(
            "SELECT island, name, exposure_json FROM beaches_config WHERE name LIKE ?",
            (f"%{args.show}%",)).fetchall()
        conn.close()
        for island, name, js in rows:
            if not js:
                print(f"{island} / {name} : pas de fenêtre calculée")
                continue
            w = json.loads(js)
            print(f"\n{island} / {name} — côte à {w['snap_km']} km, face {compass(w['facing_deg'])} "
                  f"({w['facing_deg']}°), {100 * w['open_frac']:.0f} % d'horizon ouvert")
            print(f"  secteurs : {describe(w)}")
            for b, d in zip(range(0, 360, w["step_deg"]), w["open_km"]):
                bar = "█" * int(min(d, 40) / 2)
                print(f"  {b:03d}° {compass(b):>4} {d:5.1f} km {bar}")
        return 0
    run("all" if args.all else "missing", dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
