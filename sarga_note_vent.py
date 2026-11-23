#!/usr/bin/env python3
"""
sarga_note_vent.py
==================
Note de plage EN PARALLÈLE (jamais affichée, jamais utilisée par les alertes) :
« sargasses vues par satellite à proximité » × « vent qui porte vers la baie ».

Pourquoi ce script
------------------
La note locale de beaches.py compte les particules dans un rayon de 2 km, alors
que les particules sont espacées d'environ 5 km : elle vaut 0 presque tous les
jours. Mesuré le 2026-11-22 contre la webcam de Flamands (218 jours, 20 jours
de radeaux), la note ci-dessous trie les jours de radeaux 85 fois sur 100
(marge 76 à 92), contre 59 pour la note locale.

Mais cette mesure utilisait un vent d'ARCHIVE. Ce script calcule la même note
chaque jour avec le vent réellement disponible (prévu la veille, constaté le
jour même) et la range dans SA table (`note_vent_parallele`), pour que la
webcam la juge au fil des semaines avant toute mise en service.

Ce qu'il calcule, pour chaque plage de PLAGES
---------------------------------------------
1. Satellite : dernier composite AFAI 7 jours publié, nombre de pixels au-dessus
   du seuil à moins de 25, 50 et 100 km de la plage (même grille que la mesure).
2. Vent : direction moyenne (moyenne vectorielle) des 24 h qui précèdent 14 h UTC,
   heure de la capture webcam. Source Open-Meteo, sans clé.
3. Note = pixels à moins de 50 km si le vent vient de `vent_max_deg` ou moins,
   sinon 0. Tous les ingrédients sont gardés pour pouvoir rejuger plus tard.
Deux lignes par passage : le jour même (avance 0) et le lendemain (avance 1,
avec le vent PRÉVU).

Règles respectées
-----------------
- N'écrit que dans `note_vent_parallele`. Ne lit que `beaches_config` et
  `webcam_truth`. Ne touche ni aux scores, ni à la carte, ni aux alertes.
- Aucun message Telegram, aucune clé.

Limites connues
---------------
- Une seule plage : le vent favorable (90° ou moins) ne vaut que pour Flamands,
  qui ouvre au nord-ouest.
- Si le satellite ou le vent ne répond pas, aucune ligne n'est écrite ce jour-là
  et le script sort en code 1 (visible dans logs/note_vent.log et dans --bilan).

Usage
-----
  python sarga_note_vent.py --run      # calcule et range les deux lignes du jour
  python sarga_note_vent.py --essai    # calcule et affiche, n'écrit rien
  python sarga_note_vent.py --bilan    # compare ce qui est rangé à la webcam
"""

from __future__ import annotations

import argparse
import logging
import math
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import requests

log = logging.getLogger("note_vent")

DB_PATH = Path(__file__).parent / "sargassum_data.db"
VERSION = "sat50-x-vent-v1"
SEUIL_AFAI = 1e-4            # même seuil que le semis de la simulation
RAYONS_KM = (25, 50, 100)
RAYON_NOTE_KM = 50
HEURE_CAPTURE = 14           # la webcam est lue à 14 h UTC
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SargassumCollector/1.0)"}
ERDDAP = "https://cwcgom.aoml.noaa.gov/erddap/griddap/noaa_aoml_atlantic_oceanwatch_AFAI_7D.nc"
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"

# Plage du catalogue → vent favorable et zone satellite (un pixel sur trois, environ 5 km).
# La zone et le pas sont ceux de la mesure du 2026-11-22 : les changer décale la grille
# et rend les comptes de pixels incomparables avec cette mesure.
PLAGES: dict[str, dict] = {
    "Flamands": {"vent_max_deg": 90.0, "zone": (16.4, 19.4, -64.4, -61.3), "pas": 3},
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS note_vent_parallele (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    target_date    TEXT NOT NULL,      -- AAAA-MM-JJ du jour jugé (capture de 14 h UTC)
    beach_name     TEXT NOT NULL,      -- nom du catalogue beaches_config
    lead_days      INTEGER NOT NULL,   -- 0 = calculé le jour même, 1 = calculé la veille
    composite_date TEXT NOT NULL,      -- date du composite satellite utilisé
    n25            INTEGER NOT NULL,   -- pixels au-dessus du seuil à moins de 25 km
    n50            INTEGER NOT NULL,
    n100           INTEGER NOT NULL,
    lisible50      REAL NOT NULL,      -- part de pixels lisibles (non masqués) à moins de 50 km
    wind_dir       REAL NOT NULL,      -- direction d'où vient le vent moyen des 24 h, en degrés
    wind_kn        REAL NOT NULL,      -- vitesse moyenne des 24 h, en nœuds
    porte          INTEGER NOT NULL,   -- 1 si le vent porte vers la baie
    note           REAL NOT NULL,      -- n50 si le vent porte, sinon 0
    version        TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    UNIQUE (beach_name, target_date, lead_days)
);
"""


def vent_moyen(vitesses: list[float], directions: list[float]) -> tuple[float, float]:
    """Direction (moyenne vectorielle, 350° et 10° donnent 0°) et vitesse moyenne."""
    if not vitesses or len(vitesses) != len(directions):
        raise ValueError("série de vent vide ou incohérente")
    u = sum(v * math.sin(math.radians(d)) for v, d in zip(vitesses, directions)) / len(vitesses)
    w = sum(v * math.cos(math.radians(d)) for v, d in zip(vitesses, directions)) / len(vitesses)
    return math.degrees(math.atan2(u, w)) % 360, sum(vitesses) / len(vitesses)


def distances_km(lat0: float, lon0: float, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    """Distance de chaque point de la grille à la plage (approximation plane, suffisante à 100 km)."""
    lo, la = np.meshgrid(lons.astype(float), lats.astype(float))
    return np.hypot((lo - lon0) * math.cos(math.radians(lat0)), la - lat0) * 111.2


def compter_pixels(afai: np.ndarray, dist: np.ndarray) -> dict:
    """Pixels au-dessus du seuil par rayon, et part de pixels lisibles à moins de 50 km."""
    with np.errstate(invalid="ignore"):
        det = afai > SEUIL_AFAI
    proche = dist < RAYON_NOTE_KM
    if not proche.any():
        raise ValueError("aucun pixel de la zone à moins de 50 km de la plage")
    r = {f"n{k}": int((det & (dist < k)).sum()) for k in RAYONS_KM}
    r["lisible50"] = round(float(np.isfinite(afai[proche]).mean()), 3)
    return r


def note(n50: int, wind_dir: float, vent_max_deg: float) -> tuple[int, float]:
    """(le vent porte ?, note). Note continue : pas de seuil d'alerte ici."""
    porte = int(wind_dir <= vent_max_deg)
    return porte, float(n50 * porte)


def lire_satellite(zone: tuple[float, float, float, float], pas: int) -> tuple[str, np.ndarray, np.ndarray, np.ndarray]:
    """Dernier composite 7 jours publié sur la zone : (date, latitudes, longitudes, AFAI)."""
    import xarray as xr
    la0, la1, lo0, lo1 = zone
    q = f"AFAI[(last)][({la0}):{pas}:({la1})][({lo0}):{pas}:({lo1})]"
    r = requests.get(ERDDAP + "?" + q.replace("[", "%5B").replace("]", "%5D"), headers=HEADERS, timeout=120)
    r.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=".nc") as f:
        f.write(r.content)
        f.flush()
        with xr.open_dataset(f.name) as ds:
            jour = str(ds["time"].values[0])[:10]
            return jour, ds["latitude"].values, ds["longitude"].values, ds["AFAI"].values[0].astype("float32")


def lire_vent(lat: float, lon: float) -> dict[str, tuple[float, float]]:
    """Vent horaire (constaté puis prévu) autour d'aujourd'hui : heure ISO → (nœuds, degrés)."""
    r = requests.get(OPEN_METEO, timeout=60, headers=HEADERS, params={
        "latitude": round(lat, 2), "longitude": round(lon, 2), "hourly": "wind_speed_10m,wind_direction_10m",
        "wind_speed_unit": "kn", "timezone": "UTC", "past_days": 2, "forecast_days": 3})
    r.raise_for_status()
    h = r.json()["hourly"]
    return {t: (v, d) for t, v, d in zip(h["time"], h["wind_speed_10m"], h["wind_direction_10m"])
            if v is not None and d is not None}


def vent_des_24h(vent: dict[str, tuple[float, float]], jour: str) -> tuple[float, float]:
    """Vent moyen des 24 h qui précèdent la capture de `jour`. Refuse une série trouée."""
    fin = datetime.strptime(jour, "%Y-%m-%d").replace(hour=HEURE_CAPTURE)
    heures = [(fin - timedelta(hours=k)).strftime("%Y-%m-%dT%H:00") for k in range(24, 0, -1)]
    manque = [h for h in heures if h not in vent]
    if manque:
        raise ValueError(f"vent manquant pour {len(manque)} heure(s) sur 24 avant le {jour}")
    return vent_moyen([vent[h][0] for h in heures], [vent[h][1] for h in heures])


def calculer(conn: sqlite3.Connection, aujourdhui: str) -> list[dict]:
    """Les lignes du jour (avance 0) et du lendemain (avance 1) pour chaque plage."""
    lignes = []
    demain = (datetime.strptime(aujourdhui, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    for plage, reglage in PLAGES.items():
        pos = conn.execute("SELECT lat, lon FROM beaches_config WHERE name = ?", (plage,)).fetchone()
        if pos is None:
            raise ValueError(f"plage {plage} absente de beaches_config")
        lat, lon = float(pos[0]), float(pos[1])
        jour_sat, lats, lons, afai = lire_satellite(reglage["zone"], reglage["pas"])
        age = (datetime.strptime(aujourdhui, "%Y-%m-%d") - datetime.strptime(jour_sat, "%Y-%m-%d")).days
        if not 0 <= age <= 5:
            raise ValueError(f"composite satellite daté du {jour_sat} : trop vieux ou dans le futur")
        comptes = compter_pixels(afai, distances_km(lat, lon, lats, lons))
        vent = lire_vent(lat, lon)
        for avance, cible in ((0, aujourdhui), (1, demain)):
            wind_dir, wind_kn = vent_des_24h(vent, cible)
            porte, valeur = note(comptes[f"n{RAYON_NOTE_KM}"], wind_dir, reglage["vent_max_deg"])
            lignes.append({"target_date": cible, "beach_name": plage, "lead_days": avance, "composite_date": jour_sat,
                           **comptes, "wind_dir": round(wind_dir, 1), "wind_kn": round(wind_kn, 1),
                           "porte": porte, "note": valeur})
    return lignes


def enregistrer(conn: sqlite3.Connection, lignes: list[dict]) -> None:
    """Range les lignes en une transaction. Relancer le même jour remplace, sans doublon."""
    maintenant = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with conn:
        for l in lignes:
            conn.execute(
                """INSERT INTO note_vent_parallele (target_date, beach_name, lead_days, composite_date, n25, n50, n100,
                       lisible50, wind_dir, wind_kn, porte, note, version, created_at)
                   VALUES (:target_date, :beach_name, :lead_days, :composite_date, :n25, :n50, :n100,
                       :lisible50, :wind_dir, :wind_kn, :porte, :note, :version, :created_at)
                   ON CONFLICT (beach_name, target_date, lead_days) DO UPDATE SET
                       composite_date = excluded.composite_date, n25 = excluded.n25, n50 = excluded.n50,
                       n100 = excluded.n100, lisible50 = excluded.lisible50, wind_dir = excluded.wind_dir,
                       wind_kn = excluded.wind_kn, porte = excluded.porte, note = excluded.note,
                       version = excluded.version, created_at = excluded.created_at""",
                {**l, "version": VERSION, "created_at": maintenant})


def tri(notes: list[float], verites: list[int]) -> float | None:
    """Chance qu'un jour de radeaux ait une note plus haute qu'un jour sans (0,5 = hasard)."""
    p = [x for x, y in zip(notes, verites) if y]
    q = [x for x, y in zip(notes, verites) if not y]
    if not p or not q:
        return None
    return sum((a > b) + 0.5 * (a == b) for a in p for b in q) / (len(p) * len(q))


def bilan(conn: sqlite3.Connection) -> None:
    """Ce qui est rangé, comparé aux verdicts sûrs de la webcam. Affiche aussi les jours manqués."""
    for plage in PLAGES:
        for avance in (0, 1):
            rows = conn.execute(
                """SELECT n.target_date, n.note, n.porte, n.n50, w.verdict FROM note_vent_parallele n
                   LEFT JOIN webcam_truth w ON w.beach_name = n.beach_name AND w.capture_date = n.target_date
                   WHERE n.beach_name = ? AND n.lead_days = ? ORDER BY n.target_date""", (plage, avance)).fetchall()
            if not rows:
                print(f"{plage}, avance {avance} : aucune ligne rangée")
                continue
            debut, fin = rows[0][0], rows[-1][0]
            attendus = (datetime.strptime(fin, "%Y-%m-%d") - datetime.strptime(debut, "%Y-%m-%d")).days + 1
            juges = [(r[1], int(r[4] == "radeaux")) for r in rows if r[4] in ("radeaux", "rien")]
            a = tri([x for x, _ in juges], [y for _, y in juges])
            print(f"{plage}, avance {avance} : {len(rows)} jours rangés sur {attendus} du {debut} au {fin} | "
                  f"{len(juges)} jugés par la webcam dont {sum(y for _, y in juges)} avec radeaux | "
                  f"tri {'pas encore mesurable' if a is None else f'{a:.2f}'} (0,50 = hasard)")
            for r in rows[-5:]:
                print(f"    {r[0]}  note {r[1]:5.0f}  pixels à 50 km {r[3]:3d}  vent {'porte' if r[2] else 'ne porte pas'}  webcam : {r[4] or 'pas encore lue'}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", action="store_true", help="calcule et range les lignes du jour et du lendemain")
    ap.add_argument("--essai", action="store_true", help="calcule et affiche, sans rien écrire")
    ap.add_argument("--bilan", action="store_true", help="compare ce qui est rangé à la webcam")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    if not (args.run or args.essai or args.bilan):
        ap.print_help()
        return 2

    conn = sqlite3.connect(DB_PATH, timeout=60)
    code = 0
    try:
        if args.run or args.essai:
            try:
                # Le réseau est lu AVANT d'ouvrir la moindre écriture : pas d'attente dans un verrou.
                lignes = calculer(conn, datetime.now(timezone.utc).strftime("%Y-%m-%d"))
            except Exception as e:
                # Un cron qui échoue ne doit pas rester muet : ligne d'erreur datée + code 1
                log.error("calcul impossible, aucune ligne écrite : %s", e)
                return 1
            for l in lignes:
                log.info("%s %s avance %d : note %.0f (pixels 25/50/100 km : %d/%d/%d, composite du %s, vent %.0f° %.1f nd, %s)",
                         l["beach_name"], l["target_date"], l["lead_days"], l["note"], l["n25"], l["n50"], l["n100"],
                         l["composite_date"], l["wind_dir"], l["wind_kn"], "porte" if l["porte"] else "ne porte pas")
            if args.run:
                conn.executescript(SCHEMA)
                enregistrer(conn, lignes)
                log.info("%d lignes rangées dans note_vent_parallele", len(lignes))
        if args.bilan:
            conn.executescript(SCHEMA)
            bilan(conn)
    finally:
        conn.close()
    return code


if __name__ == "__main__":
    sys.exit(main())
