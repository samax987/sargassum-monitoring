#!/usr/bin/env python3
"""
sarga_webcam_truth.py
=====================
Vérité terrain automatique : lit la capture webcam du jour sur une plage et dit
s'il y a des radeaux de sargasses dans l'eau devant elle.

Pourquoi ce script
------------------
La vérification des prévisions (sarga_verification.py) n'avait pour juge que
les observations humaines : 8 sur Flamands en 9 mois. Impossible de dire si le
« dernier kilomètre » s'améliore avec si peu de points. La webcam de Flamands
donne une image par jour, tous les jours : ce script en tire un verdict
quotidien, rangé dans SA table (`webcam_truth`).

Règle d'or respectée : ce script n'écrit JAMAIS dans `beach_observations`.
Une lecture automatique n'est pas un signalement humain validé.

Comment ça lit l'image (sans IA, numpy + PIL seulement)
-------------------------------------------------------
1. Le sable est repéré par sa couleur, puis le trait de côte colonne par colonne.
2. Dans l'eau, à plus de 30 px du rivage (hors déferlement), on cherche des
   taches nettement plus brunes ET pas plus claires que l'eau qui les entoure.
   Teinte RELATIVE à l'eau voisine : un critère absolu « brun » prenait les
   rochers et ratait les radeaux en eau turquoise (versions 1 et 2 abandonnées).
3. Verdict d'après la surface trouvée (`large_px`) :
     trait de côte trouvé sur < 85 % des colonnes → 'illisible' (voile, cadrage)
     image identique à celle de la veille       → 'illisible' (webcam figée)
     large_px ≥ 300                              → 'radeaux'
     large_px < 100                              → 'rien'
     entre les deux                              → 'douteux'
   Seuls 'radeaux' et 'rien' servent de vérité ; le reste est ignoré.

Ce que ça vaut (test à l'aveugle, 2026-11-09)
---------------------------------------------
Un lecteur (IA) a noté 42 jours sans connaître ni la date ni le détecteur,
jours jamais utilisés pour régler les seuils :
  'radeaux' : 10 jours, dont 9 avec des sargasses dans l'eau (8 nettes)
  'rien'    : 25 jours, dont 24 sans arrivage notable (17 vraiment vides,
              7 avec « un peu », 1 arrivage net manqué)
  7 jours sans verdict (2 douteux, 5 illisibles)
À comparer au hasard : 12 jours sur 42 (29 %) avaient un arrivage net.
Réserve honnête : le seuil « 85 % des colonnes » a été choisi APRÈS avoir vu
ce test ; les chiffres sont donc un peu optimistes.

Limites connues
---------------
- Ne voit que les radeaux NETS dans l'eau. Les « un peu » passent en 'rien'
  et les dépôts sur le sable ne sont pas lus.
- Un gros bateau sombre au mouillage peut compter pour une tache (saison des
  yachts, novembre à avril) : d'où la zone 'douteux' entre 100 et 300 px.
- Les masques (îlot, pointe) sont propres au cadrage de la caméra Flamand_Beach.
  Si la caméra est déplacée ou zoomée, re-valider avant de faire confiance.
- Saint-Jean_Plaine a été testée et écartée : sable illisible 22 fois sur 22.

Usage
-----
  python sarga_webcam_truth.py --today            # capture du jour → base
  python sarga_webcam_truth.py --backfill         # tout l'historique → base
  python sarga_webcam_truth.py --image x.jpg      # lit une image, n'écrit rien
  python sarga_webcam_truth.py --report           # bilan de ce qui est en base
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from PIL import Image

log = logging.getLogger("webcam_truth")

DB_PATH = Path(__file__).parent / "sargassum_data.db"
DETECTOR_VERSION = "couleur-v4"

# Caméras validées par un test à l'aveugle : nom caméra → (île, plage du catalogue)
CAMERAS: dict[str, tuple[str, str]] = {"Flamand_Beach": ("Saint-Barth", "Flamands")}

# Seuils du verdict (réglés sur 33 jours, vérifiés sur 42 autres)
SEUIL_COLS = 0.85       # part minimale de colonnes où le trait de côte est trouvé
SEUIL_RADEAUX = 300     # px de taches brunes au large à partir desquels on dit 'radeaux'
SEUIL_RIEN = 100        # px en dessous desquels on dit 'rien'

# Géométrie de lecture (image ramenée à 1280×720)
X0, X1, Y0 = 60, 1100, 140

SCHEMA = """
CREATE TABLE IF NOT EXISTS webcam_truth (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    capture_date     TEXT NOT NULL,     -- AAAA-MM-JJ (capture de 14 h UTC = 10 h locale)
    island           TEXT NOT NULL,
    beach_name       TEXT NOT NULL,     -- nom du catalogue beaches_config
    camera_name      TEXT NOT NULL,
    file_path        TEXT,
    large_px         INTEGER,           -- taches brunes dans l'eau (sert au verdict)
    cote_px          INTEGER,           -- brun contre le rivage (information seulement)
    cols             REAL,              -- part de colonnes où le trait de côte est trouvé
    verdict          TEXT NOT NULL,     -- radeaux | rien | douteux | illisible
    note             TEXT,
    detector_version TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    UNIQUE (camera_name, capture_date)
);
"""


# ── Lecture d'une image ──────────────────────────────────────────────────────

def box_mean(a: np.ndarray, r: int) -> np.ndarray:
    """Moyenne glissante sur un carré de côté 2r+1 (image intégrale, sans scipy)."""
    m = np.pad(a.astype(np.float32), r, mode="edge")
    c = np.pad(m.cumsum(0).cumsum(1), ((1, 0), (1, 0)))
    k = 2 * r + 1
    return (c[k:, k:] - c[:-k, k:] - c[k:, :-k] + c[:-k, :-k]) / (k * k)


def analyse(path: str | Path) -> dict:
    """Mesure une image : {'large_px', 'cote_px', 'cols'}."""
    a = np.asarray(Image.open(path).convert("RGB").resize((1280, 720))).astype(np.float32) / 255
    r, g, b = a[..., 0], a[..., 1], a[..., 2]
    v = a.max(-1)
    sat = (v - a.min(-1)) / np.maximum(v, 1e-6)
    h, w = v.shape
    yy, xx = np.mgrid[0:h, 0:w]

    # Sable : clair, tirant sur le jaune, peu saturé
    sable = (v > 0.70) & ((r - b) > 0.08) & (sat < 0.40)
    # Trait de côte : dans chaque colonne, première rangée où 10 px sur 15 sont du sable
    run = np.cumsum(sable, 0).astype(np.int32)
    win = 15
    dens = run[win:, :] - run[:-win, :]
    ys = np.full(w, -1)
    for x in range(X0, X1):
        idx = np.nonzero(dens[Y0:620, x] >= 10)[0]
        if len(idx):
            ys[x] = Y0 + idx[0]
    # Lissage : médiane sur ±20 colonnes, pour ignorer un parasol ou un rocher
    ysm = ys.copy()
    for x in range(X0, X1):
        seg = ys[max(X0, x - 20):min(X1, x + 21)]
        seg = seg[seg > 0]
        ysm[x] = int(np.median(seg)) if len(seg) >= 10 else -1
    valid = ysm > 0

    # Le large : à plus de 30 px du rivage (hors déferlement), hors îlot et pointe rocheuse
    large = (yy < (ysm[None, :] - 30)) & valid[None, :] & (yy > 30) & (xx >= 120) \
        & ~((xx < 420) & (yy < 125)) & ~((xx > 820) & (yy < 230))
    # Teinte normalisée : positive = brun/roux, très négative = bleu
    nd = (r - b) / (r + g + b + 0.05)
    nd_loc = box_mean(nd, 30)
    v_loc = box_mean(v, 30)
    cand = large & (nd > -0.12) & ((nd - nd_loc) > 0.12) & (v < v_loc + 0.03) & (v > 0.15) & (v < 0.75)
    radeau = cand & (box_mean(cand, 2) >= 0.30)      # écarte les pixels isolés (bruit JPEG, reflets)

    # Brun contre le rivage : gardé pour information, trop sensible au cadrage pour juger
    brun = ((r - b) > 0.04) & (r >= g - 0.01) & (v > 0.20) & (v < 0.62) & (sat < 0.55)
    bande = (yy >= (ysm[None, :] - 30)) & (yy < (ysm[None, :] - 4)) & valid[None, :] \
        & (xx >= 120) & ~((xx > 820) & (yy < 230))
    cote = brun & bande & (box_mean(brun, 4) >= 0.35)

    return {"large_px": int(radeau.sum()), "cote_px": int(cote.sum()),
            "cols": round(float(valid[X0:X1].mean()), 2)}


def verdict(mesure: dict) -> str:
    """Verdict d'après les mesures : radeaux | rien | douteux | illisible."""
    if mesure["cols"] < SEUIL_COLS:
        return "illisible"
    if mesure["large_px"] >= SEUIL_RADEAUX:
        return "radeaux"
    if mesure["large_px"] < SEUIL_RIEN:
        return "rien"
    return "douteux"


def empreinte(path: str | Path) -> str:
    """Empreinte du fichier, pour repérer une webcam figée (même image deux jours de suite)."""
    return hashlib.sha1(Path(path).read_bytes()).hexdigest()


# ── Base ─────────────────────────────────────────────────────────────────────

def captures(conn: sqlite3.Connection, camera: str) -> list[tuple[str, str]]:
    """[(date, chemin)] : la dernière capture réussie de chaque jour, dans l'ordre."""
    rows = conn.execute(
        """SELECT substr(captured_at, 1, 10) AS jour, file_path, MAX(captured_at)
           FROM webcam_captures
           WHERE camera_name = ? AND success = 1 AND file_path IS NOT NULL
           GROUP BY jour ORDER BY jour""",
        (camera,),
    ).fetchall()
    return [(jour, chemin) for jour, chemin, _ in rows]


def lire_jour(jour: str, chemin: str, chemin_veille: str | None) -> tuple[dict, str, str | None]:
    """Mesure + verdict d'un jour. Retourne (mesure, verdict, note)."""
    mesure = analyse(chemin)
    if chemin_veille and Path(chemin_veille).exists() and empreinte(chemin) == empreinte(chemin_veille):
        return mesure, "illisible", "image identique à la veille (webcam figée)"
    return mesure, verdict(mesure), None


def traiter(conn: sqlite3.Connection, camera: str, seulement: str | None = None) -> Counter:
    """Lit les captures d'une caméra et range les verdicts. `seulement` = une date précise."""
    island, beach = CAMERAS[camera]
    liste = captures(conn, camera)
    bilan: Counter = Counter()
    for i, (jour, chemin) in enumerate(liste):
        if seulement and jour != seulement:
            continue
        if not Path(chemin).exists():
            log.warning("%s %s : fichier absent du disque (%s)", camera, jour, chemin)
            bilan["fichier absent"] += 1
            continue
        veille = liste[i - 1][1] if i > 0 else None
        try:
            mesure, v, note = lire_jour(jour, chemin, veille)
        except Exception as exc:  # image tronquée, format inattendu : on note et on continue
            log.warning("%s %s : image illisible (%s)", camera, jour, exc)
            mesure, v, note = {"large_px": None, "cote_px": None, "cols": None}, "illisible", f"erreur : {exc}"
        conn.execute(
            """INSERT INTO webcam_truth
               (capture_date, island, beach_name, camera_name, file_path, large_px, cote_px,
                cols, verdict, note, detector_version, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT (camera_name, capture_date) DO UPDATE SET
                 file_path = excluded.file_path, large_px = excluded.large_px,
                 cote_px = excluded.cote_px, cols = excluded.cols, verdict = excluded.verdict,
                 note = excluded.note, detector_version = excluded.detector_version,
                 created_at = excluded.created_at""",
            (jour, island, beach, camera, chemin, mesure["large_px"], mesure["cote_px"],
             mesure["cols"], v, note, DETECTOR_VERSION,
             datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
        )
        bilan[v] += 1
        log.info("%s %s : %s (large=%s px, colonnes=%s)%s", camera, jour, v,
                 mesure["large_px"], mesure["cols"], f" — {note}" if note else "")
    conn.commit()
    return bilan


def rapport(conn: sqlite3.Connection) -> None:
    """Affiche ce qui est en base : répartition des verdicts et 10 derniers jours."""
    for camera, (island, beach) in CAMERAS.items():
        rows = conn.execute(
            "SELECT verdict, COUNT(*) FROM webcam_truth WHERE camera_name = ? GROUP BY verdict",
            (camera,)).fetchall()
        total = sum(n for _, n in rows)
        print(f"\n{camera} → {beach} ({island}) : {total} jours lus")
        for v, n in sorted(rows, key=lambda x: -x[1]):
            print(f"  {v:<10} {n:>4}")
        print("  Derniers jours :")
        for jour, v, px, cols, note in conn.execute(
                """SELECT capture_date, verdict, large_px, cols, note FROM webcam_truth
                   WHERE camera_name = ? ORDER BY capture_date DESC LIMIT 10""", (camera,)):
            print(f"    {jour}  {v:<10} large={px} px  colonnes={cols}  {note or ''}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--today", action="store_true", help="lit la capture du jour (date UTC)")
    ap.add_argument("--backfill", action="store_true", help="lit tout l'historique des captures")
    ap.add_argument("--image", help="lit une image et affiche le verdict, sans rien écrire")
    ap.add_argument("--report", action="store_true", help="affiche le bilan de la base")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.image:
        mesure = analyse(args.image)
        print(f"{args.image} : {verdict(mesure)}  {mesure}")
        return 0
    if not (args.today or args.backfill or args.report):
        ap.print_help()
        return 2

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.executescript(SCHEMA)
    code = 0
    if args.today or args.backfill:
        jour = datetime.now(timezone.utc).strftime("%Y-%m-%d") if args.today else None
        for camera in CAMERAS:
            bilan = traiter(conn, camera, seulement=jour)
            if not sum(bilan.values()):
                # Un cron qui ne trouve rien ne doit pas rester muet
                log.error("%s : aucune capture à lire%s", camera, f" pour le {jour}" if jour else "")
                code = 1
            else:
                log.info("%s : %s", camera, dict(bilan))
    if args.report:
        rapport(conn)
    conn.close()
    return code


if __name__ == "__main__":
    sys.exit(main())
