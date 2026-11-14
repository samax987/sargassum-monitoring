#!/usr/bin/env python3
"""
sarga_verification.py
=====================
Vérification des prévisions plage × jour contre les observations terrain.

Pourquoi ce script
------------------
Jusqu'au 2026-11-09 le projet ne gardait ses prévisions que ~2 semaines
(purge de beach_risk_scores) et la calibration spatiale ne trouvait plus aucun
match (observations de juillet vs dérive purgée). On ne pouvait donc pas
répondre à LA question : « ce qu'on avait annoncé pour cette plage ce jour-là,
c'était vrai ou faux ? ». beaches.py archive désormais chaque prévision dans
`forecast_archive` (jamais purgé) ; ce script confronte cette archive aux
observations (`beach_observations`, alimentée par Sam et les bénévoles).

Leçon Grain (2026-10) appliquée : on ne juge jamais un prédicteur sans témoin.
Chaque échéance est comparée à deux références bêtes :
  - climatologie : toujours annoncer la classe majoritaire des observations ;
  - persistance  : annoncer ce qui a été observé la dernière fois sur la plage
                   (≤ 7 jours avant).
Si le modèle ne bat pas ces deux-là, il n'apporte rien — et on le dira.

Ce qui est mesuré, par échéance j+0 … j+5
-----------------------------------------
Vérité binaire = « présence » : observed_risk ∈ {low, medium, high}.
Prévision binaire = `presence` archivée (badge local) ≠ 'none'.
  POD       : taux de détection (arrivées annoncées / arrivées observées)
  FAR       : taux de fausses alertes (alertes à tort / alertes émises)
  Exactitude: (bons oui + bons non) / n
  PSS       : score de Peirce = POD − POFD ; 0 = hasard, 1 = parfait
  MAE rang  : écart moyen entre niveaux (none=0 … high=3), prévu vs observé
+ les mêmes pour le signal régional (`risk_level`, l'approche à 50 km).

Second juge : la webcam (depuis le 2026-11-10)
----------------------------------------------
Les observations humaines sont rares (8 sur Flamands en 9 mois). La table
`webcam_truth` (sarga_webcam_truth.py) donne un verdict par jour sur Flamands.
On ne garde que les verdicts sûrs : 'radeaux' = présence, 'rien' = absence
d'arrivage net. Ce juge est affiché et stocké À PART (verification_webcam_summary),
jamais mélangé aux observations humaines : il ne voit que les radeaux nets dans
l'eau, pas les « un peu » ni les dépôts sur le sable.

Un seul régime de modèle à la fois (depuis le 2026-11-11)
--------------------------------------------------------
Le 2026-11-10 à 12h UTC, la simulation a cessé de semer les points satellite
à moins de 15 km d'une côte (masque côtier, faux signal). Avant : presque
toutes les plages en « faible » ou « moyen » ; après : presque tout à
« aucun ». Mélanger les deux régimes dans un même score ne voudrait rien dire.
On ne juge donc que les prévisions du régime ACTUEL (colonne
forecast_archive.seed_coast_km de la prévision la plus récente), sauf option
contraire. Le régime jugé est écrit dans le rapport et dans metrics_json.

Une prévision n'est jugée que si elle a été calculée AVANT l'observation
-----------------------------------------------------------------------
Sinon une échéance « j+0 » pouvait être une prévision faite le soir pour une
capture webcam de 14h UTC le même jour, c'est-à-dire prévoir le passé.
Heure de référence : observed_at pour le terrain (comparée telle quelle, ce
qui est prudent si elle est en heure locale), captured_at de la capture pour
la webcam (14h UTC par défaut si la capture n'est pas retrouvée).

Usage
-----
  python sarga_verification.py            # calcule, affiche, stocke
  python sarga_verification.py --dry-run  # calcule et affiche seulement
  python sarga_verification.py --verbose  # détaille chaque match
  python sarga_verification.py --report   # réaffiche le dernier bilan stocké
  python sarga_verification.py --masque 0 # juge l'ancien régime (sans masque côtier)
  python sarga_verification.py --tous-regimes  # mélange tout (à éviter, pour comparaison)

Tables écrites : verification_matches (détail, remplacée à chaque run) et
verification_summary (une ligne par run × échéance, historisée).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "sargassum_data.db"

ISLANDS = ("Saint-Barth", "Saint-Martin", "Guadeloupe", "Martinique", "Marie-Galante")
RANK = {"none": 0, "low": 1, "medium": 2, "high": 3}
LEADS = (0, 1, 2, 3, 4, 5)
PERSISTENCE_MAX_DAYS = 7


# ── Correspondance noms observés ↔ catalogue de plages ──────────────────────

def normalize(name: str) -> str:
    """minuscules, sans accents, séparateurs → espaces simples."""
    s = unicodedata.normalize("NFKD", name or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = "".join(c if c.isalnum() else " " for c in s)
    return " ".join(s.split())


def load_catalog(conn: sqlite3.Connection) -> dict[str, list[str]]:
    """{île: [noms du catalogue]} depuis beaches_config (source de vérité)."""
    cat: dict[str, list[str]] = defaultdict(list)
    try:
        rows = conn.execute(
            "SELECT island, name FROM beaches_config WHERE active = 1").fetchall()
    except sqlite3.OperationalError:
        rows = []
    if not rows:   # repli : noms présents dans l'archive
        rows = conn.execute(
            "SELECT DISTINCT island, beach_name FROM forecast_archive").fetchall()
    for island, name in rows:
        cat[island].append(name)
    return cat


def match_beach(island: str, obs_name: str, catalog: dict[str, list[str]]) -> str | None:
    """Nom du catalogue correspondant à l'observation, ou None.

    Exact après normalisation, sinon le nom de catalogue le plus long contenu
    dans le nom observé (« Sainte Anne - Les Salines » → Les_Salines ;
    « Anse des Flamands » → Flamands). Jamais entre deux îles différentes."""
    n_obs = normalize(obs_name)
    if not n_obs:
        return None
    best, best_len = None, 0
    for cand in catalog.get(island, []):
        n_c = normalize(cand)
        if n_c == n_obs:
            return cand
        # substring sur frontières de mots, nom de catalogue d'au moins 4 lettres
        if len(n_c) >= 4 and f" {n_c} " in f" {n_obs} " and len(n_c) > best_len:
            best, best_len = cand, len(n_c)
    return best


# ── Schéma ───────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS verification_matches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at     TEXT NOT NULL,
    obs_id          INTEGER,
    island          TEXT,
    beach_name      TEXT,           -- nom catalogue
    obs_beach       TEXT,           -- nom tel qu'observé
    target_date     TEXT,
    observed_risk   TEXT,
    day_offset      INTEGER,
    forecast_at     TEXT,           -- computed_at de la prévision retenue
    presence        TEXT,           -- prévu, local
    risk_level      TEXT,           -- prévu, régional
    local_score     REAL,
    regional_score  REAL,
    n_stranded      INTEGER,
    persistence     TEXT            -- dernière obs ≤ 7 j avant (ou NULL)
);
CREATE TABLE IF NOT EXISTS verification_summary (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at     TEXT NOT NULL,
    day_offset      INTEGER NOT NULL,
    n               INTEGER NOT NULL,
    metrics_json    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS verification_webcam_summary (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at     TEXT NOT NULL,
    day_offset      INTEGER NOT NULL,
    n               INTEGER NOT NULL,
    metrics_json    TEXT NOT NULL
);
"""

# Verdicts webcam utilisables comme vérité (les autres, douteux/illisible, sont ignorés)
WEBCAM_TRUTH = {"radeaux": True, "rien": False}


# ── Cœur ─────────────────────────────────────────────────────────────────────

def binary_metrics(pairs: list[tuple[bool, bool]]) -> dict:
    """pairs = [(observé, prévu)] → POD, FAR, exactitude, PSS."""
    hits = sum(1 for o, f in pairs if o and f)
    miss = sum(1 for o, f in pairs if o and not f)
    fa   = sum(1 for o, f in pairs if not o and f)
    cn   = sum(1 for o, f in pairs if not o and not f)
    n = len(pairs)
    pod  = hits / (hits + miss) if (hits + miss) else None
    far  = fa / (hits + fa) if (hits + fa) else None
    pofd = fa / (fa + cn) if (fa + cn) else None
    acc  = (hits + cn) / n if n else None
    pss  = (pod - pofd) if (pod is not None and pofd is not None) else None
    return {"n": n, "hits": hits, "miss": miss, "false_alarms": fa, "correct_neg": cn,
            "pod": pod, "far": far, "accuracy": acc, "pss": pss}


TOUS_REGIMES = object()   # sentinelle : ne pas filtrer par régime


def regime_courant(conn: sqlite3.Connection):
    """Masque côtier (km) de la prévision archivée la plus récente.

    Renvoie TOUS_REGIMES si l'archive ne connaît pas encore la colonne
    (anciennes bases, tests) : on ne filtre alors rien, comme avant."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(forecast_archive)")}
    if "seed_coast_km" not in cols:
        return TOUS_REGIMES
    # Les lignes au régime inconnu (NULL : grille de repli, anciennes simulations)
    # ne décident pas du régime courant.
    row = conn.execute(
        """SELECT seed_coast_km FROM forecast_archive
           WHERE seed_coast_km IS NOT NULL ORDER BY computed_at DESC LIMIT 1"""
    ).fetchone()
    return row[0] if row else TOUS_REGIMES


def nom_regime(regime) -> str:
    if regime is TOUS_REGIMES:
        return "tous régimes mélangés"
    if regime is None:
        return "régime inconnu"
    return "sans masque côtier" if regime == 0 else f"masque côtier {regime:g} km"


def last_forecast(conn: sqlite3.Connection, name: str, date: str, lead: int,
                  regime=TOUS_REGIMES, avant: str | None = None):
    """Dernière prévision archivée pour (plage, date visée, échéance), ou None.

    regime : masque côtier (km) exigé ; TOUS_REGIMES = pas de filtre.
    Sans ce filtre, la dernière prévision d'avant le masque resterait retenue
    pour toute une diagonale de dates (j+1 du 11/11, j+2 du 12/11…).
    avant : heure de l'observation ; seules les prévisions calculées avant comptent."""
    sql = """SELECT computed_at, presence, risk_level, local_score,
                    regional_score, n_stranded, presence_raw
             FROM forecast_archive
             WHERE beach_name = ? AND target_date = ? AND day_offset = ?"""
    params: list = [name, date, lead]
    if regime is not TOUS_REGIMES:
        sql += " AND seed_coast_km IS ?"
        params.append(regime)
    if avant:
        sql += " AND computed_at <= ?"
        params.append(avant)
    sql += " ORDER BY computed_at DESC LIMIT 1"
    return conn.execute(sql, params).fetchone()


def run_webcam(conn: sqlite3.Connection, computed_at: str,
               dry_run: bool = False, verbose: bool = False, regime=None) -> int:
    """Second juge : confronte l'archive aux verdicts sûrs de la webcam.

    Même logique et mêmes témoins que le juge terrain, mais résultat séparé.
    Retourne le nombre de couples verdict × échéance."""
    has_truth = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='webcam_truth'"
    ).fetchone()
    if not has_truth:
        return 0
    if regime is None:
        regime = regime_courant(conn)
    # Heure réelle de la capture (webcam_captures), 14h UTC à défaut
    has_captures = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='webcam_captures'"
    ).fetchone()
    heure = ("(SELECT MIN(c.captured_at) FROM webcam_captures c WHERE c.file_path = w.file_path)"
             if has_captures else "NULL")
    rows = conn.execute(
        f"""SELECT w.capture_date, w.island, w.beach_name, w.verdict,
                   COALESCE({heure}, w.capture_date || 'T14:00:00')
            FROM webcam_truth w
            WHERE w.verdict IN (%s) ORDER BY w.capture_date""" % ",".join("?" * len(WEBCAM_TRUTH)),
        tuple(WEBCAM_TRUTH),
    ).fetchall()
    history: dict[tuple[str, str], list[tuple[str, bool]]] = defaultdict(list)
    for date, island, beach, v, _ in rows:
        history[(island, beach)].append((date, WEBCAM_TRUTH[v]))

    matches = []
    for date, island, beach, v, vu_a in rows:
        d0 = datetime.fromisoformat(date)
        # persistance : dernier verdict sûr de la même plage, strictement avant, ≤ 7 j
        prev = [(d, o) for d, o in history[(island, beach)]
                if d < date and (d0 - datetime.fromisoformat(d)).days <= PERSISTENCE_MAX_DAYS]
        persistence = max(prev)[1] if prev else None
        for lead in LEADS:
            fc = last_forecast(conn, beach, date, lead, regime, avant=vu_a)
            if not fc:
                continue
            matches.append({"day_offset": lead, "obs": WEBCAM_TRUTH[v],
                            "presence": fc[1] or "none", "risk_level": fc[2] or "none",
                            "presence_raw": fc[6] or fc[1] or "none",
                            "persistence": persistence})
            if verbose:
                print(f"  [webcam] {date} {beach:<18} j+{lead} vu={v:<8} "
                      f"prévu présence={fc[1]:<6} régional={fc[2]:<6}")

    summary = {}
    for lead in LEADS:
        ms = [m for m in matches if m["day_offset"] == lead]
        if not ms:
            continue
        obs_bin = [m["obs"] for m in ms]
        majority = Counter(obs_bin).most_common(1)[0][0]
        pers_pairs = [(m["obs"], m["persistence"]) for m in ms if m["persistence"] is not None]
        summary[lead] = {
            "local": binary_metrics([(m["obs"], RANK[m["presence"]] > 0) for m in ms]),
            "local_raw": binary_metrics([(m["obs"], RANK[m["presence_raw"]] > 0) for m in ms]),
            "regional": binary_metrics([(m["obs"], RANK[m["risk_level"]] > 0) for m in ms]),
            "climatology": binary_metrics([(o, majority) for o in obs_bin]),
            "persistence": binary_metrics(pers_pairs) if pers_pairs else None,
            "base_rate": sum(obs_bin) / len(obs_bin),
            "regime": nom_regime(regime),
        }

    print_webcam_report(summary, len(rows), len(matches), nom_regime(regime))
    if not dry_run and summary:
        conn.executemany(
            "INSERT INTO verification_webcam_summary (computed_at, day_offset, n, metrics_json) VALUES (?,?,?,?)",
            [(computed_at, lead, s["local"]["n"], json.dumps(s)) for lead, s in summary.items()],
        )
        conn.commit()
    return len(matches)


def run(dry_run: bool = False, verbose: bool = False, regime=None) -> dict:
    """regime : masque côtier (km) à juger ; None = régime actuel ; TOUS_REGIMES = tout."""
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    # L'archive est créée par beaches.py au premier scoring post-2026-11-09.
    has_archive = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='forecast_archive'"
    ).fetchone()
    if not has_archive:
        print("[VERIF] Pas encore de table forecast_archive : lancez d'abord beaches.py "
              "(le pipeline cron le fait toutes les 6 h). Rien à vérifier.")
        conn.close()
        return {"matches": 0, "leads": 0, "unmatched": 0}
    catalog = load_catalog(conn)
    if regime is None:
        regime = regime_courant(conn)
    print(f"[VERIF] Prévisions jugées : {nom_regime(regime)}")

    obs_rows = conn.execute(
        """SELECT id, observed_at, island, beach_name, observed_risk
           FROM beach_observations
           WHERE island IN (%s) AND observed_risk IN ('none','low','medium','high')
           ORDER BY observed_at""" % ",".join("?" * len(ISLANDS)),
        ISLANDS,
    ).fetchall()

    # Historique par plage pour la persistance : {(île, plage): [(date, risk)]}
    history: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    matched_obs = []
    n_unmatched = 0
    for obs_id, observed_at, island, obs_beach, risk in obs_rows:
        name = match_beach(island, obs_beach, catalog)
        if not name:
            n_unmatched += 1
            if verbose:
                print(f"  [skip] hors catalogue : {island} / {obs_beach}")
            continue
        date = observed_at[:10]
        history[(island, name)].append((date, risk))
        matched_obs.append((obs_id, date, island, name, obs_beach, risk, observed_at))

    computed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    matches = []
    for obs_id, date, island, name, obs_beach, risk, observed_at in matched_obs:
        # persistance : dernière observation de la MÊME plage, strictement avant
        d0 = datetime.fromisoformat(date)
        prev = [(d, r) for d, r in history[(island, name)]
                if d < date and (d0 - datetime.fromisoformat(d)).days <= PERSISTENCE_MAX_DAYS]
        persistence = max(prev)[1] if prev else None

        for lead in LEADS:
            fc = last_forecast(conn, name, date, lead, regime, avant=observed_at)
            if not fc:
                continue
            matches.append({
                "obs_id": obs_id, "island": island, "beach_name": name,
                "obs_beach": obs_beach, "target_date": date, "observed_risk": risk,
                "day_offset": lead, "forecast_at": fc[0], "presence": fc[1] or "none",
                "risk_level": fc[2] or "none", "local_score": fc[3],
                "regional_score": fc[4], "n_stranded": fc[5], "persistence": persistence,
                # présence SANS fenêtre d'exposition (pour juger si la fenêtre aide)
                "presence_raw": fc[6] or fc[1] or "none",
            })
            if verbose:
                print(f"  {date} {island:<13} {name:<18} j+{lead} obs={risk:<6} "
                      f"prévu présence={fc[1]:<6} régional={fc[2]:<6} ⚓{fc[5] or 0}")

    # ── Agrégats par échéance ────────────────────────────────────────────────
    summary = {}
    for lead in LEADS:
        rows = [m for m in matches if m["day_offset"] == lead]
        if not rows:
            continue
        obs_bin = [RANK[m["observed_risk"]] > 0 for m in rows]
        local   = binary_metrics([(o, RANK[m["presence"]] > 0) for o, m in zip(obs_bin, rows)])
        raw     = binary_metrics([(o, RANK[m["presence_raw"]] > 0) for o, m in zip(obs_bin, rows)])
        region  = binary_metrics([(o, RANK[m["risk_level"]] > 0) for o, m in zip(obs_bin, rows)])
        mae_local = sum(abs(RANK[m["presence"]] - RANK[m["observed_risk"]]) for m in rows) / len(rows)

        # Témoin 1 : climatologie = classe majoritaire observée
        majority = Counter(obs_bin).most_common(1)[0][0]
        clim = binary_metrics([(o, majority) for o in obs_bin])
        # Témoin 2 : persistance (sous-ensemble où elle existe)
        pers_pairs = [(o, RANK[m["persistence"]] > 0) for o, m in zip(obs_bin, rows)
                      if m["persistence"] is not None]
        pers = binary_metrics(pers_pairs) if pers_pairs else None
        summary[lead] = {"local": local, "local_raw": raw, "regional": region,
                         "mae_rank_local": mae_local,
                         "climatology": clim, "persistence": pers,
                         "base_rate": sum(obs_bin) / len(obs_bin),
                         "regime": nom_regime(regime)}

    print_report(summary, computed_at, len(obs_rows), len(matched_obs), n_unmatched, len(matches))

    if not dry_run:
        conn.execute("DELETE FROM verification_matches")
        conn.executemany(
            """INSERT INTO verification_matches
               (computed_at, obs_id, island, beach_name, obs_beach, target_date, observed_risk,
                day_offset, forecast_at, presence, risk_level, local_score, regional_score,
                n_stranded, persistence)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [(computed_at, m["obs_id"], m["island"], m["beach_name"], m["obs_beach"],
              m["target_date"], m["observed_risk"], m["day_offset"], m["forecast_at"],
              m["presence"], m["risk_level"], m["local_score"], m["regional_score"],
              m["n_stranded"], m["persistence"]) for m in matches],
        )
        conn.executemany(
            "INSERT INTO verification_summary (computed_at, day_offset, n, metrics_json) VALUES (?,?,?,?)",
            [(computed_at, lead, s["local"]["n"], json.dumps(s)) for lead, s in summary.items()],
        )
        conn.commit()
        print(f"[VERIF] {len(matches)} matches, {len(summary)} échéances stockées.")
    n_webcam = run_webcam(conn, computed_at, dry_run=dry_run, verbose=verbose, regime=regime)
    conn.close()
    return {"matches": len(matches), "leads": len(summary), "unmatched": n_unmatched,
            "webcam_matches": n_webcam}


# ── Rapport ──────────────────────────────────────────────────────────────────

def _pct(x) -> str:
    return "  —  " if x is None else f"{100 * x:5.0f}%"


def print_report(summary: dict, computed_at: str, n_obs: int, n_matched: int,
                 n_unmatched: int, n_matches: int) -> None:
    print("\n" + "=" * 72)
    print("  VÉRIFICATION prévisions plage × jour — Sargasses")
    print("=" * 72)
    print(f"  Calculé                      : {computed_at}")
    regime = next(iter(summary.values())).get("regime") if summary else None
    if regime:
        print(f"  Prévisions jugées            : {regime}")
    print(f"  Observations (5 îles)        : {n_obs}  dont rattachées au catalogue : {n_matched}"
          f"  (hors catalogue : {n_unmatched})")
    print(f"  Couples obs × échéance       : {n_matches}")
    if not summary:
        print("\n  Aucune prévision archivée ne couvre une date observée.")
        print("  Normal au démarrage : l'archive (forecast_archive) commence le 2026-11-09,")
        print("  les matches apparaîtront dès les prochains signalements terrain.")
        print("=" * 72)
        return
    _print_table(summary)
    print()
    print("  Lecture : PSS > 0 = mieux que le hasard ; le modèle doit aussi battre")
    print("  l'exactitude des témoins (climatologie, persistance), sinon il n'apporte rien.")
    print("  « sans fenêtre » = même score local sans la fenêtre d'exposition : si cette")
    print("  colonne fait aussi bien, la géométrie n'apporte rien et on le saura.")
    print("=" * 72)


def _print_table(summary: dict) -> None:
    """Tableau des scores par échéance (commun aux juges terrain et webcam)."""
    print()
    print(f"  {'Éch.':<5}{'n':>4}  {'taux obs':>8}  │ {'PRÉSENCE (local, fenêtre)':^27} │ {'sans fenêtre':^13} │ {'APPROCHE (rég.)':^14} │ {'TÉMOINS exactitude':^18}")
    print(f"  {'':<5}{'':>4}  {'':>8}  │ {'POD':>5} {'FAR':>6} {'Exact':>6} {'PSS':>6} │ {'Exact':>6} {'PSS':>6} │ {'POD':>6} {'PSS':>6} │ {'climato':>8} {'persist':>8}")
    print("  " + "─" * 112)
    for lead, s in sorted(summary.items()):
        lo, re, cl, pe = s["local"], s["regional"], s["climatology"], s["persistence"]
        rw = s.get("local_raw") or lo
        pss_l = "  —  " if lo["pss"] is None else f"{lo['pss']:+5.2f}"
        pss_w = "  —  " if rw["pss"] is None else f"{rw['pss']:+5.2f}"
        pss_r = "  —  " if re["pss"] is None else f"{re['pss']:+5.2f}"
        pers_acc = _pct(pe["accuracy"]) if pe else "  —  "
        print(f"  j+{lead:<3}{lo['n']:>4}  {_pct(s['base_rate']):>8}  │ {_pct(lo['pod']):>5} {_pct(lo['far']):>6} "
              f"{_pct(lo['accuracy']):>6} {pss_l:>6} │ {_pct(rw['accuracy']):>6} {pss_w:>6} │ "
              f"{_pct(re['pod']):>6} {pss_r:>6} │ {_pct(cl['accuracy']):>8} {pers_acc:>8}")


def print_webcam_report(summary: dict, n_verdicts: int, n_matches: int,
                        regime: str | None = None) -> None:
    print("\n" + "=" * 72)
    print("  SECOND JUGE : webcam de Flamands (lecture automatique, à part)")
    print("=" * 72)
    if regime is None and summary:
        regime = next(iter(summary.values())).get("regime")
    if regime:
        print(f"  Prévisions jugées                            : {regime}")
    print(f"  Verdicts sûrs en base ('radeaux' ou 'rien') : {n_verdicts}")
    print(f"  Couples verdict × échéance                   : {n_matches}")
    if not summary:
        print("\n  Aucune prévision archivée ne couvre un jour lu par la webcam.")
        print("  Normal au démarrage : l'archive commence le 2026-11-09.")
        print("=" * 72)
        return
    _print_table(summary)
    print()
    print("  Attention : la webcam ne voit que les radeaux NETS dans l'eau. Un « un peu »")
    print("  ou un dépôt sur le sable compte ici pour 'rien'. Test à l'aveugle du lecteur :")
    print("  'radeaux' juste 9 fois sur 10, 'rien' juste 24 fois sur 25 (42 jours).")
    print("=" * 72)


def print_last(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT MAX(computed_at) FROM verification_summary").fetchone()
    if not row or not row[0]:
        print("Aucun bilan stocké. Lancez : python sarga_verification.py")
        return
    rows = conn.execute(
        "SELECT day_offset, metrics_json FROM verification_summary WHERE computed_at = ?",
        (row[0],)).fetchall()
    summary = {d: json.loads(j) for d, j in rows}
    n = conn.execute("SELECT COUNT(*) FROM verification_matches").fetchone()[0]
    print_report(summary, row[0], -1, -1, -1, n)


def print_last_webcam(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT MAX(computed_at) FROM verification_webcam_summary").fetchone()
    if not row or not row[0]:
        return
    rows = conn.execute(
        "SELECT day_offset, n, metrics_json FROM verification_webcam_summary WHERE computed_at = ?",
        (row[0],)).fetchall()
    print_webcam_report({d: json.loads(j) for d, _, j in rows}, -1, sum(n for _, n, _ in rows))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="n'écrit rien en base")
    ap.add_argument("--verbose", action="store_true", help="détaille chaque match")
    ap.add_argument("--report", action="store_true", help="réaffiche le dernier bilan stocké")
    ap.add_argument("--masque", type=float, default=None,
                    help="régime à juger : masque côtier en km (0 = avant le masque). "
                         "Par défaut : le régime de la prévision la plus récente")
    ap.add_argument("--tous-regimes", action="store_true",
                    help="mélange tous les régimes (comparaison seulement)")
    args = ap.parse_args()
    if args.report:
        conn = sqlite3.connect(DB_PATH)
        conn.executescript(SCHEMA)
        print_last(conn)
        print_last_webcam(conn)
        conn.close()
        return
    regime = TOUS_REGIMES if args.tous_regimes else args.masque
    run(dry_run=args.dry_run, verbose=args.verbose, regime=regime)


if __name__ == "__main__":
    sys.exit(main())
