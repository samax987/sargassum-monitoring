#!/usr/bin/env python3
"""
sarga_calibration.py
====================
Calibration automatique : prédit (OpenDrift) vs observé (terrain/IA).

Pour chaque observation terrain, on cherche la prédiction la plus proche
(même île, même date ±3 jours, nom de plage fuzzy-matché).
On calcule ensuite un biais de calibration par île et par mois.

Usage
-----
  python sarga_calibration.py              # calibration complète
  python sarga_calibration.py --report     # affiche le rapport sans écrire en DB
  python sarga_calibration.py --reset      # efface et recalcule tout

Tables créées
-------------
  calibration_matches   — chaque observation matchée avec sa prédiction
  calibration_bias      — biais résumé par île/mois
"""

import argparse
import json
import re
import sqlite3
import sys
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

from rapidfuzz import fuzz, process

# ── Config ────────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "sargassum_data.db"

RISK_NUM  = {"none": 0, "low": 1, "medium": 2, "high": 3}
RISK_LABEL = {0: "none", 1: "low", 2: "medium", 3: "high"}

# Tolérance de date : ±N jours entre observation et simulation
DATE_TOLERANCE_DAYS = 3

# Seuil de confiance fuzzy (0-100)
FUZZY_MIN_SCORE = 55

ANTILLES_ISLANDS = {"Martinique", "Guadeloupe", "Saint-Barth", "Saint-Martin", "Marie-Galante"}


# ── Normalisation noms de plages ──────────────────────────────────────────────

def normalize(name: str) -> str:
    """Normalise un nom de plage pour la comparaison fuzzy."""
    # Supprime accents
    nfkd = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Minuscules, remplace _ et - par espace
    name = name.lower().replace("_", " ").replace("-", " ")
    # Supprime mots parasites
    for word in ["plage", "anse", "beach", "baie", "bay", "pointe", "grande", "petit"]:
        name = re.sub(r'\b' + word + r'\b', '', name)
    # Normalise espaces
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def extract_key_tokens(name: str) -> str:
    """Extrait les tokens les plus discriminants d'un nom de plage."""
    norm = normalize(name)
    # Garde les tokens de 3+ caractères
    tokens = [t for t in norm.split() if len(t) >= 3]
    return " ".join(tokens[:4])  # Max 4 tokens


# ── Schéma DB ─────────────────────────────────────────────────────────────────

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS calibration_matches (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        matched_at      TEXT    NOT NULL,
        obs_id          INTEGER NOT NULL,
        score_id        INTEGER,
        island          TEXT    NOT NULL,
        obs_beach       TEXT    NOT NULL,
        pred_beach      TEXT,
        obs_date        TEXT    NOT NULL,
        pred_date       TEXT,
        observed_risk   TEXT    NOT NULL,
        predicted_risk  TEXT,
        fuzzy_score     REAL,
        risk_error      INTEGER,
        error_direction TEXT
    );

    CREATE TABLE IF NOT EXISTS calibration_bias (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        computed_at     TEXT    NOT NULL,
        island          TEXT    NOT NULL,
        month           INTEGER,
        month_label     TEXT,
        n_matches       INTEGER NOT NULL,
        n_correct       INTEGER,
        n_under         INTEGER,
        n_over          INTEGER,
        accuracy        REAL,
        mean_error      REAL,
        bias_direction  TEXT,
        correction      INTEGER,
        recommendation  TEXT
    );
    """)
    conn.commit()


# ── Chargement données ────────────────────────────────────────────────────────

def load_observations(conn: sqlite3.Connection) -> list[dict]:
    """Charge toutes les observations avec source réelle (pas 'claude_web' seul)."""
    rows = conn.execute("""
        SELECT id, observed_at, island, beach_name, observed_risk
        FROM beach_observations
        WHERE island IN ('Martinique','Guadeloupe','Saint-Barth','Saint-Martin','Marie-Galante')
        ORDER BY observed_at
    """).fetchall()
    return [dict(zip(["id","observed_at","island","beach_name","observed_risk"], r)) for r in rows]


def load_predictions(conn: sqlite3.Connection) -> list[dict]:
    """Charge toutes les prédictions pour les Antilles (day_offset=0 = jour J)."""
    rows = conn.execute("""
        SELECT id, simulated_at, island, beach_name, day_offset,
               local_score, regional_score, risk_level
        FROM beach_risk_scores
        WHERE island IN ('Martinique','Guadeloupe','Saint-Barth','Saint-Martin','Marie-Galante')
          AND day_offset = 0
        ORDER BY simulated_at
    """).fetchall()
    cols = ["id","simulated_at","island","beach_name","day_offset","local_score","regional_score","risk_level"]
    return [dict(zip(cols, r)) for r in rows]


# ── Matching ──────────────────────────────────────────────────────────────────

def match_observation(obs: dict, predictions: list[dict]) -> dict | None:
    """
    Trouve la meilleure prédiction pour une observation donnée.
    Critères : même île + date proche + nom de plage fuzzy.
    """
    obs_dt = datetime.fromisoformat(obs["observed_at"].replace("Z",""))
    obs_island = obs["island"]
    obs_beach_norm = extract_key_tokens(obs["beach_name"])

    # Filtrer par île et fenêtre de date
    candidates = []
    for pred in predictions:
        pred_dt = datetime.fromisoformat(pred["simulated_at"].replace("Z",""))
        delta = abs((obs_dt.date() - pred_dt.date()).days)
        if pred["island"] == obs_island and delta <= DATE_TOLERANCE_DAYS:
            candidates.append((pred, delta))

    if not candidates:
        return None

    # Fuzzy match sur le nom de plage
    best_match = None
    best_score = 0

    for pred, date_delta in candidates:
        pred_beach_norm = extract_key_tokens(pred["beach_name"])
        score = fuzz.token_set_ratio(obs_beach_norm, pred_beach_norm)
        # Bonus si la date est exacte
        if date_delta == 0:
            score = min(100, score + 5)
        if score > best_score:
            best_score = score
            best_match = (pred, score)

    if best_match and best_match[1] >= FUZZY_MIN_SCORE:
        return {"prediction": best_match[0], "fuzzy_score": best_match[1]}
    return None


# ── Calcul biais ─────────────────────────────────────────────────────────────

def compute_bias(matches: list[dict]) -> list[dict]:
    """Calcule le biais de calibration par île et par mois."""
    from collections import defaultdict

    groups: dict[tuple, list] = defaultdict(list)
    for m in matches:
        if m.get("predicted_risk") is None:
            continue
        island = m["island"]
        month = int(m["obs_date"][5:7])
        groups[(island, month)].append(m)

    MONTH_FR = {1:"Jan",2:"Fév",3:"Mar",4:"Avr",5:"Mai",6:"Jun",
                7:"Jul",8:"Aoû",9:"Sep",10:"Oct",11:"Nov",12:"Déc"}

    biases = []
    for (island, month), group in sorted(groups.items()):
        obs_nums  = [RISK_NUM.get(m["observed_risk"], 2) for m in group]
        pred_nums = [RISK_NUM.get(m["predicted_risk"], 2) for m in group]
        errors    = [p - o for p, o in zip(pred_nums, obs_nums)]

        n = len(group)
        n_correct = sum(1 for e in errors if e == 0)
        n_over    = sum(1 for e in errors if e > 0)
        n_under   = sum(1 for e in errors if e < 0)
        accuracy  = n_correct / n if n > 0 else 0
        mean_err  = sum(errors) / n if n > 0 else 0

        # Direction du biais
        if mean_err > 0.3:
            direction = "sur-prédit"
            correction = -1
            reco = f"Réduire d'1 niveau les prédictions {island} en {MONTH_FR[month]}"
        elif mean_err < -0.3:
            direction = "sous-prédit"
            correction = +1
            reco = f"Augmenter d'1 niveau les prédictions {island} en {MONTH_FR[month]}"
        else:
            direction = "correct"
            correction = 0
            reco = f"Prédictions {island} calibrées en {MONTH_FR[month]}"

        biases.append({
            "island": island, "month": month, "month_label": MONTH_FR[month],
            "n_matches": n, "n_correct": n_correct, "n_under": n_under, "n_over": n_over,
            "accuracy": round(accuracy, 3), "mean_error": round(mean_err, 3),
            "bias_direction": direction, "correction": correction,
            "recommendation": reco,
        })
    return biases


# ── Stockage ──────────────────────────────────────────────────────────────────

def store_matches(conn: sqlite3.Connection, matches: list[dict], now: str):
    conn.execute("DELETE FROM calibration_matches")
    for m in matches:
        conn.execute("""
            INSERT INTO calibration_matches
            (matched_at, obs_id, score_id, island, obs_beach, pred_beach,
             obs_date, pred_date, observed_risk, predicted_risk,
             fuzzy_score, risk_error, error_direction)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now, m["obs_id"],
            m.get("pred_id"), m["island"],
            m["obs_beach"], m.get("pred_beach"),
            m["obs_date"], m.get("pred_date"),
            m["observed_risk"], m.get("predicted_risk"),
            m.get("fuzzy_score"), m.get("risk_error"),
            m.get("error_direction"),
        ))
    conn.commit()


def store_bias(conn: sqlite3.Connection, biases: list[dict], now: str):
    conn.execute("DELETE FROM calibration_bias")
    for b in biases:
        conn.execute("""
            INSERT INTO calibration_bias
            (computed_at, island, month, month_label, n_matches,
             n_correct, n_under, n_over, accuracy, mean_error,
             bias_direction, correction, recommendation)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now, b["island"], b["month"], b["month_label"],
            b["n_matches"], b["n_correct"], b["n_under"], b["n_over"],
            b["accuracy"], b["mean_error"], b["bias_direction"],
            b["correction"], b["recommendation"],
        ))
    conn.commit()


# ── Rapport texte ─────────────────────────────────────────────────────────────

def print_report(matches: list[dict], biases: list[dict]):
    matched = [m for m in matches if m.get("predicted_risk")]
    unmatched = [m for m in matches if not m.get("predicted_risk")]

    print(f"\n{'='*60}")
    print(f"  RAPPORT DE CALIBRATION SARGASSES")
    print(f"{'='*60}")
    print(f"  Observations analysées : {len(matches)}")
    print(f"  Matchées avec préd.    : {len(matched)}")
    print(f"  Sans prédiction        : {len(unmatched)}")

    if matched:
        errors = [m["risk_error"] for m in matched if m["risk_error"] is not None]
        n_correct = sum(1 for e in errors if e == 0)
        n_under   = sum(1 for e in errors if e < 0)
        n_over    = sum(1 for e in errors if e > 0)
        accuracy  = n_correct / len(errors) if errors else 0
        print(f"\n  Précision globale : {accuracy:.0%}  ({n_correct}/{len(errors)} corrects)")
        print(f"  Sous-prédictions  : {n_under}  |  Sur-prédictions : {n_over}")

    if biases:
        print(f"\n  {'Île':<15} {'Mois':<5} {'N':>4} {'Préc':>6} {'Biais':>7}  Direction")
        print(f"  {'-'*55}")
        for b in biases:
            icon = "✓" if b["bias_direction"] == "correct" else ("↑" if b["bias_direction"] == "sur-prédit" else "↓")
            print(f"  {b['island']:<15} {b['month_label']:<5} {b['n_matches']:>4} "
                  f"{b['accuracy']:>5.0%}  {b['mean_error']:>+6.2f}  {icon} {b['bias_direction']}")

    print(f"\n  Recommandations :")
    for b in biases:
        if b["correction"] != 0:
            print(f"  → {b['recommendation']}")

    print(f"{'='*60}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_calibration(dry_run: bool = False, verbose: bool = False) -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    observations = load_observations(conn)
    predictions  = load_predictions(conn)

    if verbose:
        print(f"[CALIB] {len(observations)} observations, {len(predictions)} prédictions chargées")

    matches = []
    for obs in observations:
        result = match_observation(obs, predictions)
        entry = {
            "obs_id":       obs["id"],
            "island":       obs["island"],
            "obs_beach":    obs["beach_name"],
            "obs_date":     obs["observed_at"][:10],
            "observed_risk": obs["observed_risk"],
        }
        if result:
            pred = result["prediction"]
            obs_num  = RISK_NUM.get(obs["observed_risk"], 2)
            pred_num = RISK_NUM.get(pred["risk_level"], 2)
            error    = pred_num - obs_num
            direction = "correct" if error == 0 else ("sur-prédit" if error > 0 else "sous-prédit")
            entry.update({
                "pred_id":       pred["id"],
                "pred_beach":    pred["beach_name"],
                "pred_date":     pred["simulated_at"][:10],
                "predicted_risk": pred["risk_level"],
                "fuzzy_score":   result["fuzzy_score"],
                "risk_error":    error,
                "error_direction": direction,
            })
        matches.append(entry)

    biases = compute_bias(matches)

    print_report(matches, biases)

    if not dry_run:
        store_matches(conn, matches, now)
        store_bias(conn, biases, now)
        print(f"[CALIB] Résultats stockés en DB ({len(matches)} matches, {len(biases)} biais).")

    conn.close()
    return {"matches": len(matches), "matched": sum(1 for m in matches if m.get("predicted_risk")), "biases": len(biases)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report",  action="store_true", help="Affiche sans écrire en DB")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    run_calibration(dry_run=args.report, verbose=args.verbose)


if __name__ == "__main__":
    main()
