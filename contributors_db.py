#!/usr/bin/env python3
"""
contributors_db.py
==================
Couche d'accès base pour le portail contributeurs (bénévoles SBH).

Deux tables :
  - contributors             : comptes bénévoles, validés manuellement par Sam
  - contributor_observations : signalements en attente de modération (staging)

Principe de sécurité : un signalement n'entre dans `beach_observations`
(la table que lit la calibration `sarga_calibration_spatial.py`) qu'APRÈS
approbation explicite. Tant qu'il est dans le staging, il n'influence jamais
le modèle de prédiction. La promotion se fait dans `approve_observation()`.

Conventions du projet :
  - tables créées avec CREATE TABLE IF NOT EXISTS (jamais destructif)
  - connexions SQLite toujours fermées dans un finally
  - requêtes paramétrées uniquement
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "sargassum_data.db"

# ── Statuts ──────────────────────────────────────────────────────────────────
# Comptes
ACCOUNT_PENDING = "pending"     # inscrit, en attente de validation par Sam
ACCOUNT_ACTIVE = "active"       # validé : peut se connecter et signaler
ACCOUNT_REJECTED = "rejected"   # refusé
ACCOUNT_BANNED = "banned"       # banni après coup
VALID_ACCOUNT_STATUS = {ACCOUNT_PENDING, ACCOUNT_ACTIVE, ACCOUNT_REJECTED, ACCOUNT_BANNED}

# Signalements
OBS_PENDING = "pending"         # soumis, en attente de modération
OBS_APPROVED = "approved"       # validé et promu dans beach_observations
OBS_REJECTED = "rejected"       # rejeté


def _utcnow() -> str:
    """Horodatage ISO sans microsecondes, cohérent avec le reste de la base."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def get_connection(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ── Initialisation du schéma ─────────────────────────────────────────────────

def init_db(db_path: Path | str = DB_PATH) -> None:
    """Crée les tables si absentes. Idempotent — appelé au démarrage de Flask."""
    conn = get_connection(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contributors (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                username      TEXT    NOT NULL UNIQUE,
                display_name  TEXT    NOT NULL,
                password_hash TEXT    NOT NULL,
                status        TEXT    NOT NULL DEFAULT 'pending',
                is_trusted    INTEGER NOT NULL DEFAULT 0,
                created_at    TEXT    NOT NULL,
                approved_at   TEXT,
                last_login_at TEXT,
                obs_count     INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contributor_observations (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                contributor_id  INTEGER NOT NULL,
                observed_at     TEXT    NOT NULL,
                island          TEXT    NOT NULL,
                beach_name      TEXT    NOT NULL,
                observed_risk   TEXT    NOT NULL,
                coverage_pct    INTEGER,
                notes           TEXT,
                submitted_at    TEXT    NOT NULL,
                client_ip       TEXT,
                status          TEXT    NOT NULL DEFAULT 'pending',
                reviewed_at     TEXT,
                promoted_obs_id INTEGER,
                FOREIGN KEY (contributor_id) REFERENCES contributors(id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_contrib_obs_status "
            "ON contributor_observations(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_contrib_obs_contrib "
            "ON contributor_observations(contributor_id)"
        )
        # Migration additive : photo jointe (chemin relatif au projet, ex.
        # "contrib_photos/ab12cd34.jpg"). try/except car SQLite n'a pas
        # ADD COLUMN IF NOT EXISTS — convention du projet.
        try:
            conn.execute(
                "ALTER TABLE contributor_observations ADD COLUMN photo_path TEXT"
            )
        except sqlite3.OperationalError:
            pass  # colonne déjà présente
        # Multi-photos (juin 2026) : liste JSON de chemins. photo_path (single)
        # reste pour rétro-compat ; photos_json est désormais la source.
        try:
            conn.execute(
                "ALTER TABLE contributor_observations ADD COLUMN photos_json TEXT"
            )
        except sqlite3.OperationalError:
            pass
        # Backfill : les anciens signalements à photo unique → liste JSON.
        conn.execute(
            "UPDATE contributor_observations SET photos_json = '[\"' || photo_path || '\"]' "
            "WHERE photos_json IS NULL AND photo_path IS NOT NULL"
        )
        conn.commit()
        logger.info("contributors_db : tables prêtes")
    finally:
        conn.close()


# ── Comptes ──────────────────────────────────────────────────────────────────

def create_contributor(
    username: str,
    display_name: str,
    password_hash: str,
    db_path: Path | str = DB_PATH,
) -> int | None:
    """Crée un compte en statut 'pending'.

    Retourne l'id du nouveau compte, ou None si le username est déjà pris.
    """
    conn = get_connection(db_path)
    try:
        cur = conn.execute(
            """INSERT INTO contributors
               (username, display_name, password_hash, status, created_at)
               VALUES (?, ?, ?, 'pending', ?)""",
            (username, display_name, password_hash, _utcnow()),
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        # username déjà existant (contrainte UNIQUE)
        return None
    finally:
        conn.close()


def get_by_username(username: str, db_path: Path | str = DB_PATH) -> dict | None:
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM contributors WHERE username = ?", (username,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_by_id(contributor_id: int, db_path: Path | str = DB_PATH) -> dict | None:
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM contributors WHERE id = ?", (contributor_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def record_login(contributor_id: int, db_path: Path | str = DB_PATH) -> None:
    conn = get_connection(db_path)
    try:
        conn.execute(
            "UPDATE contributors SET last_login_at = ? WHERE id = ?",
            (_utcnow(), contributor_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_account_status(
    contributor_id: int, status: str, db_path: Path | str = DB_PATH
) -> bool:
    """Change le statut d'un compte. Renseigne approved_at au 1er passage en 'active'."""
    if status not in VALID_ACCOUNT_STATUS:
        raise ValueError(f"statut compte invalide : {status}")
    conn = get_connection(db_path)
    try:
        if status == ACCOUNT_ACTIVE:
            cur = conn.execute(
                "UPDATE contributors SET status = ?, "
                "approved_at = COALESCE(approved_at, ?) WHERE id = ?",
                (status, _utcnow(), contributor_id),
            )
        else:
            cur = conn.execute(
                "UPDATE contributors SET status = ? WHERE id = ?",
                (status, contributor_id),
            )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_trusted(
    contributor_id: int, trusted: bool, db_path: Path | str = DB_PATH
) -> None:
    conn = get_connection(db_path)
    try:
        conn.execute(
            "UPDATE contributors SET is_trusted = ? WHERE id = ?",
            (1 if trusted else 0, contributor_id),
        )
        conn.commit()
    finally:
        conn.close()


def list_pending_accounts(db_path: Path | str = DB_PATH) -> list[dict]:
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM contributors WHERE status = 'pending' "
            "ORDER BY created_at ASC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_accounts(db_path: Path | str = DB_PATH) -> list[dict]:
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM contributors ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Signalements (staging) ───────────────────────────────────────────────────

def add_observation(
    contributor_id: int,
    observed_at: str,
    island: str,
    beach_name: str,
    observed_risk: str,
    coverage_pct: int | None = None,
    notes: str | None = None,
    client_ip: str | None = None,
    photos: list[str] | None = None,
    db_path: Path | str = DB_PATH,
) -> int:
    """Enregistre un signalement en attente de modération. Retourne son id.

    `photos` : liste de chemins relatifs (0 à 3). Stockée en JSON ; le premier
    est aussi copié dans photo_path (rétro-compat avec l'ancien schéma).
    """
    photos = [p for p in (photos or []) if p]
    conn = get_connection(db_path)
    try:
        cur = conn.execute(
            """INSERT INTO contributor_observations
               (contributor_id, observed_at, island, beach_name, observed_risk,
                coverage_pct, notes, submitted_at, client_ip,
                photo_path, photos_json, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')""",
            (contributor_id, observed_at, island, beach_name, observed_risk,
             coverage_pct, notes, _utcnow(), client_ip,
             photos[0] if photos else None, json.dumps(photos)),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def photos_from_row(row: dict) -> list[str]:
    """Chemins des photos d'un signalement (gère photos_json + legacy photo_path)."""
    raw = row.get("photos_json")
    if raw:
        try:
            v = json.loads(raw)
            if isinstance(v, list):
                return [p for p in v if p]
        except (ValueError, TypeError):
            pass
    p = row.get("photo_path")
    return [p] if p else []


def get_observation(obs_id: int, db_path: Path | str = DB_PATH) -> dict | None:
    """Un signalement par id (utilisé par la route photo, contrôle propriétaire)."""
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM contributor_observations WHERE id = ?", (obs_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def list_pending_observations(db_path: Path | str = DB_PATH) -> list[dict]:
    """Signalements à valider, enrichis du nom du contributeur (jointure)."""
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            """
            SELECT o.*, c.display_name, c.username, c.is_trusted
            FROM contributor_observations o
            JOIN contributors c ON c.id = o.contributor_id
            WHERE o.status = 'pending'
            ORDER BY o.submitted_at ASC
            """
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["photos"] = photos_from_row(d)
            out.append(d)
        return out
    finally:
        conn.close()


def list_observations_for(
    contributor_id: int, limit: int = 50, db_path: Path | str = DB_PATH
) -> list[dict]:
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            """SELECT * FROM contributor_observations
               WHERE contributor_id = ?
               ORDER BY submitted_at DESC LIMIT ?""",
            (contributor_id, limit),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["photos"] = photos_from_row(d)
            out.append(d)
        return out
    finally:
        conn.close()


def approve_observation(obs_id: int, db_path: Path | str = DB_PATH) -> int | None:
    """Approuve un signalement et le PROMEUT dans `beach_observations`.

    La ligne créée porte source='contributor' : la calibration la reprend
    automatiquement (sa requête inclut déjà Saint-Barth et ne filtre pas la
    source). Tout est fait dans une seule transaction.

    Retourne l'id créé dans beach_observations, ou None si le signalement est
    introuvable ou déjà traité.
    """
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM contributor_observations "
            "WHERE id = ? AND status = 'pending'",
            (obs_id,),
        ).fetchone()
        if row is None:
            return None
        # 1) Promotion dans la table lue par la calibration
        cur = conn.execute(
            """INSERT INTO beach_observations
               (observed_at, island, beach_name, observed_risk,
                coverage_pct, notes, source)
               VALUES (?, ?, ?, ?, ?, ?, 'contributor')""",
            (row["observed_at"], row["island"], row["beach_name"],
             row["observed_risk"], row["coverage_pct"], row["notes"]),
        )
        promoted_id = cur.lastrowid
        # 2) Marque le staging comme approuvé + lien vers la ligne promue
        conn.execute(
            "UPDATE contributor_observations "
            "SET status = 'approved', reviewed_at = ?, promoted_obs_id = ? "
            "WHERE id = ?",
            (_utcnow(), promoted_id, obs_id),
        )
        # 3) Compteur du contributeur (motivation / future confiance)
        conn.execute(
            "UPDATE contributors SET obs_count = obs_count + 1 WHERE id = ?",
            (row["contributor_id"],),
        )
        conn.commit()
        return promoted_id
    finally:
        conn.close()


def reject_observation(obs_id: int, db_path: Path | str = DB_PATH) -> bool:
    conn = get_connection(db_path)
    try:
        cur = conn.execute(
            "UPDATE contributor_observations "
            "SET status = 'rejected', reviewed_at = ? "
            "WHERE id = ? AND status = 'pending'",
            (_utcnow(), obs_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def latest_public_observations(
    island: str, within_hours: int = 24, db_path: Path | str = DB_PATH
) -> dict[str, dict]:
    """Dernière observation APPROUVÉE par plage, dans la fenêtre de fraîcheur.

    Pour l'affichage public sur la carte (à côté de la prévision). ANONYME :
    aucun nom de contributeur n'est exposé. On ne prend que les signalements
    `approved` (donc validés par Sam pour un usage public) et récents — au-delà
    de `within_hours`, une observation devient trompeuse pendant un épisode.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=within_hours)
              ).strftime("%Y-%m-%dT%H:%M:%S")
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            """SELECT beach_name, observed_risk, coverage_pct, observed_at,
                      photo_path, photos_json, id
               FROM contributor_observations
               WHERE island = ? AND status = 'approved' AND observed_at >= ?
               ORDER BY observed_at DESC""",
            (island, cutoff),
        ).fetchall()
        latest: dict[str, dict] = {}
        for r in rows:
            # Première occurrence = la plus récente (tri DESC) → on la garde
            if r["beach_name"] not in latest:
                latest[r["beach_name"]] = {
                    "risk": r["observed_risk"],
                    "coverage": r["coverage_pct"],
                    "observed_at": r["observed_at"],
                    "n_photos": len(photos_from_row(dict(r))),
                    "obs_id": r["id"],
                }
        return latest
    finally:
        conn.close()


def get_approved_photos(obs_id: int, db_path: Path | str = DB_PATH) -> list[str]:
    """Photos d'un signalement APPROUVÉ (pour la route photo publique).

    Liste vide si le signalement n'est pas approuvé — les photos en attente
    ou rejetées ne sont jamais servies publiquement.
    """
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT photo_path, photos_json FROM contributor_observations "
            "WHERE id = ? AND status = 'approved'",
            (obs_id,),
        ).fetchone()
        return photos_from_row(dict(row)) if row else []
    finally:
        conn.close()


def get_owner_photos(
    obs_id: int, contributor_id: int, db_path: Path | str = DB_PATH
) -> list[str]:
    """Photos d'un signalement appartenant à ce contributeur (route photo privée)."""
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT photo_path, photos_json FROM contributor_observations "
            "WHERE id = ? AND contributor_id = ?",
            (obs_id, contributor_id),
        ).fetchone()
        return photos_from_row(dict(row)) if row else []
    finally:
        conn.close()


def count_recent_submissions(
    contributor_id: int, since_iso: str, db_path: Path | str = DB_PATH
) -> int:
    """Nombre de signalements soumis depuis `since_iso` (rate-limit applicatif)."""
    conn = get_connection(db_path)
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM contributor_observations "
            "WHERE contributor_id = ? AND submitted_at >= ?",
            (contributor_id, since_iso),
        ).fetchone()[0]
    finally:
        conn.close()


# Permet `python contributors_db.py` pour créer/vérifier les tables à la main.
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
    print("Tables contributeurs initialisées dans", DB_PATH)
