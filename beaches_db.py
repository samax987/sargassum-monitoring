#!/usr/bin/env python3
"""
beaches_db.py
=============
Couche de persistance pour la configuration des plages.

Les plages etaient hardcodees dans beaches.py. Cette couche permet de les
stocker en DB pour permettre une edition dynamique via l'admin web.

Au premier appel, migre automatiquement les donnees depuis BEACHES de
beaches.py si la table est vide.
"""

import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent / "sargassum_data.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS beaches_config (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    island        TEXT    NOT NULL,
    name          TEXT    NOT NULL,
    lat           REAL    NOT NULL,
    lon           REAL    NOT NULL,
    radius_km     REAL    NOT NULL DEFAULT 2.0,
    exposure      TEXT    DEFAULT 'moderate',
    orientation   TEXT    DEFAULT '',
    description   TEXT    DEFAULT '',
    active        INTEGER NOT NULL DEFAULT 1,
    updated_at    TEXT    DEFAULT (datetime('now')),
    UNIQUE(island, name)
);
CREATE INDEX IF NOT EXISTS idx_beaches_active ON beaches_config(active);
CREATE INDEX IF NOT EXISTS idx_beaches_island ON beaches_config(island);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def is_table_empty() -> bool:
    conn = get_conn()
    cur = conn.execute("SELECT COUNT(*) FROM beaches_config")
    count = cur.fetchone()[0]
    conn.close()
    return count == 0


def seed_from_hardcoded(hardcoded: list[dict]) -> int:
    """Migre les plages hardcodees vers la DB (idempotent)."""
    conn = get_conn()
    inserted = 0
    for b in hardcoded:
        try:
            conn.execute("""
                INSERT INTO beaches_config (island, name, lat, lon, radius_km, active)
                VALUES (?, ?, ?, ?, ?, 1)
            """, (b['island'], b['name'], b['lat'], b['lon'], b.get('radius_km', 2.0)))
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # deja present
    conn.commit()
    conn.close()
    return inserted


def list_all(only_active: bool = True) -> list[dict]:
    """Retourne toutes les plages (compatible BEACHES original)."""
    conn = get_conn()
    sql = "SELECT * FROM beaches_config"
    if only_active:
        sql += " WHERE active = 1"
    sql += " ORDER BY island, name"
    rows = conn.execute(sql).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def list_for_island(island: str, only_active: bool = True) -> list[dict]:
    conn = get_conn()
    sql = "SELECT * FROM beaches_config WHERE island = ?"
    params = [island]
    if only_active:
        sql += " AND active = 1"
    sql += " ORDER BY name"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_by_id(beach_id: int) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("SELECT * FROM beaches_config WHERE id = ?", (beach_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_beach(beach_id: int, **fields) -> bool:
    """Met a jour les champs autorises d'une plage."""
    allowed = {'island', 'name', 'lat', 'lon', 'radius_km',
               'exposure', 'orientation', 'description', 'active'}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return False

    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    set_clause += ", updated_at = datetime('now')"
    values = list(updates.values()) + [beach_id]

    conn = get_conn()
    cur = conn.execute(f"UPDATE beaches_config SET {set_clause} WHERE id = ?", values)
    conn.commit()
    ok = cur.rowcount > 0
    conn.close()
    return ok


def create_beach(island: str, name: str, lat: float, lon: float,
                 radius_km: float = 2.0, **extras) -> Optional[int]:
    conn = get_conn()
    try:
        cur = conn.execute("""
            INSERT INTO beaches_config (island, name, lat, lon, radius_km,
                                        exposure, orientation, description, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
        """, (
            island, name, lat, lon, radius_km,
            extras.get('exposure', 'moderate'),
            extras.get('orientation', ''),
            extras.get('description', ''),
        ))
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        return None
    finally:
        conn.close()


def delete_beach(beach_id: int) -> bool:
    """Suppression douce : passe active = 0 (preserve les scores historiques)."""
    return update_beach(beach_id, active=0)


def hard_delete(beach_id: int) -> bool:
    """Suppression definitive (a eviter si des scores existent)."""
    conn = get_conn()
    cur = conn.execute("DELETE FROM beaches_config WHERE id = ?", (beach_id,))
    conn.commit()
    ok = cur.rowcount > 0
    conn.close()
    return ok
