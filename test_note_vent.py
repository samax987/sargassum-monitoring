"""Tests de la note en parallèle (sarga_note_vent.py). Aucun accès réseau : satellite et vent sont simulés."""

import sqlite3
from datetime import datetime, timedelta

import numpy as np
import pytest

import sarga_note_vent as nv


def base(tmp_path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE beaches_config (name TEXT, lat REAL, lon REAL)")
    conn.execute("INSERT INTO beaches_config VALUES ('Flamands', 17.919, -62.857)")
    conn.execute("CREATE TABLE webcam_truth (beach_name TEXT, capture_date TEXT, verdict TEXT)")
    conn.execute("CREATE TABLE beach_observations (id INTEGER)")
    conn.commit()
    return conn


def vent_constant(direction: float, jour: str = "2026-11-22", trou: bool = False) -> dict:
    t0 = datetime.strptime(jour, "%Y-%m-%d") - timedelta(days=2)
    v = {(t0 + timedelta(hours=h)).strftime("%Y-%m-%dT%H:00"): (15.0, direction) for h in range(120)}
    if trou:
        v.pop(f"{jour}T05:00")
    return v


def grille(n_proche: int):
    """Grille autour de Flamands avec n_proche pixels détectés tout près, et un coin illisible au loin."""
    lats, lons = np.linspace(16.4, 19.4, 67), np.linspace(-64.4, -61.3, 70)
    a = np.zeros((67, 70), dtype="float32")
    d = nv.distances_km(17.919, -62.857, lats, lons)
    ordre = np.argsort(d.ravel())[:n_proche]
    a.ravel()[ordre] = 5e-4
    a[0, :5] = np.nan
    return lats, lons, a


def test_vent_moyen_passe_le_nord():
    d, v = nv.vent_moyen([10, 10], [350, 10])
    assert min(d, 360 - d) < 1e-6 and v == 10


def test_vent_moyen_refuse_le_vide():
    with pytest.raises(ValueError):
        nv.vent_moyen([], [])


def test_compter_pixels_par_rayon():
    lats, lons, a = grille(6)
    c = nv.compter_pixels(a, nv.distances_km(17.919, -62.857, lats, lons))
    assert c["n25"] == c["n50"] == c["n100"] == 6 and c["lisible50"] == 1.0


def test_pixel_illisible_ne_compte_pas():
    lats, lons, a = grille(0)
    d = nv.distances_km(17.919, -62.857, lats, lons)
    a[d < 20] = np.nan
    c = nv.compter_pixels(a, d)
    assert c["n50"] == 0 and c["lisible50"] < 1.0


def test_note_nulle_quand_le_vent_ne_porte_pas():
    assert nv.note(40, 89.9, 90.0) == (1, 40.0)
    assert nv.note(40, 90.1, 90.0) == (0, 0.0)


def test_vent_des_24h_refuse_une_serie_trouee():
    with pytest.raises(ValueError):
        nv.vent_des_24h(vent_constant(80, trou=True), "2026-11-22")


def test_calculer_deux_lignes_et_rejouer_sans_doublon(tmp_path, monkeypatch):
    conn = base(tmp_path)
    monkeypatch.setattr(nv, "lire_satellite", lambda zone, pas: ("2026-11-21", *grille(7)))
    monkeypatch.setattr(nv, "lire_vent", lambda lat, lon: vent_constant(80))
    conn.executescript(nv.SCHEMA)
    lignes = nv.calculer(conn, "2026-11-22")
    assert [(l["target_date"], l["lead_days"], l["note"]) for l in lignes] == [("2026-11-22", 0, 7.0), ("2026-11-23", 1, 7.0)]
    nv.enregistrer(conn, lignes)
    monkeypatch.setattr(nv, "lire_vent", lambda lat, lon: vent_constant(120))
    nv.enregistrer(conn, nv.calculer(conn, "2026-11-22"))
    rows = conn.execute("SELECT target_date, lead_days, porte, note, n50 FROM note_vent_parallele ORDER BY 1").fetchall()
    assert rows == [("2026-11-22", 0, 0, 0.0, 7), ("2026-11-23", 1, 0, 0.0, 7)]
    # Règle d'or : rien d'autre n'est écrit
    assert conn.execute("SELECT COUNT(*) FROM beach_observations").fetchone()[0] == 0


def test_composite_trop_vieux_refuse(tmp_path, monkeypatch):
    conn = base(tmp_path)
    monkeypatch.setattr(nv, "lire_satellite", lambda zone, pas: ("2026-11-10", *grille(7)))
    monkeypatch.setattr(nv, "lire_vent", lambda lat, lon: vent_constant(80))
    with pytest.raises(ValueError):
        nv.calculer(conn, "2026-11-22")


def test_main_sort_en_code_1_sans_rien_ecrire_si_le_satellite_tombe(tmp_path, monkeypatch):
    conn = base(tmp_path)
    conn.close()
    monkeypatch.setattr(nv, "DB_PATH", tmp_path / "t.db")
    monkeypatch.setattr(nv, "lire_satellite", lambda zone, pas: (_ for _ in ()).throw(RuntimeError("500")))
    monkeypatch.setattr("sys.argv", ["x", "--run"])
    assert nv.main() == 1
    c = sqlite3.connect(tmp_path / "t.db")
    assert c.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='note_vent_parallele'").fetchone()[0] == 0


def test_tri():
    assert nv.tri([5, 0, 0], [1, 0, 0]) == 1.0
    assert nv.tri([0, 0], [1, 0]) == 0.5
    assert nv.tri([1, 2], [0, 0]) is None


def test_bilan_compte_les_jours_manques(tmp_path, monkeypatch, capsys):
    conn = base(tmp_path)
    conn.executescript(nv.SCHEMA)
    monkeypatch.setattr(nv, "lire_satellite", lambda zone, pas: ("2026-11-21", *grille(7)))
    monkeypatch.setattr(nv, "lire_vent", lambda lat, lon: vent_constant(80))
    nv.enregistrer(conn, nv.calculer(conn, "2026-11-22"))
    monkeypatch.setattr(nv, "lire_satellite", lambda zone, pas: ("2026-11-24", *grille(0)))
    monkeypatch.setattr(nv, "lire_vent", lambda lat, lon: vent_constant(80, "2026-11-25"))
    nv.enregistrer(conn, nv.calculer(conn, "2026-11-25"))
    conn.executemany("INSERT INTO webcam_truth VALUES ('Flamands', ?, ?)", [("2026-11-22", "radeaux"), ("2026-11-25", "rien")])
    conn.commit()
    nv.bilan(conn)
    s = capsys.readouterr().out
    assert "2 jours rangés sur 4" in s and "tri 1.00" in s
