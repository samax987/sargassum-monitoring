#!/usr/bin/env python3
"""
test_beaches.py
===============
Tests unitaires et de regression pour beaches.py

Couverture:
  - haversine_km()        : distances connues
  - _score_beach()        : scoring gaussien
  - risk_label()          : seuils de risque
  - compute_beach_scores(): integration DB (SQLite en memoire)
  - Cas limites           : liste vide, NaN, ratio=0

Usage:
  python test_beaches.py
  python -m pytest test_beaches.py -v
"""

import json
import math
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# Ajout du repertoire parent pour pouvoir importer beaches.py
sys.path.insert(0, str(Path(__file__).parent))
import beaches


class TestHaversine(unittest.TestCase):
    """Tests de la formule haversine_km."""

    def test_same_point_zero(self):
        """Distance d'un point a lui-meme = 0."""
        self.assertAlmostEqual(beaches.haversine_km(14.39, -60.86, 14.39, -60.86), 0.0)

    def test_equator_one_degree(self):
        """1 degre de longitude sur l'equateur ~ 111.2 km."""
        d = beaches.haversine_km(0.0, 0.0, 0.0, 1.0)
        self.assertAlmostEqual(d, 111.2, delta=0.5)

    def test_paris_london_approx(self):
        """Paris-Londres ~ 340 km (test de plausibilite)."""
        d = beaches.haversine_km(48.85, 2.35, 51.51, -0.13)
        self.assertAlmostEqual(d, 340, delta=10)

    def test_symmetry(self):
        """Distance A→B = distance B→A."""
        d1 = beaches.haversine_km(14.39, -60.86, 17.90, -62.83)
        d2 = beaches.haversine_km(17.90, -62.83, 14.39, -60.86)
        self.assertAlmostEqual(d1, d2, places=6)

    def test_martinique_saint_barth(self):
        """Martinique (Les Salines) → Saint-Barth (Saint-Jean) ~ 370-390 km."""
        d = beaches.haversine_km(14.3917, -60.8617, 17.9000, -62.8267)
        self.assertGreater(d, 360)
        self.assertLess(d, 460)


class TestRiskLabel(unittest.TestCase):
    """Tests de la fonction risk_label()."""

    def test_none_below_low_threshold(self):
        self.assertEqual(beaches.risk_label(0.0), "none")
        self.assertEqual(beaches.risk_label(4.99), "none")

    def test_low_boundary(self):
        self.assertEqual(beaches.risk_label(5.0), "low")
        self.assertEqual(beaches.risk_label(24.99), "low")

    def test_medium_boundary(self):
        self.assertEqual(beaches.risk_label(25.0), "medium")
        self.assertEqual(beaches.risk_label(74.99), "medium")

    def test_high_boundary(self):
        self.assertEqual(beaches.risk_label(75.0), "high")
        self.assertEqual(beaches.risk_label(1000.0), "high")

    def test_negative_score(self):
        """Score negatif -> none (ne devrait pas arriver mais doit etre robuste)."""
        self.assertEqual(beaches.risk_label(-1.0), "none")


class TestScoreBeach(unittest.TestCase):
    """Tests de la fonction _score_beach()."""

    def test_empty_positions(self):
        """Aucune particule -> tous les scores a zero, closest_km = None."""
        result = beaches._score_beach([], 14.39, -60.86, 3.0, 1.0)
        self.assertEqual(result["sample_count"], 0)
        self.assertEqual(result["local_score"], 0.0)
        self.assertEqual(result["regional_score"], 0.0)
        self.assertIsNone(result["closest_km"])
        self.assertEqual(result["density_km2"], 0.0)

    def test_particle_at_beach(self):
        """Particule exactement sur la plage -> score gaussien maximal."""
        beach_lat, beach_lon, radius = 14.39, -60.86, 3.0
        positions = [[beach_lon, beach_lat]]  # format [lon, lat]
        result = beaches._score_beach(positions, beach_lat, beach_lon, radius, 1.0)
        self.assertEqual(result["sample_count"], 1)
        self.assertAlmostEqual(result["closest_km"], 0.0, places=1)
        # Gaussienne en 0 = exp(0) = 1.0 * ratio=1 -> local_score ~ 1.0
        self.assertAlmostEqual(result["local_score"], 1.0, places=2)

    def test_particle_far_away(self):
        """Particule tres loin -> scores proches de zero."""
        beach_lat, beach_lon, radius = 14.39, -60.86, 3.0
        # Particule a ~500 km (Atlantique)
        positions = [[-50.0, 14.0]]
        result = beaches._score_beach(positions, beach_lat, beach_lon, radius, 1.0)
        self.assertEqual(result["sample_count"], 0)
        self.assertAlmostEqual(result["local_score"], 0.0, places=3)
        # Regional score: sigma=50km, dist>>50km -> quasi zero
        self.assertAlmostEqual(result["regional_score"], 0.0, places=3)

    def test_ratio_scaling(self):
        """Le ratio multiplie les scores de facon lineaire."""
        beach_lat, beach_lon, radius = 14.39, -60.86, 5.0
        positions = [[beach_lon, beach_lat]]

        r1 = beaches._score_beach(positions, beach_lat, beach_lon, radius, 1.0)
        r10 = beaches._score_beach(positions, beach_lat, beach_lon, radius, 10.0)

        self.assertAlmostEqual(r10["local_score"], r1["local_score"] * 10, places=2)
        self.assertAlmostEqual(r10["regional_score"], r1["regional_score"] * 10, places=2)

    def test_multiple_particles(self):
        """Plusieurs particules s'accumulent correctement."""
        beach_lat, beach_lon, radius = 14.39, -60.86, 5.0
        # 3 particules tres proches de la plage
        positions = [
            [beach_lon, beach_lat],
            [beach_lon + 0.01, beach_lat],
            [beach_lon, beach_lat + 0.01],
        ]
        result = beaches._score_beach(positions, beach_lat, beach_lon, radius, 1.0)
        self.assertEqual(result["sample_count"], 3)
        self.assertGreater(result["local_score"], 2.5)  # 3 gaussiennes ~ 3

    def test_malformed_position_skipped(self):
        """Les positions malformees (trop courtes) sont ignorees sans erreur."""
        positions = [[], [1.0], [-60.86, 14.39]]  # seule la derniere est valide
        result = beaches._score_beach(positions, 14.39, -60.86, 5.0, 1.0)
        # Doit traiter uniquement la 3e position sans lever d'exception
        self.assertIsNotNone(result)
        self.assertGreaterEqual(result["sample_count"], 0)

    def test_density_calculation(self):
        """density_km2 = est_count / (pi * radius^2)."""
        beach_lat, beach_lon, radius = 14.39, -60.86, 3.0
        positions = [[beach_lon, beach_lat]]  # 1 particule dans le rayon
        result = beaches._score_beach(positions, beach_lat, beach_lon, radius, 1.0)
        expected_area = math.pi * radius ** 2
        expected_density = 1.0 / expected_area
        self.assertAlmostEqual(result["density_km2"], expected_density, places=4)


class TestStrandedFlag(unittest.TestCase):
    """Jeu régional complet : points [lon, lat, flag] (flag 1 = échouée)."""

    def test_three_field_points_accepted(self):
        """Un point à 3 champs est scoré comme un point à 2 champs."""
        pts2 = [[-60.86, 14.39], [-60.87, 14.40]]
        pts3 = [[-60.86, 14.39, 1], [-60.87, 14.40, 0]]
        r2 = beaches._score_beach(pts2, 14.39, -60.86, 3.0, 1.0)
        r3 = beaches._score_beach(pts3, 14.39, -60.86, 3.0, 1.0)
        self.assertEqual(r2["local_score"], r3["local_score"])
        self.assertEqual(r2["regional_score"], r3["regional_score"])

    def test_n_stranded_counts_only_in_radius(self):
        """n_stranded = échouées DANS radius_km uniquement."""
        pts = [[-60.86, 14.39, 1],        # sur la plage, échouée
               [-60.861, 14.391, 0],      # sur la plage, active
               [-62.83, 17.90, 1]]        # échouée mais à 400 km
        r = beaches._score_beach(pts, 14.39, -60.86, 3.0, 1.0)
        self.assertEqual(r["n_stranded"], 1)
        self.assertEqual(r["sample_count"], 2)

    def test_no_flag_means_zero_stranded(self):
        r = beaches._score_beach([[-60.86, 14.39]], 14.39, -60.86, 3.0, 1.0)
        self.assertEqual(r["n_stranded"], 0)

    def test_shift_keeps_flag(self):
        """La correction de biais ne doit pas perdre le flag échouage."""
        out = beaches._shift_positions([[-60.86, 14.39, 1], [-60.0, 14.0]], 5.0, -5.0, 14.39)
        self.assertEqual(len(out[0]), 3)
        self.assertEqual(out[0][2], 1)
        self.assertEqual(len(out[1]), 2)

    def test_decode_region_roundtrip_and_garbage(self):
        import zlib as _zlib, json as _json
        region = [[-60.86, 14.39, 1]]
        blob = _zlib.compress(_json.dumps(region).encode())
        self.assertEqual(beaches._decode_region(blob), region)
        self.assertIsNone(beaches._decode_region(None))
        self.assertIsNone(beaches._decode_region(b"pas du zlib"))


class TestExposureWindow(unittest.TestCase):
    """Fenêtre d'exposition : seules les particules visibles par la mer comptent
    dans le score local. Fenêtre synthétique : ouverte au nord (330°–30°) sur
    40 km, fermée partout ailleurs."""

    def _window(self):
        open_km = [0.0] * 72
        for b in list(range(330, 360, 5)) + list(range(0, 35, 5)):
            open_km[b // 5] = 40.0
        return {"step_deg": 5, "max_km": 40.0, "open_km": open_km,
                "snap_lat": 17.92, "snap_lon": -62.85}

    def test_bearing(self):
        self.assertAlmostEqual(beaches._bearing_deg(17.92, -62.85, 18.02, -62.85), 0.0, delta=0.5)   # nord
        self.assertAlmostEqual(beaches._bearing_deg(17.92, -62.85, 17.92, -62.75), 90.0, delta=0.5)  # est
        self.assertAlmostEqual(beaches._bearing_deg(17.92, -62.85, 17.82, -62.85), 180.0, delta=0.5) # sud

    def test_visible_by_sea(self):
        w = self._window()
        self.assertTrue(beaches._visible_by_sea(w, 0.0, 10.0))      # nord ouvert
        self.assertTrue(beaches._visible_by_sea(w, 350.0, 39.0))    # NNO ouvert
        self.assertFalse(beaches._visible_by_sea(w, 180.0, 1.0))    # sud fermé, même tout près
        self.assertTrue(beaches._visible_by_sea(w, 0.0, 200.0))     # au-delà du plafond : ouvert
        self.assertTrue(beaches._visible_by_sea(None, 180.0, 1.0))  # sans fenêtre : pas de filtre
        self.assertTrue(beaches._visible_by_sea({"open_km": []}, 180.0, 1.0))

    def test_tolerance_one_sector(self):
        """Une particule à 35° (secteur fermé) passe grâce au voisin ouvert à 30°."""
        w = self._window()
        self.assertTrue(beaches._visible_by_sea(w, 35.0, 10.0))
        self.assertFalse(beaches._visible_by_sea(w, 45.0, 10.0))

    def test_local_score_filters_hidden_particles(self):
        """Trois particules à 1 km : en mer au nord (visible), en mer au sud
        (cachée par la terre), échouée au sud (comptée : pas de test pour les
        échouées, posées à terre par pas de 3 h)."""
        w = self._window()
        lat, lon = 17.92, -62.85
        north = [lon, lat + 0.009]           # ~1 km au nord, en mer
        south = [lon, lat - 0.009]           # ~1 km au sud, en mer
        south_stranded = [lon + 0.002, lat - 0.009, 1]
        pts = [north, south, south_stranded]
        raw = beaches._score_beach(pts, lat, lon, 2.0, 1.0)
        win = beaches._score_beach(pts, lat, lon, 2.0, 1.0, None, w)
        self.assertEqual(raw["sample_count"], 3)
        self.assertEqual(win["sample_count"], 2)
        self.assertEqual(win["sample_hidden"], 1)
        self.assertEqual(win["n_stranded"], 1)
        self.assertAlmostEqual(win["local_score_raw"], raw["local_score"], places=3)
        self.assertLess(win["local_score"], win["local_score_raw"])
        # le régional n'est PAS filtré…
        self.assertAlmostEqual(win["regional_score"], raw["regional_score"], places=3)
        # …mais regional_exposed exclut la particule en mer cachée (2 sur 3 visibles)
        self.assertLess(win["regional_exposed"], win["regional_score"])
        self.assertAlmostEqual(win["exposed_frac"], 2 / 3, places=2)
        self.assertIsNone(raw["exposed_frac"])

    def test_exposed_frac_discriminates_coasts(self):
        """Masse à 15 km au NORD : une plage ouverte au nord voit ~100 % de la
        masse régionale lui faire face, une plage ouverte au SUD ~0 %."""
        north_window = self._window()
        south_window = dict(north_window)
        south_window["open_km"] = north_window["open_km"][36:] + north_window["open_km"][:36]  # +180°
        lat, lon = 17.92, -62.85
        mass = [[lon + dx, lat + 0.135] for dx in (-0.02, -0.01, 0.0, 0.01, 0.02)]  # ~15 km N
        n = beaches._score_beach(mass, lat, lon, 2.0, 1.0, None, north_window)
        s = beaches._score_beach(mass, lat, lon, 2.0, 1.0, None, south_window)
        self.assertAlmostEqual(n["regional_score"], s["regional_score"], places=3)   # même masse
        self.assertGreater(n["exposed_frac"], 0.99)
        self.assertLess(s["exposed_frac"], 0.01)

    def test_far_stranded_is_tested(self):
        """Une échouée à 20 km (côte d'une autre île) n'est pas exemptée du test."""
        w = self._window()
        lat, lon = 17.92, -62.85
        far_south_stranded = [lon, lat - 0.18, 1]      # ~20 km au sud, fermé
        s = beaches._score_beach([far_south_stranded], lat, lon, 2.0, 1.0, None, w)
        self.assertEqual(s["regional_exposed"], 0.0)
        self.assertGreater(s["regional_score"], 0.0)

    def test_eye_point_used_when_present(self):
        """Avec eye_lat/eye_lon, les azimuts sont mesurés depuis l'œil au large."""
        w = self._window()
        w["eye_lat"], w["eye_lon"] = 17.925, -62.85   # 550 m au nord de la plage
        lat, lon = 17.92, -62.85
        # particule à l'est de l'œil (90°, fermé) mais au NE de la plage (ouvert à 30°…)
        pt = [lon + 0.005, 17.925]
        self.assertFalse(beaches._visible_by_sea(w, beaches._bearing_deg(17.925, -62.85, 17.925, lon + 0.005), 0.5))
        s = beaches._score_beach([pt], lat, lon, 2.0, 1.0, None, w)
        self.assertEqual(s["sample_hidden"], 1)

    def test_no_window_is_identity(self):
        pts = [[-62.85, 17.93], [-62.85, 17.91, 1]]
        a = beaches._score_beach(pts, 17.92, -62.85, 2.0, 1.0)
        b = beaches._score_beach(pts, 17.92, -62.85, 2.0, 1.0, None, None)
        self.assertEqual(a["local_score"], b["local_score"])
        self.assertEqual(a["local_score"], a["local_score_raw"])
        self.assertEqual(b["n_stranded"], 1)


class TestComputeBeachScoresIntegration(unittest.TestCase):
    """Tests d'integration avec une DB SQLite en memoire."""

    def setUp(self):
        """Cree une DB en memoire avec des donnees de simulation factices."""
        import tempfile
        self.db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = Path(self.db_file.name)

        conn = sqlite3.connect(self.db_path)
        # Schema aligne sur la prod (resolution 3h de mai 2026) :
        # hour_offset et positions_viz_json sont requis par beaches.py.
        conn.executescript("""
            CREATE TABLE drift_predictions (
                id INTEGER PRIMARY KEY,
                simulated_at TEXT,
                sim_start TEXT,
                sim_end TEXT,
                n_particles INTEGER,
                current_source TEXT,
                day_offset INTEGER,
                lon_min REAL, lon_max REAL, lat_min REAL, lat_max REAL,
                active_fraction REAL,
                positions_json TEXT,
                raw_metadata TEXT,
                hour_offset INTEGER,
                positions_viz_json TEXT
            );
        """)

        # Inserer des donnees factices: 100 particules, 6 jours
        import json as _json
        simulated_at = "2026-03-31T12:00:00Z"
        # Particules concentrees autour de la Martinique (14.39, -60.86)
        positions = [[-60.86 + i * 0.01, 14.39 + i * 0.01] for i in range(10)]
        positions_json = _json.dumps(positions)

        # hour_offset = day*24 : snapshots de bord de journee, comme en prod
        # (beaches.py filtre hour_offset % 24 = 0 pour le scoring journalier)
        for day in range(6):
            conn.execute("""
                INSERT INTO drift_predictions
                (simulated_at, n_particles, current_source, day_offset,
                 active_fraction, positions_json, hour_offset)
                VALUES (?, 100, 'test', ?, 1.0, ?, ?)
            """, (simulated_at, day, positions_json, day * 24))

        conn.commit()
        conn.close()

    def tearDown(self):
        import os
        os.unlink(self.db_path)

    def test_compute_inserts_scores(self):
        """compute_beach_scores() insere des scores pour chaque plage x jour."""
        n = beaches.compute_beach_scores(self.db_path)
        expected = len(beaches.BEACHES) * len(beaches.DAY_OFFSETS)
        self.assertEqual(n, expected)

    def test_scores_in_db(self):
        """Apres compute, la DB contient bien des entrees dans beach_risk_scores."""
        beaches.compute_beach_scores(self.db_path)
        conn = sqlite3.connect(self.db_path)
        count = conn.execute("SELECT COUNT(*) FROM beach_risk_scores").fetchone()[0]
        conn.close()
        self.assertGreater(count, 0)

    def test_risk_levels_valid(self):
        """Tous les risk_level sont dans {none, low, medium, high}."""
        beaches.compute_beach_scores(self.db_path)
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT DISTINCT risk_level FROM beach_risk_scores").fetchall()
        conn.close()
        valid = {"none", "low", "medium", "high"}
        for row in rows:
            self.assertIn(row[0], valid)

    def test_no_simulation_data(self):
        """Sans simulation, compute_beach_scores retourne 0."""
        import tempfile, os
        empty_db = Path(tempfile.mktemp(suffix=".db"))
        # Creer la table requise mais vide
        conn = __import__("sqlite3").connect(empty_db)
        conn.execute("""CREATE TABLE drift_predictions (
            id INTEGER PRIMARY KEY, simulated_at TEXT,
            sim_start TEXT, sim_end TEXT, n_particles INTEGER,
            current_source TEXT, day_offset INTEGER,
            lon_min REAL, lon_max REAL, lat_min REAL, lat_max REAL,
            active_fraction REAL, positions_json TEXT, raw_metadata TEXT
        )""")
        conn.commit()
        conn.close()
        n = beaches.compute_beach_scores(empty_db)
        self.assertEqual(n, 0)
        if empty_db.exists():
            os.unlink(empty_db)

    def test_archive_filled_and_not_purged(self):
        """Chaque run alimente forecast_archive (jamais purgé) avec target_date."""
        beaches.compute_beach_scores(self.db_path)
        beaches.compute_beach_scores(self.db_path)
        conn = sqlite3.connect(self.db_path)
        n_arch = conn.execute("SELECT COUNT(*) FROM forecast_archive").fetchone()[0]
        row = conn.execute(
            "SELECT target_date, day_offset, presence, scoring_mode FROM forecast_archive "
            "WHERE day_offset = 2 LIMIT 1").fetchone()
        conn.close()
        # 2 runs → 2× les lignes : rien n'est purgé (même si computed_at identique)
        self.assertEqual(n_arch, 2 * len(beaches.BEACHES) * len(beaches.DAY_OFFSETS))
        # simulated_at = 2026-03-31 → j+2 vise le 2 avril
        self.assertEqual(row[0], "2026-04-02")
        self.assertIn(row[2], {"none", "low", "medium", "high"})
        self.assertEqual(row[3], "sample")   # pas de jeu régional dans cette fixture

    def test_archive_note_le_masque_cotier(self):
        """Le masque côtier noté par le collecteur est recopié dans forecast_archive (2026-11-11)."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""UPDATE drift_predictions
                        SET raw_metadata = '{"day": 0, "seed_coast_km": 15.0, "n_seed_avant_masque": 6225}'""")
        conn.commit()
        conn.close()
        beaches.compute_beach_scores(self.db_path)
        conn = sqlite3.connect(self.db_path)
        vals = conn.execute("SELECT DISTINCT seed_coast_km FROM forecast_archive").fetchall()
        conn.close()
        self.assertEqual(vals, [(15.0,)])

    def test_archive_masque_inconnu_reste_vide(self):
        """Sans trace du masque (ancienne simulation), la colonne reste NULL : inconnu, pas 0."""
        beaches.compute_beach_scores(self.db_path)
        conn = sqlite3.connect(self.db_path)
        vals = conn.execute("SELECT DISTINCT seed_coast_km FROM forecast_archive").fetchall()
        conn.close()
        self.assertEqual(vals, [(None,)])

    def test_archive_note_le_semis_date(self):
        """Le semis daté noté par le collecteur est recopié dans forecast_archive (2026-11-16)."""
        for meta, attendu in (('{"day": 0, "seed_dated": true}', 1), ('{"day": 0, "seed_dated": false}', 0),
                              ('{"day": 0, "seed_dated": null}', None), ('{"day": 0}', None)):
            conn = sqlite3.connect(self.db_path)
            conn.execute("DELETE FROM forecast_archive") if conn.execute(
                "SELECT name FROM sqlite_master WHERE name='forecast_archive'").fetchone() else None
            conn.execute("UPDATE drift_predictions SET raw_metadata = ?", (meta,))
            conn.commit()
            conn.close()
            beaches.compute_beach_scores(self.db_path)
            conn = sqlite3.connect(self.db_path)
            vals = conn.execute("SELECT DISTINCT seed_dated FROM forecast_archive").fetchall()
            conn.close()
            self.assertEqual(vals, [(attendu,)], meta)

    def test_region_blob_scoring_mode(self):
        """Avec positions_region_blob, le scoring se fait sur le jeu complet (ratio 1)
        et compte les particules échouées dans le rayon de la plage."""
        import zlib as _zlib, json as _json
        conn = sqlite3.connect(self.db_path)
        conn.execute("ALTER TABLE drift_predictions ADD COLUMN positions_region_blob BLOB")
        # 3 particules sur Les Salines (Martinique) : 2 échouées, 1 active ;
        # 1 particule active loin (Saint-Barth).
        salines = next(b for b in beaches.BEACHES if b["name"] == "Les_Salines")
        region = [
            [salines["lon"], salines["lat"], 1],
            [salines["lon"] + 0.001, salines["lat"], 1],
            [salines["lon"] - 0.001, salines["lat"], 0],
            [-62.83, 17.90, 0],
        ]
        blob = _zlib.compress(_json.dumps(region).encode())
        conn.execute("UPDATE drift_predictions SET positions_region_blob = ?", (blob,))
        conn.commit()
        conn.close()

        beaches.compute_beach_scores(self.db_path)
        conn = sqlite3.connect(self.db_path)
        r = conn.execute(
            "SELECT n_stranded, sample_count, est_count, n_sample FROM beach_risk_scores "
            "WHERE beach_name = 'Les_Salines' AND day_offset = 0").fetchone()
        mode = conn.execute("SELECT DISTINCT scoring_mode FROM forecast_archive").fetchall()
        conn.close()
        self.assertEqual(r[0], 2)          # 2 échouées dans le rayon
        self.assertEqual(r[1], 3)          # 3 particules dans le rayon
        self.assertEqual(r[2], 3.0)        # ratio 1 : pas d'extrapolation
        self.assertEqual(r[3], 4)          # n_sample = taille du jeu complet
        self.assertEqual(mode, [("region",)])

    def test_purge_keeps_60_computed_at(self):
        """La purge ne garde que les 60 derniers computed_at."""
        # Creer les tables requises avant insertion
        beaches._get_conn(self.db_path).close()
        # Inserer 65 runs distincts (simulation du meme snapshot)
        conn = sqlite3.connect(self.db_path)
        for i in range(65):
            ts = f"2026-01-{i+1:02d}T00:00:00Z"
            for beach in beaches.BEACHES[:2]:  # juste 2 plages pour la vitesse
                conn.execute("""
                    INSERT INTO beach_risk_scores
                    (computed_at, simulated_at, island, beach_name, beach_lat, beach_lon,
                     radius_km, day_offset, sample_count, risk_level)
                    VALUES (?, '2026-01-01T00:00:00Z', ?, ?, ?, ?, ?, 0, 0, 'none')
                """, (ts, beach.get("island", ""), beach["name"],
                      beach["lat"], beach["lon"], beach["radius_km"]))
        conn.commit()
        conn.close()

        # Appeler compute (qui fait la purge apres insertion)
        beaches.compute_beach_scores(self.db_path)

        conn = sqlite3.connect(self.db_path)
        distinct = conn.execute(
            "SELECT COUNT(DISTINCT computed_at) FROM beach_risk_scores"
        ).fetchone()[0]
        conn.close()
        self.assertLessEqual(distinct, 60)


class TestBeachesData(unittest.TestCase):
    """Tests de validite des donnees statiques (coordonnees GPS)."""

    def test_all_beaches_have_required_fields(self):
        """Chaque plage doit avoir name, lat, lon, radius_km, island."""
        for beach in beaches.BEACHES:
            self.assertIn("name", beach, f"Champ 'name' manquant: {beach}")
            self.assertIn("lat", beach, f"Champ 'lat' manquant: {beach}")
            self.assertIn("lon", beach, f"Champ 'lon' manquant: {beach}")
            self.assertIn("radius_km", beach, f"Champ 'radius_km' manquant: {beach}")
            self.assertIn("island", beach, f"Champ 'island' manquant: {beach}")

    def test_coordinates_in_caribbean_range(self):
        """Toutes les plages doivent etre dans les Caraibes."""
        for beach in beaches.BEACHES:
            self.assertGreater(beach["lat"], 13.0, f"Latitude trop basse: {beach['name']}")
            self.assertLess(beach["lat"], 19.0, f"Latitude trop haute: {beach['name']}")
            self.assertGreater(beach["lon"], -64.0, f"Longitude trop basse: {beach['name']}")
            self.assertLess(beach["lon"], -60.0, f"Longitude trop haute: {beach['name']}")

    def test_radius_positive(self):
        """radius_km doit etre positif."""
        for beach in beaches.BEACHES:
            self.assertGreater(beach["radius_km"], 0, f"radius_km <= 0: {beach['name']}")

    def test_expected_islands_present(self):
        """Les 5 iles attendues sont representees."""
        islands = {b["island"] for b in beaches.BEACHES}
        expected = {"Saint-Barth", "Saint-Martin", "Martinique", "Guadeloupe", "Marie-Galante"}
        self.assertEqual(islands, expected)

    def test_no_duplicate_beach_names_per_island(self):
        """Pas de doublons de nom dans la meme ile."""
        from collections import Counter
        keys = [(b["island"], b["name"]) for b in beaches.BEACHES]
        counts = Counter(keys)
        duplicates = [k for k, v in counts.items() if v > 1]
        self.assertEqual(duplicates, [], f"Doublons detectes: {duplicates}")

    def test_saint_barth_beach_count(self):
        """Saint-Barth contient au moins les 10 plages historiques.

        BEACHES est charge depuis la table beaches_config quand elle existe :
        des plages peuvent etre AJOUTEES via l'admin (ex. Anse des Cayes).
        On verifie qu'aucune plage historique n'a disparu, sans casser a
        chaque ajout legitime.
        """
        sbh = {b["name"] for b in beaches.BEACHES if b["island"] == "Saint-Barth"}
        historiques = {
            "Flamands", "Colombier", "Saint-Jean", "Lorient",
            "Grand_Cul-de-Sac", "Petit_Cul-de-Sac", "Toiny",
            "Gouverneur", "Grande_Saline", "Marigot",
        }
        manquantes = historiques - sbh
        self.assertEqual(manquantes, set(),
                         f"Plages SBH historiques disparues : {manquantes}")
        self.assertGreaterEqual(len(sbh), 10)


class TestRegressionKnownScores(unittest.TestCase):
    """Tests de regression: les scores connus ne doivent pas changer."""

    def test_gaussian_at_sigma(self):
        """A distance = sigma, la gaussienne vaut exp(-0.5) ~ 0.6065."""
        sigma = 10.0
        # Creer une position a exactement sigma km de la plage
        # On approxime: 1 degre de lat ~ 111.2 km
        delta_lat = sigma / 111.2
        positions = [[-60.86, 14.39 + delta_lat]]
        result = beaches._score_beach(positions, 14.39, -60.86, 50.0, 1.0)
        # local_score = exp(-0.5 * (sigma/sigma_local)^2) * ratio
        # sigma_local (radius) = 50 ici, dist ~ 10 km
        # local_score ~ exp(-0.5 * (10/50)^2) = exp(-0.02) ~ 0.98
        self.assertGreater(result["local_score"], 0.95)
        self.assertLess(result["local_score"], 1.01)

    def test_risk_thresholds_unchanged(self):
        """Les seuils de risque sont fixes: low=5, medium=25, high=75."""
        self.assertEqual(beaches.RISK_THRESHOLDS["low"], 5.0)
        self.assertEqual(beaches.RISK_THRESHOLDS["medium"], 25.0)
        self.assertEqual(beaches.RISK_THRESHOLDS["high"], 75.0)

    def test_regional_sigma_unchanged(self):
        """Le sigma regional est fixe a 50 km."""
        self.assertEqual(beaches.REGIONAL_SIGMA, 50.0)


if __name__ == "__main__":
    # Affichage verbose par defaut
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
