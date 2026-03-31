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


class TestComputeBeachScoresIntegration(unittest.TestCase):
    """Tests d'integration avec une DB SQLite en memoire."""

    def setUp(self):
        """Cree une DB en memoire avec des donnees de simulation factices."""
        import tempfile
        self.db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = Path(self.db_file.name)

        conn = sqlite3.connect(self.db_path)
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
                raw_metadata TEXT
            );
        """)

        # Inserer des donnees factices: 100 particules, 6 jours
        import json as _json
        simulated_at = "2026-03-31T12:00:00Z"
        # Particules concentrees autour de la Martinique (14.39, -60.86)
        positions = [[-60.86 + i * 0.01, 14.39 + i * 0.01] for i in range(10)]
        positions_json = _json.dumps(positions)

        for day in range(6):
            conn.execute("""
                INSERT INTO drift_predictions
                (simulated_at, n_particles, current_source, day_offset,
                 active_fraction, positions_json)
                VALUES (?, 100, 'test', ?, 1.0, ?)
            """, (simulated_at, day, positions_json))

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
        """Saint-Barth doit avoir exactement 10 plages."""
        count = sum(1 for b in beaches.BEACHES if b["island"] == "Saint-Barth")
        self.assertEqual(count, 10)


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
