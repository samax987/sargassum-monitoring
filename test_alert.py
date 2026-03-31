#!/usr/bin/env python3
"""
test_alert.py
=============
Tests unitaires pour sargassum_alert.py

Couverture:
  - build_message()       : format du message Telegram
  - build_clear_message() : message de levee d'alerte
  - RISK_RANK ordering    : coherence du classement
  - Logique anti-spam     : hash / inchangé
  - Fenetre horaire       : logique temporelle

Usage:
  python test_alert.py
  python -m pytest test_alert.py -v
"""

import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

# Forcer les variables d'environnement avant l'import
os.environ["TELEGRAM_TOKEN"] = "0000000000:TEST_TOKEN_FOR_UNIT_TESTS_ONLY"
os.environ["TELEGRAM_CHAT"] = "999999999"

sys.path.insert(0, str(Path(__file__).parent))
import sargassum_alert as alert


class TestRiskOrdering(unittest.TestCase):
    """Verifie que le classement des niveaux de risque est coherent."""

    def test_risk_rank_order(self):
        self.assertLess(alert.RISK_RANK["none"], alert.RISK_RANK["low"])
        self.assertLess(alert.RISK_RANK["low"], alert.RISK_RANK["medium"])
        self.assertLess(alert.RISK_RANK["medium"], alert.RISK_RANK["high"])

    def test_all_levels_have_icon(self):
        for level in ("none", "low", "medium", "high"):
            self.assertIn(level, alert.RISK_ICONS)
            self.assertTrue(len(alert.RISK_ICONS[level]) > 0)

    def test_all_levels_have_fr_label(self):
        for level in ("none", "low", "medium", "high"):
            self.assertIn(level, alert.RISK_FR)


class TestBuildMessage(unittest.TestCase):
    """Tests du format des messages Telegram."""

    def _make_beach(self, island, name, risk, day=0, closest=50.0, score=30.0):
        return {
            "island": island,
            "beach_name": name,
            "risk_level": risk,
            "day_offset": day,
            "closest_km": closest,
            "regional_score": score,
        }

    def test_message_contains_date(self):
        beaches = [self._make_beach("Saint-Barth", "Saint-Jean", "medium")]
        msg = alert.build_message(beaches, "2026-03-31T06:00:00Z")
        self.assertIn("2026-03-31 06:00 UTC", msg)

    def test_priority_island_detailed(self):
        """Saint-Barth doit etre detaille (plage par plage)."""
        beaches = [
            self._make_beach("Saint-Barth", "Saint-Jean", "high"),
            self._make_beach("Saint-Barth", "Flamands", "medium"),
        ]
        msg = alert.build_message(beaches, "2026-03-31T06:00:00Z")
        self.assertIn("Saint-Jean", msg)
        self.assertIn("Flamands", msg)

    def test_other_island_summary(self):
        """Martinique doit etre en resume, pas en detail."""
        beaches = [
            self._make_beach("Martinique", "Les_Salines", "high"),
            self._make_beach("Martinique", "Diamant", "medium"),
        ]
        msg = alert.build_message(beaches, "2026-03-31T06:00:00Z")
        self.assertIn("Martinique", msg)
        # Les noms de plages ne doivent PAS apparaitre pour les autres iles
        self.assertNotIn("Les Salines", msg)
        self.assertNotIn("Diamant", msg)

    def test_below_threshold_not_in_priority_detail(self):
        """Les plages sous seuil ne doivent pas apparaitre dans le detail Saint-Barth."""
        beaches = [
            self._make_beach("Saint-Barth", "Saint-Jean", "low"),  # sous seuil medium
        ]
        msg = alert.build_message(beaches, "2026-03-31T06:00:00Z")
        self.assertIn("toutes plages OK", msg)
        self.assertNotIn("Saint-Jean", msg)

    def test_alert_count_in_message(self):
        """Le message indique le nombre de plages en alerte."""
        beaches = [
            self._make_beach("Saint-Barth", "Saint-Jean", "high"),
            self._make_beach("Saint-Barth", "Flamands", "high"),
            self._make_beach("Martinique", "Diamant", "medium"),
        ]
        msg = alert.build_message(beaches, "2026-03-31T06:00:00Z")
        self.assertIn("3 plages", msg)

    def test_single_beach_alert_singular(self):
        """Singulier quand une seule plage."""
        beaches = [self._make_beach("Saint-Barth", "Saint-Jean", "medium")]
        msg = alert.build_message(beaches, "2026-03-31T06:00:00Z")
        self.assertIn("1 plage", msg)
        self.assertNotIn("1 plages", msg)

    def test_dashboard_link_in_message(self):
        """Le lien dashboard doit etre present."""
        beaches = [self._make_beach("Saint-Barth", "Saint-Jean", "high")]
        msg = alert.build_message(beaches, "2026-03-31T06:00:00Z")
        self.assertIn("8501", msg)

    def test_underscore_to_space_in_beach_name(self):
        """Les underscores dans les noms de plage doivent etre remplaces par des espaces."""
        beaches = [self._make_beach("Saint-Barth", "Grand_Cul-de-Sac", "high")]
        msg = alert.build_message(beaches, "2026-03-31T06:00:00Z")
        self.assertIn("Grand Cul-de-Sac", msg)


class TestBuildClearMessage(unittest.TestCase):
    """Tests du message de levee d'alerte."""

    def test_clear_message_contains_date(self):
        msg = alert.build_clear_message("2026-03-31T06:00:00Z")
        self.assertIn("2026-03-31 06:00 UTC", msg)

    def test_clear_message_no_alert(self):
        msg = alert.build_clear_message("2026-03-31T06:00:00Z")
        self.assertIn("faible", msg.lower())

    def test_clear_message_is_string(self):
        msg = alert.build_clear_message("2026-03-31T06:00:00Z")
        self.assertIsInstance(msg, str)
        self.assertGreater(len(msg), 10)


class TestSendTelegram(unittest.TestCase):
    """Tests de la fonction d'envoi Telegram (avec mock HTTP)."""

    @patch("sargassum_alert.requests.post")
    def test_send_returns_true_on_success(self, mock_post):
        mock_post.return_value = MagicMock(ok=True)
        result = alert.send_telegram("test message")
        self.assertTrue(result)

    @patch("sargassum_alert.requests.post")
    def test_send_returns_false_on_http_error(self, mock_post):
        mock_post.return_value = MagicMock(ok=False)
        result = alert.send_telegram("test message")
        self.assertFalse(result)

    @patch("sargassum_alert.requests.post", side_effect=ConnectionError("timeout"))
    def test_send_returns_false_on_exception(self, mock_post):
        result = alert.send_telegram("test message")
        self.assertFalse(result)

    @patch("sargassum_alert.requests.post")
    def test_send_uses_correct_chat_id(self, mock_post):
        mock_post.return_value = MagicMock(ok=True)
        alert.send_telegram("hello")
        call_kwargs = mock_post.call_args
        data = call_kwargs[1]["data"] if "data" in call_kwargs[1] else call_kwargs[0][1]
        self.assertEqual(str(data["chat_id"]), os.environ["TELEGRAM_CHAT"])


class TestDatabaseOperations(unittest.TestCase):
    """Tests des operations DB (alert_state)."""

    def setUp(self):
        self.db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = Path(self.db_file.name)
        # Override DB_PATH pour les tests
        self._orig_db = alert.DB_PATH
        alert.DB_PATH = self.db_path

    def tearDown(self):
        alert.DB_PATH = self._orig_db
        os.unlink(self.db_path)

    def test_get_conn_creates_table(self):
        conn = alert.get_conn()
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        self.assertIn("alert_state", table_names)
        conn.close()

    def test_last_alert_hash_empty_db(self):
        conn = alert.get_conn()
        result = alert.last_alert_hash(conn)
        self.assertIsNone(result)
        conn.close()

    def test_save_and_retrieve_hash(self):
        conn = alert.get_conn()
        test_hash = "abc123def456"
        alert.save_alert(conn, "2026-03-31T06:00:00Z", test_hash)
        retrieved = alert.last_alert_hash(conn)
        self.assertEqual(retrieved, test_hash)
        conn.close()

    def test_last_hash_returns_most_recent(self):
        """save_alert multiple fois: last_alert_hash retourne le plus recent (ORDER BY id DESC)."""
        conn = alert.get_conn()
        # Inserer deux alertes: la seconde doit etre retournee
        alert.save_alert(conn, "2026-03-31T06:00:00Z", "hash_first")
        alert.save_alert(conn, "2026-03-31T06:00:00Z", "hash_second")
        result = alert.last_alert_hash(conn)
        self.assertEqual(result, "hash_second")
        conn.close()


class TestAntiSpamLogic(unittest.TestCase):
    """Tests de la logique anti-spam (hash de payload)."""

    def test_same_beaches_same_hash(self):
        """Memes plages en alerte -> meme hash -> pas de re-envoi."""
        beaches1 = [
            {"island": "Saint-Barth", "beach_name": "Saint-Jean", "risk_level": "high", "day_offset": 0},
        ]
        beaches2 = [
            {"island": "Saint-Barth", "beach_name": "Saint-Jean", "risk_level": "high", "day_offset": 0},
        ]
        payload1 = json.dumps(
            [(b["island"], b["beach_name"], b["risk_level"], b["day_offset"]) for b in beaches1],
            sort_keys=True,
        )
        payload2 = json.dumps(
            [(b["island"], b["beach_name"], b["risk_level"], b["day_offset"]) for b in beaches2],
            sort_keys=True,
        )
        self.assertEqual(
            hashlib.md5(payload1.encode()).hexdigest(),
            hashlib.md5(payload2.encode()).hexdigest(),
        )

    def test_different_risk_different_hash(self):
        """Changement de niveau de risque -> hash different -> envoi."""
        beaches1 = [
            {"island": "Saint-Barth", "beach_name": "Saint-Jean", "risk_level": "medium", "day_offset": 0},
        ]
        beaches2 = [
            {"island": "Saint-Barth", "beach_name": "Saint-Jean", "risk_level": "high", "day_offset": 0},
        ]
        payload1 = json.dumps(
            [(b["island"], b["beach_name"], b["risk_level"], b["day_offset"]) for b in beaches1],
            sort_keys=True,
        )
        payload2 = json.dumps(
            [(b["island"], b["beach_name"], b["risk_level"], b["day_offset"]) for b in beaches2],
            sort_keys=True,
        )
        self.assertNotEqual(
            hashlib.md5(payload1.encode()).hexdigest(),
            hashlib.md5(payload2.encode()).hexdigest(),
        )

    def test_empty_alert_list_produces_consistent_hash(self):
        """Liste vide -> hash deterministe et constant."""
        payload = json.dumps([], sort_keys=True)
        h1 = hashlib.md5(payload.encode()).hexdigest()
        h2 = hashlib.md5(payload.encode()).hexdigest()
        self.assertEqual(h1, h2)


class TestTimeWindow(unittest.TestCase):
    """Tests de la logique de fenetre horaire."""

    def test_diff_calculation_wraps_at_midnight(self):
        """
        Le calcul de diff doit gerer le wrap a minuit.
        Ex: heure=23, cible=1 -> diff min(22, 2) = 2 (dans la fenetre +/-2h).
        """
        # Reproduction du calcul de main()
        def is_in_window(now_hour, target_hour, tolerance):
            diff = abs(now_hour - target_hour)
            diff = min(diff, 24 - diff)
            return diff <= tolerance

        self.assertTrue(is_in_window(6, 6, 1))    # exactement dans la fenetre
        self.assertTrue(is_in_window(7, 6, 1))    # +1h, dans la fenetre
        self.assertFalse(is_in_window(8, 6, 1))   # +2h, hors fenetre
        self.assertTrue(is_in_window(5, 6, 1))    # -1h, dans la fenetre
        self.assertFalse(is_in_window(4, 6, 1))   # -2h, hors fenetre
        # Test wrap minuit
        self.assertTrue(is_in_window(23, 0, 1))   # 23h avec cible 0h -> diff=1


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
