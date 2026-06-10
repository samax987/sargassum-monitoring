#!/usr/bin/env python3
"""
test_contributors.py
====================
Tests unitaires de la couche DB du portail contributeurs (contributors_db.py).

Chaque test travaille sur une base SQLite TEMPORAIRE (jamais la base de prod) :
toutes les fonctions de contributors_db acceptent un parametre db_path explicite.

Couvre le coeur de la garantie de moderation : un signalement n'atteint
beach_observations (lue par la calibration) qu'apres approbation explicite.

Lancement : venv/bin/python3 -m pytest test_contributors.py -v
"""

import sqlite3
import tempfile
import unittest
from pathlib import Path

import contributors_db as cdb


class ContributorsDBTestCase(unittest.TestCase):
    """Base commune : DB temporaire avec les tables portail + beach_observations."""

    def setUp(self):
        self.db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = Path(self.db_file.name)
        # Tables du portail (idempotent)
        cdb.init_db(self.db_path)
        # Table cible de la promotion (schema identique a la prod)
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS beach_observations (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at   TEXT NOT NULL,
                island        TEXT NOT NULL,
                beach_name    TEXT NOT NULL,
                observed_risk TEXT NOT NULL,
                coverage_pct  INTEGER,
                notes         TEXT,
                source        TEXT DEFAULT 'terrain'
            )
        """)
        conn.commit()
        conn.close()

    def tearDown(self):
        import os
        os.unlink(self.db_path)

    # Helpers
    def _new_contributor(self, username="alice", status=None) -> int:
        cid = cdb.create_contributor(username, "Alice Test", "hash-factice",
                                     db_path=self.db_path)
        if status and status != cdb.ACCOUNT_PENDING:
            cdb.set_account_status(cid, status, db_path=self.db_path)
        return cid

    def _new_observation(self, cid: int, **kwargs) -> int:
        defaults = dict(
            contributor_id=cid,
            observed_at="2026-06-10T14:00:00",
            island="Saint-Barth",
            beach_name="Grande_Saline",
            observed_risk="high",
            coverage_pct=80,
            notes="test",
            client_ip="203.0.113.1",
            db_path=self.db_path,
        )
        defaults.update(kwargs)
        return cdb.add_observation(**defaults)


class TestComptes(ContributorsDBTestCase):
    """Cycle de vie des comptes benevoles."""

    def test_creation_en_pending(self):
        cid = self._new_contributor()
        c = cdb.get_by_id(cid, db_path=self.db_path)
        self.assertEqual(c["status"], cdb.ACCOUNT_PENDING)
        self.assertEqual(c["obs_count"], 0)
        self.assertIsNone(c["approved_at"])

    def test_username_unique(self):
        self._new_contributor("bob")
        doublon = cdb.create_contributor("bob", "Bob 2", "autre-hash",
                                         db_path=self.db_path)
        self.assertIsNone(doublon, "le doublon de username doit etre refuse")

    def test_activation_renseigne_approved_at(self):
        cid = self._new_contributor()
        ok = cdb.set_account_status(cid, cdb.ACCOUNT_ACTIVE, db_path=self.db_path)
        self.assertTrue(ok)
        c = cdb.get_by_id(cid, db_path=self.db_path)
        self.assertEqual(c["status"], cdb.ACCOUNT_ACTIVE)
        self.assertIsNotNone(c["approved_at"])

    def test_statut_invalide_rejete(self):
        cid = self._new_contributor()
        with self.assertRaises(ValueError):
            cdb.set_account_status(cid, "superadmin", db_path=self.db_path)

    def test_bannissement(self):
        cid = self._new_contributor(status=cdb.ACCOUNT_ACTIVE)
        cdb.set_account_status(cid, cdb.ACCOUNT_BANNED, db_path=self.db_path)
        c = cdb.get_by_id(cid, db_path=self.db_path)
        self.assertEqual(c["status"], cdb.ACCOUNT_BANNED)

    def test_liste_des_comptes_en_attente(self):
        self._new_contributor("a1")
        self._new_contributor("a2", status=cdb.ACCOUNT_ACTIVE)
        pending = cdb.list_pending_accounts(db_path=self.db_path)
        self.assertEqual([p["username"] for p in pending], ["a1"])


class TestModeration(ContributorsDBTestCase):
    """La garantie centrale : rien n'atteint la calibration sans approbation."""

    def test_signalement_cree_en_pending_et_hors_calibration(self):
        cid = self._new_contributor(status=cdb.ACCOUNT_ACTIVE)
        self._new_observation(cid)
        obs = cdb.list_pending_observations(db_path=self.db_path)
        self.assertEqual(len(obs), 1)
        self.assertEqual(obs[0]["status"], "pending")
        # Surtout : beach_observations doit rester VIDE avant approbation
        conn = sqlite3.connect(self.db_path)
        n = conn.execute("SELECT COUNT(*) FROM beach_observations").fetchone()[0]
        conn.close()
        self.assertEqual(n, 0, "un signalement non modere ne doit JAMAIS "
                               "atteindre beach_observations")

    def test_approbation_promeut_vers_beach_observations(self):
        cid = self._new_contributor(status=cdb.ACCOUNT_ACTIVE)
        oid = self._new_observation(cid)
        promoted_id = cdb.approve_observation(oid, db_path=self.db_path)
        self.assertIsNotNone(promoted_id)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM beach_observations WHERE id = ?",
                           (promoted_id,)).fetchone()
        conn.close()
        self.assertEqual(row["source"], "contributor")
        self.assertEqual(row["island"], "Saint-Barth")
        self.assertEqual(row["beach_name"], "Grande_Saline")
        self.assertEqual(row["observed_risk"], "high")
        self.assertEqual(row["coverage_pct"], 80)

    def test_approbation_met_a_jour_staging_et_compteur(self):
        cid = self._new_contributor(status=cdb.ACCOUNT_ACTIVE)
        oid = self._new_observation(cid)
        promoted_id = cdb.approve_observation(oid, db_path=self.db_path)
        obs = cdb.get_observation(oid, db_path=self.db_path)
        self.assertEqual(obs["status"], "approved")
        self.assertEqual(obs["promoted_obs_id"], promoted_id)
        self.assertIsNotNone(obs["reviewed_at"])
        c = cdb.get_by_id(cid, db_path=self.db_path)
        self.assertEqual(c["obs_count"], 1)

    def test_double_approbation_impossible(self):
        cid = self._new_contributor(status=cdb.ACCOUNT_ACTIVE)
        oid = self._new_observation(cid)
        first = cdb.approve_observation(oid, db_path=self.db_path)
        second = cdb.approve_observation(oid, db_path=self.db_path)
        self.assertIsNotNone(first)
        self.assertIsNone(second, "re-approuver ne doit pas dupliquer la promotion")
        conn = sqlite3.connect(self.db_path)
        n = conn.execute("SELECT COUNT(*) FROM beach_observations").fetchone()[0]
        conn.close()
        self.assertEqual(n, 1)

    def test_rejet_n_atteint_jamais_la_calibration(self):
        cid = self._new_contributor(status=cdb.ACCOUNT_ACTIVE)
        oid = self._new_observation(cid)
        ok = cdb.reject_observation(oid, db_path=self.db_path)
        self.assertTrue(ok)
        obs = cdb.get_observation(oid, db_path=self.db_path)
        self.assertEqual(obs["status"], "rejected")
        self.assertIsNone(obs["promoted_obs_id"])
        conn = sqlite3.connect(self.db_path)
        n = conn.execute("SELECT COUNT(*) FROM beach_observations").fetchone()[0]
        conn.close()
        self.assertEqual(n, 0)
        # Et le compteur du contributeur ne bouge pas
        c = cdb.get_by_id(cid, db_path=self.db_path)
        self.assertEqual(c["obs_count"], 0)

    def test_rejet_d_un_signalement_deja_traite_refuse(self):
        cid = self._new_contributor(status=cdb.ACCOUNT_ACTIVE)
        oid = self._new_observation(cid)
        cdb.approve_observation(oid, db_path=self.db_path)
        self.assertFalse(cdb.reject_observation(oid, db_path=self.db_path))


class TestPhotosEtListes(ContributorsDBTestCase):
    """Photo jointe et vues listees."""

    def test_photo_path_persiste(self):
        cid = self._new_contributor(status=cdb.ACCOUNT_ACTIVE)
        oid = self._new_observation(cid, photo_path="contrib_photos/abcd1234.jpg")
        obs = cdb.get_observation(oid, db_path=self.db_path)
        self.assertEqual(obs["photo_path"], "contrib_photos/abcd1234.jpg")

    def test_observation_sans_photo(self):
        cid = self._new_contributor(status=cdb.ACCOUNT_ACTIVE)
        oid = self._new_observation(cid)  # pas de photo_path
        obs = cdb.get_observation(oid, db_path=self.db_path)
        self.assertIsNone(obs["photo_path"])

    def test_liste_pending_jointe_au_contributeur(self):
        cid = self._new_contributor("carole", status=cdb.ACCOUNT_ACTIVE)
        self._new_observation(cid)
        obs = cdb.list_pending_observations(db_path=self.db_path)
        self.assertEqual(obs[0]["username"], "carole")
        self.assertEqual(obs[0]["display_name"], "Alice Test")

    def test_historique_par_contributeur(self):
        cid = self._new_contributor(status=cdb.ACCOUNT_ACTIVE)
        autre = self._new_contributor("zoe", status=cdb.ACCOUNT_ACTIVE)
        self._new_observation(cid, beach_name="Lorient")
        self._new_observation(autre, beach_name="Toiny")
        miens = cdb.list_observations_for(cid, db_path=self.db_path)
        self.assertEqual(len(miens), 1)
        self.assertEqual(miens[0]["beach_name"], "Lorient")

    def test_init_db_idempotent(self):
        # Une 2e initialisation ne doit ni planter ni detruire les donnees
        cid = self._new_contributor()
        cdb.init_db(self.db_path)
        self.assertIsNotNone(cdb.get_by_id(cid, db_path=self.db_path))


if __name__ == "__main__":
    unittest.main()
