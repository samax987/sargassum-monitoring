"""Tests du masque côtier des points de départ (sargassum_collector._distance_cote_km).

Le masque repose sur le vrai trait de côte GSHHG : on vérifie des points dont
la distance à la terre est connue, et que le réglage par défaut ne filtre rien.
"""
import unittest

import numpy as np

import sargassum_collector as sc


class TestDistanceCote(unittest.TestCase):
    def test_large_et_cote(self):
        # Plein Atlantique à ~300 km à l'est de la Guadeloupe, puis 3 km devant Flamands
        d = sc._distance_cote_km(np.array([-58.0, -62.86]), np.array([16.0, 17.95]))
        self.assertGreater(d[0], 45.0)
        self.assertLess(d[1], 6.0)

    def test_point_sur_terre(self):
        # Centre de la Basse-Terre (Guadeloupe) : distance nulle
        d = sc._distance_cote_km(np.array([-61.70, -58.0]), np.array([16.15, 16.0]))
        self.assertEqual(d[0], 0.0)

    def test_distance_croissante(self):
        # En s'éloignant vers l'est depuis la côte au vent de la Martinique
        lons = np.array([-60.75, -60.60, -60.40, -60.10])
        d = sc._distance_cote_km(lons, np.full(4, 14.70))
        self.assertTrue(np.all(np.diff(d) > 0), d)
        self.assertAlmostEqual(d[3] - d[2], 0.30 * 111 * np.cos(np.radians(14.7)), delta=6.0)

    def test_sans_terre_dans_la_zone(self):
        # Milieu d'océan, aucune terre dans la grille : distance infinie, rien n'est écarté
        d = sc._distance_cote_km(np.array([-45.0, -45.2]), np.array([20.0, 20.1]))
        self.assertTrue(np.all(np.isinf(d)))

    def test_meme_taille_que_l_entree(self):
        lons = np.linspace(-63.5, -60.0, 50)
        d = sc._distance_cote_km(lons, np.full(50, 17.0))
        self.assertEqual(d.shape, (50,))
        self.assertTrue(np.all(d >= 0))



# ── Ajouts du 2026-11-11 après relecture adverse ─────────────────────────────
# Les 5 tests ci-dessus passaient encore avec des distances fausses de 20 %
# ou sans le cosinus de latitude. Les tests suivants posent une côte FICTIVE
# (toute la terre à l'ouest d'un méridien) dont la distance exacte est connue.

import pytest


class _TerreALOuest:
    """Faux RoaringLandmask : terre = longitudes < lon_cote."""
    lon_cote = -60.0

    @classmethod
    def new(cls):
        return cls()

    def contains_many(self, lons, lats):
        return np.asarray(lons) < self.lon_cote


@pytest.fixture
def cote_fictive(monkeypatch):
    import roaring_landmask
    monkeypatch.setattr(roaring_landmask, "RoaringLandmask", _TerreALOuest)


def _km_exacts(lon, lat):
    """Distance exacte au méridien -60° (km)."""
    return (lon - _TerreALOuest.lon_cote) * 111.0 * np.cos(np.radians(lat))


@pytest.mark.parametrize("lat", [10.0, 18.0, 27.0])
def test_distance_exacte_a_une_cellule_pres(cote_fictive, lat):
    # À 27°N, oublier le cosinus fausse la distance de 12 % : doit échouer ici.
    lons = np.array([-59.80, -59.70, -59.55])
    d = sc._distance_cote_km(lons, np.full(3, lat))
    attendu = _km_exacts(lons, lat)
    assert np.all(np.abs(d - attendu) <= 0.04 * attendu + 0.9), (d, attendu)


def test_masque_garde_le_bon_cote_du_seuil(cote_fictive):
    lat = 18.0
    lons = np.array([-60.0 + 12.0 / (111.0 * np.cos(np.radians(lat))),    # ~12 km : écarté
                     -60.0 + 18.0 / (111.0 * np.cos(np.radians(lat)))])   # ~18 km : gardé
    garder = sc._masque_cotier(lons, np.full(2, lat), 15.0)
    assert garder.tolist() == [False, True]


def test_masque_eteint_garde_tout(cote_fictive):
    lons = np.array([-59.99, -59.0])
    assert sc._masque_cotier(lons, np.full(2, 15.0), 0.0).tolist() == [True, True]


def test_masque_ecarte_les_coordonnees_invalides_sans_planter(cote_fictive):
    garder = sc._masque_cotier(np.array([-59.0, np.nan]), np.array([15.0, 15.0]), 15.0)
    assert garder.tolist() == [True, False]


def test_masque_tableau_vide():
    assert sc._masque_cotier(np.array([]), np.array([]), 15.0).shape == (0,)


@pytest.mark.parametrize("valeur, attendu", [(None, 0.0), ("", 0.0), ("0", 0.0),
                                             ("15", 15.0), (" 8.5 ", 8.5)])
def test_lecture_de_la_variable(monkeypatch, valeur, attendu):
    if valeur is None:
        monkeypatch.delenv("SARGA_SEED_COAST_KM", raising=False)
    else:
        monkeypatch.setenv("SARGA_SEED_COAST_KM", valeur)
    assert sc._lire_masque_km() == attendu


@pytest.mark.parametrize("valeur", ["15km", "quinze", "-5", "nan", "inf"])
def test_variable_illisible_fait_echouer(monkeypatch, valeur):
    # Échouer est voulu : une simulation polluée en silence est pire qu'un échec signalé.
    monkeypatch.setenv("SARGA_SEED_COAST_KM", valeur)
    with pytest.raises(ValueError):
        sc._lire_masque_km()


if __name__ == "__main__":
    # pytest pour lancer aussi les tests écrits en style pytest (côte fictive)
    import sys
    sys.exit(pytest.main([__file__, "-q"]))
