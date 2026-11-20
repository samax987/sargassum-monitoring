"""Tests du semis daté (sargassum_collector, réglage SARGA_SEED_DATED, éteint par défaut).

Aucun appel réseau : les images, les forçages et les téléchargements sont fabriqués.
La dérive de rattrapage est essayée avec le vrai OpenDrift sur un courant uniforme
dont le résultat se calcule à la main.
"""
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

import sargassum_collector as sc

MAINTENANT = datetime(2026, 6, 20, 12, 7, 0)


# ── Réglage ──────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("valeur,attendu", [(None, False), ("", False), ("0", False), (" 0 ", False), ("1", True)])
def test_reglage_lu(monkeypatch, valeur, attendu):
    monkeypatch.delenv("SARGA_SEED_DATED", raising=False)
    if valeur is not None:
        monkeypatch.setenv("SARGA_SEED_DATED", valeur)
    assert sc._lire_semis_date() is attendu


@pytest.mark.parametrize("valeur", ["oui", "true", "2", "-1", "1.0"])
def test_reglage_mal_ecrit_refuse(monkeypatch, valeur):
    monkeypatch.setenv("SARGA_SEED_DATED", valeur)
    with pytest.raises(ValueError):
        sc._lire_semis_date()


# ── Dater chaque point sur les images 1 jour ─────────────────────────────────
def _images():
    """3 jours, grille 4 lat × 5 lon. Axes écrits avec des décimales « sales » comme ERDDAP."""
    lat = 15.0 + 0.045 * np.arange(4) + 0.0019
    lon = -62.0 + 0.045 * np.arange(5)
    im = np.full((3, 4, 5), -0.001, dtype="float32")
    im[0, 1, 1] = 0.002                       # vu le jour 0 seulement
    im[0, 2, 2] = 0.002; im[2, 2, 2] = 0.002  # vu les jours 0 et 2 : on garde le dernier
    im[1, 3, 4] = 0.002; im[2, 3, 4] = np.nan  # vu le jour 1, nuage le jour 2
    im[2, 0, 0] = 0.0001                      # pile au seuil : pas une détection (strictement supérieur)
    return lat, lon, im


def test_dernier_jour_vu():
    lat, lon, im = _images()
    lons = np.array([lon[1], lon[2], lon[4], lon[0], lon[3]])
    lats = np.array([lat[1], lat[2], lat[3], lat[0], lat[0]]) - 0.0019   # décimales du composite 7 jours
    d = sc._dernier_jour_vu(lons, lats, lat, lon, im)
    assert d.tolist() == [0, 2, 1, -1, -1]


def test_dernier_jour_vu_hors_grille_et_bords():
    lat, lon, im = _images()
    im[:] = 0.002                             # tout est « vu » : seule la position peut faire refuser
    lons = np.array([lon[0] - 1.0, lon[4] + 1.0, lon[2], lon[4] + 0.03, np.nan, lon[4] + 0.02, lon[2] + 0.03])
    lats = np.array([lat[0], lat[0], lat[3] + 1.0, lat[1], lat[1], lat[1], lat[1]])
    d = sc._dernier_jour_vu(lons, lats, lat, lon, im)
    # hors grille ×3 ; à plus d'un demi-pixel au-delà du bord (0,03° pour un pixel de 0,045°) ; NaN ;
    # à moins d'un demi-pixel du bord : accepté ; entre deux pixels : rattaché au plus proche
    assert d.tolist() == [-1, -1, -1, -1, -1, 2, 2]


def test_dernier_jour_vu_axe_decroissant_et_vide():
    lat, lon, im = _images()
    d = sc._dernier_jour_vu(np.array([lon[1]]), np.array([lat[1]]), lat[::-1], lon, im[:, ::-1, :])
    assert d.tolist() == [0]
    assert sc._dernier_jour_vu(np.array([]), np.array([]), lat, lon, im).tolist() == []
    assert sc._dernier_jour_vu(np.array([lon[1]]), np.array([lat[1]]), lat, lon, im[:0]).tolist() == [-1]


# ── Durée de la dérive de rattrapage ─────────────────────────────────────────
def test_heures_de_derive():
    instants = [datetime(2026, 6, d, 12) for d in (15, 16, 17, 18, 19, 20)]
    dernier = np.array([-1, 5, 4, 3, 0])
    h = sc._heures_de_derive(dernier, instants, MAINTENANT)
    # jamais daté : 0 ; image du jour (16 h, dans le futur à 12 h 07) : 0 ;
    # veille 16 h : 20,1 h → 21 ; avant-veille : 44,1 → 45 ; 15 juin : 116,1 → 117
    assert h.tolist() == [0, 0, 21, 45, 117]
    assert all(v % 3 == 0 for v in h)


def test_heures_plafonnees_et_arrondi_bas():
    instants = [datetime(2026, 6, 1, 12), datetime(2026, 6, 20, 12)]
    # 1er juin : très vieux, plafonné à 7 jours ; image du jour vue à 17 h : 1 h → 0 (moins de 1 h 30)
    h = sc._heures_de_derive(np.array([0, 1]), instants, datetime(2026, 6, 20, 17, 0))
    assert h.tolist() == [sc.SEMIS_DATE_MAX_H, 0]
    # Le plafond couvre le pire cas accepté : image du jour J-6 et composite vieux de 72 h
    pire = sc.SEMIS_DATE_IMAGE_MAX_H + 24 * (sc.SEMIS_DATE_JOURS - 1) - (sc.SEMIS_DATE_H_OBS - 12)
    assert sc.SEMIS_DATE_MAX_H >= pire and sc.SEMIS_DATE_MAX_H % sc.SEMIS_DATE_PAS_H == 0
    h = sc._heures_de_derive(np.array([1]), instants, datetime(2026, 6, 20, 17, 40))
    assert h.tolist() == [3]                  # 1 h 40 → 3 h


# ── Position à la fin de la dérive de rattrapage ─────────────────────────────
def test_positions_finales():
    nan = np.nan
    # colonnes : 0 en mer jusqu'au bout | 1 échouée au pas 2 | 2 perdue | 3 lâchée tard, en mer |
    #            4 échouée dès le lâcher | 5 valeur de remplissage (9.97e36) puis en mer
    lon = np.array([[-60.0, -61.0, -62.0, nan, nan, 9.97e36],
                    [-60.1, -61.1, -62.1, nan, -64.0, -65.0],
                    [-60.2, -61.2, nan, -63.0, nan, -65.1],
                    [-60.3, nan, nan, -63.1, nan, -65.2]])
    lat = np.where(np.isfinite(lon) & (np.abs(lon) < 400), 16.0, lon)
    st = np.array([[0, 0, 0, nan, nan, nan],
                   [0, 0, 0, nan, 1, 0],
                   [0, 1, nan, 0, nan, 0],
                   [0, nan, nan, 0, nan, 0]], dtype=float)
    lon0 = np.array([-60.0, -61.0, -62.0, -63.0, -64.5, -65.0]); lat0 = np.full(6, 17.0)
    lo, la, et = sc._positions_finales(lon, lat, st, 1.0, lon0, lat0)
    assert et.tolist() == [0, 1, 2, 0, 1, 0]
    assert lo[0] == -60.3 and lo[3] == -63.1 and lo[5] == -65.2
    assert lo[1] == -61.1            # dernière position EN MER, pas le point d'échouage (-61.2)
    assert np.isnan(lo[2]) and np.isnan(la[2])
    assert lo[4] == -64.5 and la[4] == 17.0   # jamais vue en mer : rendue à son point de départ


def test_positions_finales_codes_non_standard():
    # Si OpenDrift numérotait autrement (actif = 5, échoué = 7), on suit ses codes, pas 0 et 1.
    lon = np.array([[-60.0, -61.0], [-60.1, -61.1]]); lat = np.full((2, 2), 16.0)
    st = np.array([[5.0, 5.0], [5.0, 7.0]])
    lo, la, et = sc._positions_finales(lon, lat, st, 7.0, lon[0], lat[0], code_actif=5.0)
    assert et.tolist() == [0, 1] and lo.tolist() == [-60.1, -61.0]


# ── Plan de téléchargement du vent passé ─────────────────────────────────────
def test_plan_vent_passe():
    debut = MAINTENANT - timedelta(hours=45)          # 18 juin 15 h 07
    plan = sc._plan_vent_passe(debut, MAINTENANT, MAINTENANT)
    instants = [p[0] for p in plan]
    assert instants[0] == datetime(2026, 6, 18, 15) and instants[0] <= debut
    assert instants[-1] == datetime(2026, 6, 20, 15) and instants[-1] >= MAINTENANT
    assert all((b - a) == timedelta(hours=3) for a, b in zip(instants, instants[1:]))
    dernier_run = datetime(2026, 6, 20, 6)            # publié vers 9 h 30, donc disponible à 12 h 07
    for t, run, fh in plan:
        assert run <= dernier_run and run.hour % 6 == 0 and run + timedelta(hours=fh) == t
        assert fh in (0, 3) or run == dernier_run
    assert plan[-1][1:] == (dernier_run, 9) and plan[-2][1:] == (dernier_run, 6)


def test_plan_vent_run_pas_encore_publie():
    # À 9 h 20 le run de 6 h n'est pas encore publié (3 h 30 de délai) : on reste sur celui de minuit.
    plan = sc._plan_vent_passe(datetime(2026, 6, 20, 0), datetime(2026, 6, 20, 9, 20), datetime(2026, 6, 20, 9, 20))
    assert [(r.hour, fh) for _, r, fh in plan] == [(0, 0), (0, 3), (0, 6), (0, 9), (0, 12)]


def test_vent_passe_trous(monkeypatch, tmp_path):
    """Un pas manquant au milieu est comblé ; un bord manquant ou trop de trous lèvent une erreur."""
    import xarray as xr
    monkeypatch.setattr(sc.time, "sleep", lambda s: None)
    manquants = set()

    def faux_pas(run, fh, tmpdir):
        t = run + timedelta(hours=fh)
        if t in manquants:
            raise IOError("404")
        v = float((t - datetime(2026, 6, 19)).total_seconds() / 3600.0)   # le vent vaut l'heure : interpolation vérifiable
        return xr.Dataset({"x_wind": (("latitude", "longitude"), np.full((2, 2), v)),
                           "y_wind": (("latitude", "longitude"), np.full((2, 2), -v))},
                          coords={"latitude": [10.0, 11.0], "longitude": [-60.0, -59.0]})
    monkeypatch.setattr(sc, "_gfs_un_pas", faux_pas)
    debut, fin = datetime(2026, 6, 19, 0), datetime(2026, 6, 20, 12)      # 13 pas
    manquants.add(datetime(2026, 6, 19, 9))
    n = sc._fetch_gfs_wind_passe(tmp_path / "v.nc", debut, fin, MAINTENANT)
    assert n == 12
    with xr.open_dataset(tmp_path / "v.nc") as ds:
        assert ds.sizes["time"] == 13
        assert np.allclose(ds["x_wind"].sel(time="2026-06-19T09:00").values, 9.0)
        assert np.allclose(ds["y_wind"].sel(time="2026-06-19T09:00").values, -9.0)
        assert ds["x_wind"].attrs["standard_name"] == "x_wind"
        assert not np.isnan(ds["x_wind"].values).any()
    manquants.clear(); manquants.add(debut)
    with pytest.raises(RuntimeError, match="premier ou le dernier"):
        sc._fetch_gfs_wind_passe(tmp_path / "v2.nc", debut, fin, MAINTENANT)
    manquants.clear(); manquants.update(datetime(2026, 6, 19, h) for h in (6, 9, 12))
    with pytest.raises(RuntimeError, match="seulement 10 pas sur 13"):
        sc._fetch_gfs_wind_passe(tmp_path / "v3.nc", debut, fin, MAINTENANT)


# ── Dérive de rattrapage, avec le vrai OpenDrift ─────────────────────────────
def _courant_uniforme(chemin, u: float, v: float):
    """Courant uniforme en plein Atlantique, 6 jours, écrit comme le fichier de la prod."""
    import xarray as xr
    t = np.array([np.datetime64("2026-06-15T00:00") + np.timedelta64(24 * i, "h") for i in range(7)])
    lat = np.arange(10.0, 20.01, 0.25); lon = np.arange(-50.0, -39.99, 0.25)
    f = lambda val: (("time", "latitude", "longitude"), np.full((len(t), len(lat), len(lon)), val, "float32"))
    ds = xr.Dataset({"x_sea_water_velocity": f(u), "y_sea_water_velocity": f(v)},
                    coords={"time": t, "latitude": lat, "longitude": lon})
    ds["x_sea_water_velocity"].attrs = {"units": "m s-1", "standard_name": "x_sea_water_velocity"}
    ds["y_sea_water_velocity"].attrs = {"units": "m s-1", "standard_name": "y_sea_water_velocity"}
    ds["latitude"].attrs = {"units": "degrees_north", "standard_name": "latitude"}
    ds["longitude"].attrs = {"units": "degrees_east", "standard_name": "longitude"}
    ds.to_netcdf(chemin)
    return chemin


def test_derive_de_rattrapage_courant_uniforme(tmp_path):
    f = _courant_uniforme(tmp_path / "courant.nc", 0.5, 0.0)          # 0,5 m/s vers l'est
    # Ordre volontairement mélangé : 24 h, sur place, 48 h, 24 h
    lons = np.array([-45.0, -45.0, -46.0, -44.0]); lats = np.array([15.0, 16.0, 14.0, 13.0])
    heures = np.array([24, 0, 48, 24])
    lo, la, et = sc._deriver_graines(lons, lats, heures, MAINTENANT, [f], tmp_path, vent=False)
    assert et.tolist() == [0, 0, 0, 0]
    assert lo[1] == -45.0 and la[1] == 16.0                            # sur place : intact
    for i in (0, 2, 3):
        km_est = (lo[i] - lons[i]) * 111.32 * np.cos(np.radians(lats[i]))
        assert km_est == pytest.approx(0.5 * 3.6 * heures[i], rel=0.03), (i, km_est)   # 43,2 km par jour
        assert la[i] == pytest.approx(lats[i], abs=0.01)               # chaque particule a gardé SA latitude


def test_derive_sans_point_a_deplacer_ne_lance_rien(tmp_path):
    lons = np.array([-45.0, -46.0]); lats = np.array([15.0, 14.0])
    lo, la, et = sc._deriver_graines(lons, lats, np.array([0, 0]), MAINTENANT,
                                     [tmp_path / "absent.nc"], tmp_path, vent=False)
    assert lo.tolist() == lons.tolist() and la.tolist() == lats.tolist() and et.tolist() == [0, 0]


def test_derive_hors_des_courants_reste_sur_place(tmp_path):
    f = _courant_uniforme(tmp_path / "courant.nc", 0.5, 0.0)
    # Second point hors de la zone couverte par le fichier : OpenDrift lui donne un courant nul,
    # il ne bouge pas et n'est pas perdu (la prod télécharge 3° de marge autour de la zone).
    lo, la, et = sc._deriver_graines(np.array([-45.0, -30.0]), np.array([15.0, 15.0]), np.array([24, 24]),
                                     MAINTENANT, [f], tmp_path, vent=False)
    assert et.tolist() == [0, 0]
    assert lo[1] == pytest.approx(-30.0, abs=1e-6) and lo[0] > -44.7


def test_derive_tout_echoue_avant_la_fin(tmp_path):
    """Courant fort vers la Guadeloupe : tout s'échoue en quelques heures, OpenDrift s'arrête avant l'heure visée.
    Ce n'est pas une panne : chaque particule est rendue à sa dernière position en mer, entre son départ et la côte."""
    import xarray as xr
    t = np.array([np.datetime64("2026-06-15T00:00") + np.timedelta64(24 * i, "h") for i in range(7)])
    lat = np.arange(14.0, 18.01, 0.25); lon = np.arange(-63.0, -58.99, 0.25)
    f = lambda val: (("time", "latitude", "longitude"), np.full((len(t), len(lat), len(lon)), val, "float32"))
    ds = xr.Dataset({"x_sea_water_velocity": f(-1.5), "y_sea_water_velocity": f(0.0)},
                    coords={"time": t, "latitude": lat, "longitude": lon})
    for v in ("x_sea_water_velocity", "y_sea_water_velocity"):
        ds[v].attrs = {"units": "m s-1", "standard_name": v}
    ds.to_netcdf(tmp_path / "vers_la_cote.nc")
    lons = np.array([-60.9, -60.8]); lats = np.array([16.25, 16.30])     # 30 à 40 km au vent de la Grande-Terre
    lo, la, et = sc._deriver_graines(lons, lats, np.array([48, 48]), MAINTENANT,
                                     [tmp_path / "vers_la_cote.nc"], tmp_path, vent=False)
    assert et.tolist() == [1, 1]
    assert np.all(lo <= lons) and np.all(lo > -61.6)                     # entre le départ et l'île, pas au-delà
    assert np.allclose(la, lats, atol=0.05)


def test_derive_arretee_avant_l_heure_avec_des_particules_en_mer(tmp_path, monkeypatch):
    """OpenDrift qui s'arrête 6 h trop tôt alors que des particules dérivent encore : c'est une panne, on refuse."""
    from opendrift.models.oceandrift import OceanDrift
    vrai_run = OceanDrift.run

    def run_ecourte(self, *a, **k):
        k["end_time"] = k["end_time"] - timedelta(hours=6)
        return vrai_run(self, *a, **k)
    monkeypatch.setattr(OceanDrift, "run", run_ecourte)
    f = _courant_uniforme(tmp_path / "courant.nc", 0.5, 0.0)
    with pytest.raises(RuntimeError, match="encore en mer"):
        sc._deriver_graines(np.array([-45.0]), np.array([15.0]), np.array([24]), MAINTENANT, [f], tmp_path, vent=False)


# ── Fenêtre des images : la semaine du composite, rien d'autre ───────────────
def _sept_jours(fin_jour: int, sans=()):
    inst = [datetime(2026, 3, d, 12) for d in range(fin_jour - 6, fin_jour + 1) if d not in sans]
    return inst, np.arange(len(inst), dtype="float32").reshape(-1, 1, 1) * np.ones((1, 2, 2), "float32")


def test_fenetre_du_composite_alignee():
    inst, im = _sept_jours(19)
    i2, im2 = sc._fenetre_du_composite(inst, im, datetime(2026, 3, 19, 12))
    assert i2 == inst and im2.shape[0] == 7


def test_fenetre_ecarte_une_image_trop_vieille():
    # Le 15 manque : ERDDAP rend à sa place une image voisine, ici le 12, hors de la semaine du 13 au 19.
    inst, im = _sept_jours(19, sans=(15,))
    inst = [datetime(2026, 3, 12, 12)] + inst
    im = np.concatenate([np.full((1, 2, 2), 99, "float32"), im])
    i2, im2 = sc._fenetre_du_composite(inst, im, datetime(2026, 3, 19, 12))
    assert [t.day for t in i2] == [13, 14, 16, 17, 18, 19]
    assert 99 not in im2                       # l'image du 12 ne sert plus à dater


def test_fenetre_refuse_si_le_jour_du_composite_manque():
    inst, im = _sept_jours(18)                 # le jeu 1 jour s'arrête au 18, le composite est du 19
    with pytest.raises(RuntimeError, match="jour du composite"):
        sc._fenetre_du_composite(inst, im, datetime(2026, 3, 19, 12))
    inst, im = _sept_jours(20)                 # le jeu 1 jour a un jour d'avance : l'image du 20 est écartée
    i2, _ = sc._fenetre_du_composite(inst, im, datetime(2026, 3, 19, 12))
    assert [t.day for t in i2] == [14, 15, 16, 17, 18, 19]
    with pytest.raises(RuntimeError):
        sc._fenetre_du_composite([], np.zeros((0, 2, 2), "float32"), datetime(2026, 3, 19, 12))


# ── Enchaînement complet, téléchargements remplacés ──────────────────────────
LAT2, LON2 = np.array([16.0, 16.045]), np.array([-60.0, -59.955])


def _fausses_images(monkeypatch, instants, images):
    demandes = []

    def faux(dossier, fin, stride=3):
        demandes.append(fin)
        return LAT2, LON2, instants, images
    monkeypatch.setattr(sc, "_fetch_afai_1j", faux)
    monkeypatch.setattr(sc, "_fetch_gfs_wind_passe", lambda *a, **k: pytest.fail("vent téléchargé pour rien"))
    return demandes


def test_semis_date_composite_trop_vieux(monkeypatch):
    _fausses_images(monkeypatch, [], None)
    with pytest.raises(RuntimeError, match="en panne"):          # 16 juin 12 h → 20 juin 12 h 07 : 96 h
        sc._semis_date(np.array([-60.0]), np.array([16.0]), 3, "u", "p", "2026-06-16T12:00:00Z", MAINTENANT)


@pytest.mark.parametrize("date", [None, "", "inconnue"])
def test_semis_date_date_du_composite_illisible(monkeypatch, date):
    _fausses_images(monkeypatch, [], None)
    with pytest.raises(RuntimeError, match="illisible"):
        sc._semis_date(np.array([-60.0]), np.array([16.0]), 3, "u", "p", date, MAINTENANT)


def test_semis_date_points_introuvables_refuse(monkeypatch):
    # Images de la bonne semaine mais aucun pixel au-dessus du seuil : semer sur place en se disant « daté » serait faux.
    inst = [datetime(2026, 6, d, 12) for d in range(14, 21)]
    _fausses_images(monkeypatch, inst, np.full((7, 2, 2), -0.001, "float32"))
    with pytest.raises(RuntimeError, match="introuvables"):
        sc._semis_date(np.array([-60.0, -59.955]), np.array([16.0, 16.0]), 3, "u", "p", "2026-06-20T12:00:00Z", MAINTENANT)


def test_semis_date_demande_la_semaine_du_composite_et_rien_a_deplacer(monkeypatch):
    # Tous les pixels vus sur l'image du jour (16 h, pas encore passée à 12 h 07) : rien à déplacer, rien à télécharger.
    inst = [datetime(2026, 6, d, 12) for d in range(14, 21)]
    im = np.full((7, 2, 2), -0.001, "float32"); im[6] = 0.002
    demandes = _fausses_images(monkeypatch, inst, im)
    lo, la, bilan = sc._semis_date(np.array([-60.0]), np.array([16.0]), 3, "u", "p", "2026-06-20T12:00:00Z", MAINTENANT)
    assert demandes == [datetime(2026, 6, 20, 12)]               # la date du composite, pas « les dernières »
    assert lo.tolist() == [-60.0] and bilan["n_derives"] == 0 and bilan["n_non_dates"] == 0
    assert bilan["age_composite_h"] == pytest.approx(0.1, abs=0.05) and bilan["n_images_1j"] == 7


def test_semis_date_une_minorite_sans_date_reste_sur_place(monkeypatch):
    # 1 point sur 3 introuvable (sous la limite de 50 %) : il reste sur place, et il est compté.
    inst = [datetime(2026, 6, d, 12) for d in range(14, 21)]
    im = np.full((7, 2, 2), -0.001, "float32"); im[6, 0, :] = 0.002
    _fausses_images(monkeypatch, inst, im)
    lo, la, bilan = sc._semis_date(np.array([-60.0, -59.955, -60.0]), np.array([16.0, 16.0, 16.045]), 3, "u", "p",
                                   "2026-06-20T12:00:00Z", MAINTENANT)
    assert bilan["n_non_dates"] == 1 and bilan["n_points"] == 3 and len(lo) == 3


def test_semis_date_retire_les_perdues_et_compte(monkeypatch):
    """Chaîne complète, téléchargements et dérive remplacés : les particules perdues sortent du semis, le bilan compte tout."""
    import copernicusmarine
    import xarray as xr
    inst = [datetime(2026, 6, d, 12) for d in range(14, 21)]
    im = np.full((7, 2, 2), -0.001, "float32"); im[4] = 0.002           # tout vu le 18 juin : 44,1 h → 45 h
    monkeypatch.setattr(sc, "_fetch_afai_1j", lambda d, fin, stride=3: (LAT2, LON2, inst, im))
    monkeypatch.setattr(sc, "ENABLE_STOKES_DRIFT", False)
    courant = xr.Dataset({"uo": (("time",), [0.0]), "vo": (("time",), [0.0])}, coords={"time": [0]})
    monkeypatch.setattr(copernicusmarine, "open_dataset", lambda **k: courant)
    vent = {}

    def faux_vent(chemin, debut, fin, maintenant):
        vent.update(debut=debut, fin=fin)
        return 17
    monkeypatch.setattr(sc, "_fetch_gfs_wind_passe", faux_vent)
    recu = {}

    def fausse_derive(lons, lats, heures, maintenant, fichiers, dossier, vent):
        recu.update(heures=heures.tolist(), n_fichiers=len(fichiers), vent=vent)
        return (np.array([lons[0] - 0.5, lons[1], np.nan, lons[3]]), np.array([lats[0], lats[1], np.nan, lats[3]]),
                np.array([0, 1, 2, 0]))
    monkeypatch.setattr(sc, "_deriver_graines", fausse_derive)
    lons = np.array([-60.0, -59.955, -60.0, -59.955]); lats = np.array([16.0, 16.0, 16.045, 16.045])
    lo, la, bilan = sc._semis_date(lons, lats, 3, "u", "p", "2026-06-20T12:00:00Z", MAINTENANT)
    assert lo.tolist() == [-60.5, -59.955, -59.955] and la.tolist() == [16.0, 16.0, 16.045]   # la perdue est sortie
    assert not np.isnan(lo).any()
    assert bilan["n_perdus"] == 1 and bilan["n_echoues_avant"] == 1 and bilan["n_derives"] == 4
    assert bilan["age_median_h"] == 45 and bilan["age_max_h"] == 45 and bilan["vent_pas"] == 17 and bilan["stokes"] is False
    assert recu == {"heures": [45, 45, 45, 45], "n_fichiers": 2, "vent": True}
    assert vent["debut"] == MAINTENANT - timedelta(hours=45) and vent["fin"] == MAINTENANT
    import json
    json.dumps(bilan)                                                    # le bilan doit rester écrivable en base


# ── Branchement dans simulate_drift ──────────────────────────────────────────
class _Stop(Exception):
    """Arrête simulate_drift juste après le semis, avant tout téléchargement."""


def _brancher(monkeypatch, points):
    appels = []
    monkeypatch.setenv("COPERNICUS_USERNAME", "u"); monkeypatch.setenv("COPERNICUS_PASSWORD", "p")
    monkeypatch.delenv("SARGA_SEED_COAST_KM", raising=False)
    monkeypatch.setattr(sc, "_fetch_afai_positions", lambda stride=3: (*points, "2026-06-20T12:00:00Z"))

    def faux_semis(lons, lats, stride, u, p, afai_date):
        assert afai_date == "2026-06-20T12:00:00Z"   # la date du composite semé est bien transmise
        appels.append(len(lons))
        return lons + 0.5, lats, {"n_derives": len(lons)}
    monkeypatch.setattr(sc, "_semis_date", faux_semis)
    import copernicusmarine
    monkeypatch.setattr(copernicusmarine, "open_dataset", lambda **k: (_ for _ in ()).throw(_Stop()))
    return appels


def test_eteint_par_defaut(monkeypatch):
    appels = _brancher(monkeypatch, (np.array([-60.0, -61.0]), np.array([16.0, 17.0])))
    monkeypatch.delenv("SARGA_SEED_DATED", raising=False)
    with pytest.raises(_Stop):
        sc.simulate_drift(None)
    assert appels == []                       # sans le réglage, le semis daté n'est jamais appelé


def test_allume(monkeypatch):
    appels = _brancher(monkeypatch, (np.array([-60.0, -61.0]), np.array([16.0, 17.0])))
    monkeypatch.setenv("SARGA_SEED_DATED", "1")
    with pytest.raises(_Stop):
        sc.simulate_drift(None)
    assert appels == [2]


def test_allume_mais_grille_de_repli(monkeypatch):
    appels = _brancher(monkeypatch, (np.array([]), np.array([])))
    monkeypatch.setenv("SARGA_SEED_DATED", "1")
    with pytest.raises(_Stop):
        sc.simulate_drift(None)
    assert appels == []                       # aucun vrai pixel : rien à dater


def test_reglage_mal_ecrit_arrete_la_simulation(monkeypatch):
    _brancher(monkeypatch, (np.array([-60.0]), np.array([16.0])))
    monkeypatch.setenv("SARGA_SEED_DATED", "oui")
    with pytest.raises(ValueError):
        sc.simulate_drift(None)


def test_semis_date_qui_ne_garde_rien_annule(monkeypatch):
    _brancher(monkeypatch, (np.array([-60.0]), np.array([16.0])))
    monkeypatch.setattr(sc, "_semis_date", lambda *a: (np.array([]), np.array([]), {}))
    monkeypatch.setenv("SARGA_SEED_DATED", "1")
    with pytest.raises(RuntimeError, match="aucun point"):
        sc.simulate_drift(None)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-p", "no:cacheprovider"]))
