"""Tests de la vérité webcam (sarga_webcam_truth.py) et de son branchement
dans sarga_verification.py. Images de synthèse : aucune dépendance aux captures."""

import sqlite3

import numpy as np
import pytest
from PIL import Image

import sarga_verification as verif
import sarga_webcam_truth as wt

MER = (40, 120, 170)       # bleu
SABLE = (230, 215, 190)    # clair, jaune pâle
BRUN = (150, 110, 50)      # sargasse


def image(tmp_path, nom: str, radeau: int = 0, sable: bool = True):
    """Plage de synthèse : mer en haut, sable à partir de y=400, radeau carré optionnel."""
    a = np.zeros((720, 1280, 3), dtype=np.uint8)
    a[:] = MER
    if sable:
        a[400:] = SABLE
    if radeau:
        a[250:250 + radeau, 500:500 + radeau] = BRUN
    chemin = tmp_path / nom
    Image.fromarray(a).save(chemin, quality=95)
    return str(chemin)


# ── Lecture d'une image ──────────────────────────────────────────────────────

def test_mer_propre_donne_rien(tmp_path):
    m = wt.analyse(image(tmp_path, "propre.jpg"))
    assert m["cols"] >= wt.SEUIL_COLS
    assert m["large_px"] == 0
    assert wt.verdict(m) == "rien"


def test_gros_radeau_donne_radeaux(tmp_path):
    m = wt.analyse(image(tmp_path, "radeau.jpg", radeau=25))   # 625 px bruns au large
    assert m["large_px"] >= wt.SEUIL_RADEAUX
    assert wt.verdict(m) == "radeaux"


def test_image_sans_plage_est_illisible(tmp_path):
    # Pas de sable visible (brume, nuit, caméra déplacée) : on ne conclut rien
    m = wt.analyse(image(tmp_path, "brume.jpg", radeau=25, sable=False))
    assert wt.verdict(m) == "illisible"


@pytest.mark.parametrize("large, cols, attendu", [
    (0, 1.0, "rien"), (99, 0.90, "rien"), (100, 0.90, "douteux"), (299, 0.90, "douteux"),
    (300, 0.90, "radeaux"), (5000, 0.84, "illisible"), (0, 0.84, "illisible"),
])
def test_seuils_du_verdict(large, cols, attendu):
    assert wt.verdict({"large_px": large, "cote_px": 0, "cols": cols}) == attendu


# ── Base ─────────────────────────────────────────────────────────────────────

@pytest.fixture
def base(tmp_path):
    conn = sqlite3.connect(tmp_path / "test.db")
    conn.executescript(wt.SCHEMA)
    conn.executescript("""
        CREATE TABLE webcam_captures (id INTEGER PRIMARY KEY, captured_at TEXT, island TEXT,
            camera_name TEXT, camera_key TEXT, file_path TEXT, success INTEGER);
        CREATE TABLE beach_observations (id INTEGER PRIMARY KEY, observed_at TEXT, island TEXT,
            beach_name TEXT, observed_risk TEXT, source TEXT);
    """)
    return conn


def capture(conn, jour: str, chemin: str, heure: str = "14:00:04", ok: int = 1):
    conn.execute("INSERT INTO webcam_captures (captured_at, island, camera_name, file_path, success) "
                 "VALUES (?,?,?,?,?)", (f"{jour}T{heure}", "Saint-Barth", "Flamand_Beach", chemin, ok))


def verdicts(conn) -> dict:
    return dict(conn.execute("SELECT capture_date, verdict FROM webcam_truth"))


def test_traiter_range_un_verdict_par_jour(base, tmp_path):
    capture(base, "2026-11-01", image(tmp_path, "a.jpg"))
    capture(base, "2026-11-02", image(tmp_path, "b.jpg", radeau=25))
    capture(base, "2026-11-03", image(tmp_path, "rate.jpg"), ok=0)      # capture ratée : ignorée
    bilan = wt.traiter(base, "Flamand_Beach")
    assert verdicts(base) == {"2026-11-01": "rien", "2026-11-02": "radeaux"}
    assert bilan == {"rien": 1, "radeaux": 1}
    ligne = base.execute("SELECT island, beach_name, detector_version FROM webcam_truth LIMIT 1").fetchone()
    assert ligne == ("Saint-Barth", "Flamands", wt.DETECTOR_VERSION)


def test_relancer_ne_cree_pas_de_doublon(base, tmp_path):
    capture(base, "2026-11-01", image(tmp_path, "a.jpg"))
    wt.traiter(base, "Flamand_Beach")
    wt.traiter(base, "Flamand_Beach")
    assert base.execute("SELECT COUNT(*) FROM webcam_truth").fetchone()[0] == 1


def test_webcam_figee_est_illisible(base, tmp_path):
    # Même fichier deux jours de suite = la webcam n'a pas rafraîchi son image
    chemin = image(tmp_path, "a.jpg", radeau=25)
    copie = tmp_path / "b.jpg"
    copie.write_bytes((tmp_path / "a.jpg").read_bytes())
    capture(base, "2026-11-01", chemin)
    capture(base, "2026-11-02", str(copie))
    wt.traiter(base, "Flamand_Beach")
    assert verdicts(base) == {"2026-11-01": "radeaux", "2026-11-02": "illisible"}


def test_fichier_absent_ne_plante_pas(base, tmp_path):
    capture(base, "2026-11-01", str(tmp_path / "nexistepas.jpg"))
    bilan = wt.traiter(base, "Flamand_Beach")
    assert bilan == {"fichier absent": 1}
    assert verdicts(base) == {}


def test_un_seul_jour_demande(base, tmp_path):
    capture(base, "2026-11-01", image(tmp_path, "a.jpg"))
    capture(base, "2026-11-02", image(tmp_path, "b.jpg", radeau=25))
    wt.traiter(base, "Flamand_Beach", seulement="2026-11-02")
    assert verdicts(base) == {"2026-11-02": "radeaux"}


def test_regle_d_or_jamais_dans_beach_observations(base, tmp_path):
    # Une lecture automatique n'est pas un signalement humain validé
    capture(base, "2026-11-01", image(tmp_path, "a.jpg", radeau=25))
    wt.traiter(base, "Flamand_Beach")
    assert base.execute("SELECT COUNT(*) FROM beach_observations").fetchone()[0] == 0


# ── Branchement dans la vérification ─────────────────────────────────────────

def test_juge_webcam_dans_la_verification(base, capsys):
    base.executescript(verif.SCHEMA)
    base.execute("""CREATE TABLE forecast_archive (computed_at TEXT, beach_name TEXT, target_date TEXT,
        day_offset INTEGER, presence TEXT, risk_level TEXT, local_score REAL, regional_score REAL,
        n_stranded INTEGER, presence_raw TEXT)""")
    # 4 jours : radeaux annoncés et vus, rien annoncé et rien vu, fausse alerte, arrivée manquée
    jours = [("2026-11-10", "radeaux", "medium"), ("2026-11-11", "rien", "none"),
             ("2026-11-12", "rien", "low"), ("2026-11-13", "radeaux", "none"),
             ("2026-11-14", "douteux", "high"), ("2026-11-15", "illisible", "high")]
    for jour, vu, prevu in jours:
        base.execute("INSERT INTO webcam_truth (capture_date, island, beach_name, camera_name, verdict, "
                     "detector_version, created_at) VALUES (?,?,?,?,?,?,?)",
                     (jour, "Saint-Barth", "Flamands", "Flamand_Beach", vu, "test", "x"))
        base.execute("INSERT INTO forecast_archive VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (f"{jour}T06:00:00Z", "Flamands", jour, 0, prevu, "low", 1.0, 1.0, 0, prevu))
    n = verif.run_webcam(base, "2026-11-16T09:10:00Z")
    assert n == 4                                   # douteux et illisible ne jugent rien
    j0 = __import__("json").loads(base.execute(
        "SELECT metrics_json FROM verification_webcam_summary WHERE day_offset = 0").fetchone()[0])
    assert (j0["local"]["hits"], j0["local"]["miss"], j0["local"]["false_alarms"],
            j0["local"]["correct_neg"]) == (1, 1, 1, 1)
    assert j0["local"]["pss"] == 0                  # pile le hasard sur ce jeu fabriqué
    assert j0["persistence"]["n"] == 3              # le premier jour n'a pas de veille
    assert "SECOND JUGE" in capsys.readouterr().out
    # Le juge webcam ne touche pas aux tables du juge terrain
    assert base.execute("SELECT COUNT(*) FROM verification_summary").fetchone()[0] == 0


def test_juge_webcam_dry_run_n_ecrit_rien(base):
    base.executescript(verif.SCHEMA)
    base.execute("""CREATE TABLE forecast_archive (computed_at TEXT, beach_name TEXT, target_date TEXT,
        day_offset INTEGER, presence TEXT, risk_level TEXT, local_score REAL, regional_score REAL,
        n_stranded INTEGER, presence_raw TEXT)""")
    base.execute("INSERT INTO webcam_truth (capture_date, island, beach_name, camera_name, verdict, "
                 "detector_version, created_at) VALUES ('2026-11-10','Saint-Barth','Flamands','Flamand_Beach','rien','t','x')")
    base.execute("INSERT INTO forecast_archive VALUES ('2026-11-10T06:00:00Z','Flamands','2026-11-10',0,'none','low',0,0,0,'none')")
    assert verif.run_webcam(base, "x", dry_run=True) == 1
    assert base.execute("SELECT COUNT(*) FROM verification_webcam_summary").fetchone()[0] == 0


def test_sans_table_webcam_la_verification_continue(tmp_path):
    conn = sqlite3.connect(tmp_path / "vide.db")
    conn.executescript(verif.SCHEMA)
    assert verif.run_webcam(conn, "x") == 0


# ── Un seul régime de modèle à la fois (2026-11-11) ──────────────────────────

def _archive_avec_regime(base):
    base.executescript(verif.SCHEMA)
    base.execute("""CREATE TABLE forecast_archive (computed_at TEXT, beach_name TEXT, target_date TEXT,
        day_offset INTEGER, presence TEXT, risk_level TEXT, local_score REAL, regional_score REAL,
        n_stranded INTEGER, presence_raw TEXT, seed_coast_km REAL)""")


def test_la_derniere_prevision_d_un_autre_regime_est_ignoree(base):
    _archive_avec_regime(base)
    # Même plage, même date visée, même échéance : l'ancienne prévision (sans masque)
    # est la SEULE pour cette date. Sans filtre, elle serait jugée.
    base.execute("INSERT INTO forecast_archive VALUES ('2026-11-09T18:00:00Z','Flamands','2026-11-12',3,"
                 "'none','low',0,9,0,'none',0)")
    base.execute("INSERT INTO forecast_archive VALUES ('2026-11-10T12:00:00Z','Flamands','2026-11-13',3,"
                 "'none','none',0,1,0,'none',15)")
    assert verif.regime_courant(base) == 15
    assert verif.last_forecast(base, "Flamands", "2026-11-12", 3, 15) is None
    assert verif.last_forecast(base, "Flamands", "2026-11-12", 3, 0)[2] == "low"
    assert verif.last_forecast(base, "Flamands", "2026-11-12", 3)[2] == "low"   # sans filtre : comme avant


def test_juge_webcam_ne_melange_pas_les_regimes(base, capsys):
    _archive_avec_regime(base)
    for jour in ("2026-11-12", "2026-11-13"):
        base.execute("INSERT INTO webcam_truth (capture_date, island, beach_name, camera_name, verdict, "
                     "detector_version, created_at) VALUES (?,?,?,?,?,?,?)",
                     (jour, "Saint-Barth", "Flamands", "Flamand_Beach", "rien", "t", "x"))
    base.execute("INSERT INTO forecast_archive VALUES ('2026-11-09T18:00:00Z','Flamands','2026-11-12',3,"
                 "'none','low',0,9,0,'none',0)")
    base.execute("INSERT INTO forecast_archive VALUES ('2026-11-10T12:00:00Z','Flamands','2026-11-13',3,"
                 "'none','none',0,1,0,'none',15)")
    assert verif.run_webcam(base, "2026-11-16T09:10:00Z") == 1          # seul le régime actuel
    assert "masque côtier 15 km" in capsys.readouterr().out
    assert verif.run_webcam(base, "x", dry_run=True, regime=0) == 1     # l'ancien régime à part
    assert verif.run_webcam(base, "x", dry_run=True, regime=verif.TOUS_REGIMES) == 2


def test_une_prevision_calculee_apres_la_capture_ne_compte_pas(base):
    # Capture à 14h UTC ; prévision j+0 calculée à 18h le même jour : c'est prévoir le passé.
    _archive_avec_regime(base)
    base.execute("INSERT INTO webcam_truth (capture_date, island, beach_name, camera_name, verdict, "
                 "detector_version, created_at) VALUES ('2026-11-10','Saint-Barth','Flamands',"
                 "'Flamand_Beach','rien','t','x')")
    base.execute("INSERT INTO forecast_archive VALUES ('2026-11-10T18:02:21Z','Flamands','2026-11-10',0,"
                 "'none','none',0,1,0,'none',15)")
    assert verif.run_webcam(base, "x", dry_run=True) == 0
    base.execute("INSERT INTO forecast_archive VALUES ('2026-11-10T06:02:00Z','Flamands','2026-11-10',0,"
                 "'none','none',0,1,0,'none',15)")
    assert verif.run_webcam(base, "x", dry_run=True) == 1
    fc = verif.last_forecast(base, "Flamands", "2026-11-10", 0, 15, avant="2026-11-10T14:00:05")
    assert fc[0] == "2026-11-10T06:02:00Z"


def test_un_regime_inconnu_ne_decide_pas_du_regime_courant(base):
    # Une simulation sur grille de repli (NULL) arrive en dernier : le régime reste 15 km.
    _archive_avec_regime(base)
    base.execute("INSERT INTO forecast_archive VALUES ('2026-11-10T12:00:00Z','Flamands','2026-11-11',1,"
                 "'none','none',0,1,0,'none',15)")
    base.execute("INSERT INTO forecast_archive VALUES ('2026-11-10T18:00:00Z','Flamands','2026-11-11',1,"
                 "'none','none',0,1,0,'none',NULL)")
    assert verif.regime_courant(base) == 15
