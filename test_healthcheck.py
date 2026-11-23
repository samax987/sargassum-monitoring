"""Tests du controle « echec de simulation » du watchdog (sargassum_healthcheck.check_sim_failure)."""
from datetime import datetime, timedelta, timezone

import pytest

import sargassum_healthcheck as hc


def _il_y_a(heures: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=heures)).strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture
def journal(tmp_path, monkeypatch):
    f = tmp_path / "echecs_simulation.log"
    monkeypatch.setattr(hc, "ECHECS_SIM_LOG", f)
    return f


def test_sans_journal_tout_va_bien(journal):
    assert hc.check_sim_failure()[0] is True


def test_echec_recent_declenche(journal):
    journal.write_text(_il_y_a(30) + "\n" + _il_y_a(0.5) + "\n")
    ok, msg = hc.check_sim_failure()
    assert ok is False and "0.5h" in msg


def test_echec_ancien_ne_declenche_plus(journal):
    # Echec a 12h, succes a 18h : au controle de 18h30 l'echec a 6,5 h, c'est retabli
    journal.write_text(_il_y_a(6.5) + "\n")
    assert hc.check_sim_failure()[0] is True


def test_ligne_illisible_ne_plante_pas(journal):
    journal.write_text("n importe quoi\n\n" + _il_y_a(1) + "\n")
    assert hc.check_sim_failure()[0] is False
    journal.write_text("n importe quoi\n")
    assert hc.check_sim_failure()[0] is True


def test_le_controle_est_branche(journal):
    assert any(f is hc.check_sim_failure for _, f, _ in hc.CHECKS)


# ── Controle de la note en parallele (check_note_vent) ──

def _base_note(tmp_path, created_at=None, table=True):
    import sqlite3
    conn = sqlite3.connect(tmp_path / "n.db")
    if table:
        conn.execute("CREATE TABLE note_vent_parallele (created_at TEXT)")
        if created_at:
            conn.execute("INSERT INTO note_vent_parallele VALUES (?)", (created_at,))
    return conn


def test_note_vent_recente_ok(tmp_path):
    # Juste avant le passage du lendemain, puis apres UN passage manque : toujours bon
    assert hc.check_note_vent(_base_note(tmp_path, _il_y_a(23.9)))[0] is True
    assert hc.check_note_vent(_base_note(tmp_path / "..", _il_y_a(47)))[0] is True


def test_note_vent_deux_passages_manques_declenche(tmp_path):
    ok, msg = hc.check_note_vent(_base_note(tmp_path, _il_y_a(51)))
    assert ok is False and "51h" in msg and "sans effet" in msg


def test_note_vent_table_absente_ou_vide_declenche(tmp_path):
    assert hc.check_note_vent(_base_note(tmp_path, table=False))[0] is False
    (tmp_path / "v").mkdir()
    assert hc.check_note_vent(_base_note(tmp_path / "v"))[0] is False


def test_note_vent_est_branchee():
    assert any(f is hc.check_note_vent for _, f, _ in hc.CHECKS)
