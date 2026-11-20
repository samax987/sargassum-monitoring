#!/bin/bash
set -e
PYTHON=/opt/sargassum/venv/bin/python3
DIR=/opt/sargassum

# Active Stokes drift CMEMS Wave (gain ~10% précision dernier km)
# Nécessite swap (>=2 GB) sur le VPS, sinon OOM-killer.
export ENABLE_STOKES_DRIFT=1
# Masque côtier des points de départ : écarte ceux à moins de 15 km d une côte,
# qui sont surtout du faux signal satellite (mesuré le 2026-11-10, voir CLAUDE.md).
# Pour revenir en arrière : commenter la ligne suivante.
export SARGA_SEED_COAST_KM=15
# Semis daté des points de départ : ÉTEINT. Chaque pixel serait transporté par les
# courants, le vent et les vagues des jours passés depuis le jour où le satellite
# l a vu jusqu à maintenant (voir CLAUDE.md, témoin du 2026-11-16). Ajoute environ
# 60 téléchargements de vent et une dérive de plus par passage.
# Pour l allumer : retirer le # de la ligne suivante.
# export SARGA_SEED_DATED=1
# cache matplotlib dans un dossier accessible a webapps : sans lui, matplotlib
# recree son cache a chaque run et perd quelques secondes en avertissements.
export MPLCONFIGDIR=/opt/sargassum/.cache/matplotlib

mkdir -p "$DIR/logs"
cd "$DIR"
echo ""
echo "──────────────────────────────────────────────"
echo "▶  $(date -u '+%Y-%m-%dT%H:%M:%SZ')  sargassum_run_linux.sh"
echo "──────────────────────────────────────────────"

echo "[1/4] Collecte des données…"
"$PYTHON" "$DIR/sargassum_collector.py"

echo "[2/4] Simulation de dérive OpenDrift…"
if "$PYTHON" "$DIR/sargassum_collector.py" --simulate; then
    # succès : on efface la trace, sinon le check santé signalerait un échec périmé
    rm -f "$DIR/logs/ECHEC_SIMULATION"
else
    echo "  ⚠️  Simulation OpenDrift échouée — données de dérive non mises à jour"
    # Trace lisible par le check santé quotidien : un échec ici ne doit plus être silencieux.
    date -u '+%Y-%m-%dT%H:%M:%SZ' > "$DIR/logs/ECHEC_SIMULATION"
    # Journal jamais effacé : ECHEC_SIMULATION disparaît au premier succès suivant,
    # donc un échec isolé passait inaperçu. sargassum_healthcheck.py lit ce journal.
    date -u '+%Y-%m-%dT%H:%M:%SZ' >> "$DIR/logs/echecs_simulation.log"
fi

echo "[3/4] Calcul des scores de plage…"
"$PYTHON" "$DIR/beaches.py"

echo "[4a/4] Alerte Telegram groupée (chat principal)…"
"$PYTHON" "$DIR/sargassum_alert.py" || \
    echo "  ⚠️  Alerte Telegram groupée échouée — non bloquant"

echo "[4b/4] Alertes Telegram personnalisées (abonnés)…"
"$PYTHON" "$DIR/sargassum_alert_subscribers.py" || \
    echo "  ⚠️  Alertes abonnés échouées — non bloquant"

echo "✅  Run terminé — $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
