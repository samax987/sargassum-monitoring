#!/bin/bash
# sargassum_run.sh
# Pipeline complet : collecte → simulation de dérive → scores de plage.
# Appelé par launchd toutes les 6 heures.

set -e

PYTHON=/opt/homebrew/bin/python3
DIR=/Users/samueldemacedo/Desktop

cd "$DIR"

echo ""
echo "──────────────────────────────────────────────"
echo "▶  $(date -u '+%Y-%m-%dT%H:%M:%SZ')  sargassum_run.sh"
echo "──────────────────────────────────────────────"

echo ""
echo "[1/3] Collecte des données…"
"$PYTHON" "$DIR/sargassum_collector.py"

echo ""
echo "[2/3] Simulation de dérive OpenDrift…"
"$PYTHON" "$DIR/sargassum_collector.py" --simulate

echo ""
echo "[3/3] Calcul des scores de plage…"
"$PYTHON" "$DIR/beaches.py"

echo ""
echo "✅  Run terminé."
