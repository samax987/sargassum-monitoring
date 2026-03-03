#!/bin/bash
set -e
PYTHON=/opt/sargassum/venv/bin/python3
DIR=/opt/sargassum

cd "$DIR"
echo ""
echo "──────────────────────────────────────────────"
echo "▶  $(date -u '+%Y-%m-%dT%H:%M:%SZ')  sargassum_run_linux.sh"
echo "──────────────────────────────────────────────"

echo "[1/3] Collecte des données…"
"$PYTHON" "$DIR/sargassum_collector.py"

echo "[2/3] Simulation de dérive OpenDrift…"
"$PYTHON" "$DIR/sargassum_collector.py" --simulate || \
    echo "  ⚠️  Simulation OpenDrift échouée — données de dérive non mises à jour"

echo "[3/3] Calcul des scores de plage…"
"$PYTHON" "$DIR/beaches.py"

echo "✅  Run terminé — $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
