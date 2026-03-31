#!/bin/bash
set -e
PYTHON=/opt/sargassum/venv/bin/python3
DIR=/opt/sargassum

mkdir -p "$DIR/logs"
cd "$DIR"
echo ""
echo "──────────────────────────────────────────────"
echo "▶  $(date -u '+%Y-%m-%dT%H:%M:%SZ')  sargassum_run_linux.sh"
echo "──────────────────────────────────────────────"

echo "[1/4] Collecte des données…"
"$PYTHON" "$DIR/sargassum_collector.py"

echo "[2/4] Simulation de dérive OpenDrift…"
"$PYTHON" "$DIR/sargassum_collector.py" --simulate || \
    echo "  ⚠️  Simulation OpenDrift échouée — données de dérive non mises à jour"

echo "[3/4] Calcul des scores de plage…"
"$PYTHON" "$DIR/beaches.py"

echo "[4/4] Alertes Telegram…"
"$PYTHON" "$DIR/sargassum_alert.py" || \
    echo "  ⚠️  Alerte Telegram échouée — non bloquant"

echo "✅  Run terminé — $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
