# CLAUDE.md — Sargassum Monitoring

Instructions pour Claude Code sur ce projet. Lis ce fichier avant toute modification.

## Contexte du projet

Système de surveillance automatisée des échouages de sargasses aux Antilles françaises.
Déployé sur VPS `45.55.239.73`, dashboard accessible sur `http://45.55.239.73:8501`.

Le projet vise le **"dernier kilomètre"** : combiner données satellites/océanographiques
avec des observations terrain réelles pour produire des alertes précises au niveau de la plage.

---

## Architecture

```
/opt/sargassum/
├── sargassum_collector.py      # Collecte 6 sources + simulation OpenDrift
├── beaches.py                  # Scoring gaussien par plage (local + régional)
├── sargassum_alert.py          # Alertes Telegram anti-spam
├── sargassum_dashboard.py      # Dashboard Streamlit 7 pages
├── sargassum_webcam_capture.py # Captures webcams automatiques
├── sarga_news_scraper.py       # Scraping news caribéennes + calibration
├── sarga_claude_intel.py       # Collecteur IA (Claude Haiku) — URLs + texte + web
├── sarga_calibration.py        # Calibration automatique prédit vs observé
├── sargassum_run_linux.sh      # Pipeline cron complet
├── test_beaches.py             # 31 tests unitaires beaches.py
├── test_alert.py               # 26 tests unitaires sargassum_alert.py
├── requirements.txt            # Dépendances figées
└── .env                        # Secrets (jamais committer)
```

### Base de données SQLite (`sargassum_data.db`)

13 tables :

| Table | Contenu |
|---|---|
| `noaa_afai` | Satellite AFAI 7 jours (NOAA ERDDAP) |
| `noaa_sir_reports` | Rapports PDF SIR hebdomadaires |
| `foresea_forecasts` | Prévisions FORESEA CNRS |
| `sargassum_monitoring` | Articles API WordPress |
| `copernicus_currents` | Courants totaux surface Copernicus Marine |
| `aviso_geostrophic` | Courants géostrophiques SSH AVISO+ |
| `drift_predictions` | Snapshots dérive OpenDrift j+0 à j+5 |
| `beach_risk_scores` | Scores risque par plage × jour (14 000+ lignes) |
| `webcam_captures` | Captures webcam (chemins fichiers) |
| `alert_state` | Historique alertes Telegram (anti-spam) |
| `beach_observations` | Observations terrain + imports + IA (513+ lignes) |
| `news_observations` | Observations extraites du scraper news |
| `claude_intel_log` | Logs des collectes Claude Haiku |
| `calibration_matches` | Matchs obs. terrain ↔ prédictions |
| `calibration_bias` | Biais de calibration par île/mois |

---

## Crons actifs

| Heure UTC | Script | Description |
|---|---|---|
| `0 */6 * * *` | `sargassum_run_linux.sh` | Pipeline complet : collecte + drift + scoring + alerte |
| `0 7 * * *` | `sarga_news_scraper.py` | Scraping news caribéennes quotidien |
| `0 8 * * 1` | `sarga_claude_intel.py` | Collecte IA web (Claude Haiku) — chaque lundi |
| `0 9 * * 1` | `sarga_calibration.py` | Recalibration prédit vs observé — chaque lundi |
| `0 14 * * *` | `sargassum_webcam_capture.py --once` | Capture webcams |

---

## Pages du dashboard

| Page | Contenu |
|---|---|
| **Carte** | Carte Folium : particules dérive j+0→j+5, flèches courants |
| **Métriques** | KPIs AFAI, graphiques temporels, vitesses courants |
| **Actualités** | Rapports NOAA SIR, FORESEA CNRS, Sargassum Monitoring |
| **Plages** | Carte risque par île, heatmap scores, tableau détaillé |
| **Webcams** | Dernières captures + historique 24h |
| **Observations** | 4 onglets : Terrain / Analyser URL / Analyser texte / Collecte IA |
| **Calibration** | Biais prédit vs observé, graphiques, recommandations correction |

---

## Conventions de code

### Python
- Python 3.12+, type hints encouragés sur les fonctions publiques
- Chaque script est autonome et executable directement (`python script.py`)
- Les scripts acceptent `--dry-run` pour tester sans écrire en DB
- Chargement `.env` en tête de script via lecture manuelle (pas python-dotenv)
- Connexions SQLite toujours fermées dans un `finally` ou bloc `with`

### Base de données
- Toujours utiliser `CREATE TABLE IF NOT EXISTS` — ne jamais détruire une table existante
- Colonnes ajoutées avec `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` ou try/except
- Pas de migrations destructives

### Dashboard Streamlit
- Chaque page est un bloc `if page == "..."` ou `elif page == "..."`
- Toutes les connexions DB passent par `get_connection(db_path)` (défini en tête de fichier)
- Les DataFrames s'affichent avec `st.dataframe(..., use_container_width=True, hide_index=True)`
- Pas de `st.experimental_*` (déprécié)

### Sécurité
- `.env` jamais committé (dans `.gitignore`)
- `sargassum_data.db` jamais committé (données locales)
- `captures/` jamais commité (images)
- La clé `ANTHROPIC_API_KEY` est dans `.env`, jamais en dur dans le code

---

## Variables d'environnement (`.env`)

```
COPERNICUS_USERNAME=...       # Copernicus Marine Service
COPERNICUS_PASSWORD=...
AVISO_USERNAME=...            # AVISO+ (optionnel, fallback Copernicus)
AVISO_PASSWORD=...
TELEGRAM_TOKEN=...            # Bot Telegram (@BotFather)
TELEGRAM_CHAT=...             # Chat ID pour les alertes
ANTHROPIC_API_KEY=...         # Claude Haiku (sarga_claude_intel.py + dashboard)
```

---

## Algorithme de scoring des plages (`beaches.py`)

Pour chaque plage (lat/lon) et chaque day_offset (0 à 5) :

```python
local_score    = somme gaussienne des particules dans rayon radius_km (σ = radius_km)
regional_score = somme gaussienne des particules dans rayon 200 km  (σ = 50 km)
closest_km     = distance à la particule la plus proche

risk_level = "none"   si regional_score < 5
           = "low"    si regional_score ∈ [5, 25)
           = "medium" si regional_score ∈ [25, 75)
           = "high"   si regional_score ≥ 75
```

Les scores sont extrapolés à la population totale via `ratio = n_active / n_sample`.

---

## Module `sarga_claude_intel.py`

Utilise l'API Anthropic (Claude Haiku) pour extraire des observations sargasses depuis :
- Une URL (fetch + analyse)
- Un texte brut (WhatsApp, email, article)
- 10 sources web caribéennes (collecte hebdomadaire)

**Fonctions exportées :**
```python
analyze_url(url: str) -> list[dict]
analyze_text(text: str, source_hint: str) -> list[dict]
store_observations(observations, source_name, source_url, conn, dry_run) -> int
web_collect(dry_run, verbose) -> int
```

**Format de retour :**
```json
{
  "island": "Martinique",
  "beach_name": "Tartane",
  "event_date": "2026-04-03",
  "risk_level": "high",
  "coverage_pct": 80,
  "description": "...",
  "confidence": 0.9
}
```

---

## Module `sarga_calibration.py`

Compare les prédictions `beach_risk_scores` (OpenDrift) aux observations `beach_observations`.

- **Matching** : même île + date ±3 jours + nom de plage fuzzy (rapidfuzz, seuil 55%)
- **Biais** : calculé par île × mois (`mean_error` = RISK_NUM[prédit] - RISK_NUM[observé])
- **Résultat** : tables `calibration_matches` et `calibration_bias`

---

## Tests

```bash
cd /opt/sargassum
venv/bin/python3 -m pytest test_beaches.py test_alert.py -v
# 57 tests, couverture 61%
```

---

## Commandes utiles sur le VPS

```bash
# Statut du service
systemctl status sargassum-dashboard.service

# Relancer le dashboard
systemctl restart sargassum-dashboard.service

# Logs en direct
journalctl -u sargassum-dashboard.service -f

# Lancer la collecte manuellement
cd /opt/sargassum && bash sargassum_run_linux.sh

# Lancer la calibration manuellement
cd /opt/sargassum && venv/bin/python3 sarga_calibration.py --verbose

# Analyser un texte avec Claude Haiku
echo "Texte ici" | venv/bin/python3 sarga_claude_intel.py --text

# Vérifier la DB
sqlite3 sargassum_data.db ".tables"
sqlite3 sargassum_data.db "SELECT COUNT(*) FROM beach_observations;"
```

---

## Prochaines améliorations identifiées

1. **Alertes Telegram** : déclencher quand une plage passe en `high` (TELEGRAM_TOKEN disponible)
2. **Observations sur la carte** : superposer les observations terrain sur la page Plages
3. **Analyse photo IA** : Claude Haiku vision pour estimer la densité depuis une photo
4. **Rapport hebdomadaire Telegram** : synthèse lundi matin automatique
5. **Modèle saisonnier** : patterns récurrents quand on aura 6+ mois de données
