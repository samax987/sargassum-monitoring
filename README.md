# Sargassum Monitoring — Saint-Barthélemy

Système de surveillance automatisée des sargasses pour Saint-Barth.
Collecte des données satellite et océanographiques, simulation de dérive OpenDrift,
scoring de risque par plage, et dashboard Streamlit.

---

## Architecture

```
sargassum_collector.py     Collecte 6 sources + simulation OpenDrift
beaches.py                 Scoring de risque par plage (Gaussian density)
sargassum_dashboard.py     Dashboard Streamlit (5 pages)
sargassum_webcam_capture.py  Capture horaire des webcams
sargassum_run.sh           Pipeline cron : collecte → simulation → scores
com.sargassum.collector.plist  launchd job toutes les 6h
com.sargassum.webcam.plist     launchd job toutes les 1h
```

Toutes les données sont stockées dans **`sargassum_data.db`** (SQLite).

---

## Sources de données

| Source | Données | Fréquence |
|---|---|---|
| NOAA SIR | Rapport hebdomadaire PDF + KMZ | Hebdomadaire |
| NOAA ERDDAP AFAI | Indice de sargasses 7-jours (pixels satellite) | Quotidien |
| Copernicus Marine | Courants de surface Caraïbes (uo/vo, 0.25°) | Horaire |
| AVISO+ DUACS | Courants géostrophiques SSH (ugos/vgos, 0.125°) | Quotidien |
| FORESEA CNRS | Prévisions et posts WordPress | À la demande |
| Sargassum Monitoring | Articles scientifiques (API WordPress) | À la demande |
| OpenDrift | Simulation de dérive 5 jours (7 000+ particules) | À chaque run |
| Webcams | 8 caméras Saint-Barth / Martinique / Saint-Martin | Horaire |

---

## Installation

```bash
pip install streamlit plotly folium streamlit-folium \
            requests beautifulsoup4 numpy schedule \
            copernicusmarine opendrift psutil
```

Créer un fichier `.env` dans le répertoire du projet :

```
COPERNICUS_USERNAME=votre_login
COPERNICUS_PASSWORD=votre_mot_de_passe
```

Inscription gratuite : https://data.marine.copernicus.eu

---

## Utilisation

```bash
# Collecte unique (6 sources)
python sargassum_collector.py

# Simulation de dérive OpenDrift
python sargassum_collector.py --simulate

# Calcul des scores de risque par plage
python beaches.py

# Rapport de risque (sans recalcul)
python beaches.py --report

# Capture webcams (run unique)
python sargassum_webcam_capture.py --once

# Dashboard
streamlit run sargassum_dashboard.py
```

---

## Dashboard

5 pages accessibles via la sidebar :

- **Carte** — carte Folium avec particules de dérive (j+0…j+5), courants Copernicus et AVISO
- **Métriques** — KPI (couverture AFAI, vitesses courants) + graphiques Plotly
- **Actualités** — rapports NOAA SIR, prévisions FORESEA, articles Sargassum Monitoring
- **Plages** — carte Saint-Barth + heatmap de risque par plage × jour
- **Webcams** — dernières captures par caméra, historique 24h

---

## Scoring de risque des plages

Le risque est calculé avec deux scores gaussiens extrapolés à la population entière
(`n_active / n_sample`) :

| Score | Formule | Usage |
|---|---|---|
| `local_score` | Σ exp(−d²/2r²) × ratio | Arrivées imminentes (σ = radius_km) |
| `regional_score` | Σ exp(−d²/2×50²) × ratio | Masse qui approche (σ = 50 km) |

Le `risk_level` est dérivé du `regional_score` :

| Niveau | Seuil | Signification |
|---|---|---|
| 🟢 none | < 5 | Aucune masse à moins de ~120 km |
| 🟡 low | ≥ 5 | 1 particule autour de 50–100 km |
| 🟠 medium | ≥ 25 | Masse significative en approche |
| 🔴 high | ≥ 75 | Masse importante à < 50 km |

### Plages couvertes

| Plage | Lat | Lon | Rayon |
|---|---|---|---|
| Flamands | 17.9067 | -62.8467 | 3 km |
| Colombier | 17.9033 | -62.8600 | 2 km |
| Saint-Jean | 17.9000 | -62.8267 | 4 km |
| Lorient | 17.9000 | -62.8100 | 3 km |
| Grand Cul-de-Sac | 17.9117 | -62.7917 | 3 km |
| Petit Cul-de-Sac | 17.9067 | -62.7967 | 2 km |
| Toiny | 17.8933 | -62.7817 | 2 km |
| Gouverneur | 17.8717 | -62.8433 | 3 km |
| Grande Saline | 17.8717 | -62.8267 | 3 km |
| Marigot | 17.9033 | -62.8067 | 2 km |

---

## Automatisation (macOS launchd)

```bash
# Installer les jobs
launchctl load ~/Library/LaunchAgents/com.sargassum.collector.plist
launchctl load ~/Library/LaunchAgents/com.sargassum.webcam.plist

# Déclencher manuellement
launchctl start com.sargassum.collector
launchctl start com.sargassum.webcam

# Logs
tail -f /tmp/sargassum_collector.log
tail -f /tmp/sargassum_webcam.log
```

| Job | Script | Fréquence | Log |
|---|---|---|---|
| `com.sargassum.collector` | `sargassum_run.sh` | Toutes les 6h + au démarrage | `/tmp/sargassum_collector.log` |
| `com.sargassum.webcam` | `sargassum_webcam_capture.py --once` | Toutes les 1h + au démarrage | `/tmp/sargassum_webcam.log` |

---

## Structure de la base de données

| Table | Contenu |
|---|---|
| `noaa_sir_reports` | Rapports PDF hebdomadaires NOAA |
| `noaa_afai` | Couverture sargasses AFAI 7-jours |
| `copernicus_currents` | Courants de surface Copernicus |
| `aviso_geostrophic` | Courants géostrophiques AVISO+ DUACS |
| `foresea_forecasts` | Prévisions FORESEA CNRS |
| `sargassum_monitoring` | Articles Sargassum Monitoring |
| `drift_predictions` | Snapshots OpenDrift (j+0…j+5, ≤ 500 pts) |
| `beach_risk_scores` | Scores gaussiens par plage × jour |
| `webcam_captures` | Métadonnées captures webcam |
