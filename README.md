# Sargassum Monitoring — Surveillance des sargasses aux Antilles

Système automatisé de surveillance, prévision et alerte des échouages de sargasses sur les plages des Antilles françaises.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          VPS (45.55.239.73)                          │
│                                                                      │
│  Cron (0 */6 * * *)                                                  │
│  └── sargassum_run_linux.sh                                          │
│       ├── [1/4] sargassum_collector.py   ← 6 sources de données     │
│       ├── [2/4] sargassum_collector.py --simulate  ← OpenDrift      │
│       ├── [3/4] beaches.py              ← scoring par plage         │
│       └── [4/4] sargassum_alert.py      ← alertes Telegram          │
│                                                                      │
│  Crons quotidiens / hebdo                                            │
│  ├── sargassum_webcam_capture.py  (14h)  ← captures webcam          │
│  ├── sarga_news_scraper.py        (7h)   ← scraping news caraïbes   │
│  ├── sarga_claude_intel.py        (lun 8h) ← collecte IA web        │
│  ├── sarga_calibration_spatial.py (lun 9h) ← biais prédit/observé   │
│  └── sargassum_healthcheck.py     (30 *)  ← surveillance pipeline   │
│                                                                      │
│  systemd (derrière nginx + Cloudflare)                               │
│  ├── sargassum-web.service       → sargassum.villasuite.app         │
│  │     site public bilingue FR/EN + portail contributeurs           │
│  ├── sargassum-dashboard.service → /dashboard/ (Basic Auth)         │
│  └── sargassum-bot.service       → bot Telegram abonnements         │
│                                                                      │
│  SQLite: sargassum_data.db (25 tables)                               │
└──────────────────────────────────────────────────────────────────────┘
```

## Sources de données

| Source | Type | Fréquence | Table SQLite |
|---|---|---|---|
| NOAA ERDDAP AFAI_7D | Satellite AFAI (détection sargasse) | À chaque collecte | `noaa_afai` |
| NOAA SIR | Rapport PDF hebdomadaire + KMZ | À chaque collecte | `noaa_sir_reports` |
| FORESEA CNRS | Prévisions API WordPress | À chaque collecte | `foresea_forecasts` |
| Sargassum Monitoring | Articles API WordPress | À chaque collecte | `sargassum_monitoring` |
| Copernicus Marine | Courants totaux surface (CMEMS) | À chaque collecte | `copernicus_currents` |
| AVISO+ DUACS | Courants géostrophiques SSH | À chaque collecte | `aviso_geostrophic` |
| OpenDrift | Simulation dérive 5 jours | À chaque collecte | `drift_predictions` |
| Webcams | 7 caméras (Saint-Barth + Martinique) | 14h UTC quotidien | `webcam_captures` |

## Algorithme de scoring des plages

Pour chaque plage et chaque jour (j+0 à j+5), on calcule :

- **`local_score`** : somme gaussienne σ=`radius_km` → détecte les arrivées imminentes
- **`regional_score`** : somme gaussienne σ=50 km → détecte les masses qui approchent
- **`closest_km`** : distance à la particule de dérive la plus proche
- **`density_km2`** : particules estimées par km² dans la zone de catchment

```
risk_level = none   si regional_score < 5
           = low    si regional_score ∈ [5, 25)
           = medium si regional_score ∈ [25, 75)
           = high   si regional_score ≥ 75
```

Les scores sont extrapolés à la population totale de particules via `ratio = n_active / n_sample`.

## Plages surveillées (5 îles)

La liste vit dans la table `beaches_config`, gérée via l'interface `/admin/beaches`
(ajout/édition sans toucher au code). Répartition d'origine : Saint-Barthélemy 10,
Saint-Martin 8, Martinique 13, Guadeloupe 10, Marie-Galante 5 — extensible
(ex. « Anse des Cayes » ajoutée à SBH).

## Installation

### Prérequis

- Python 3.12+
- Compte Copernicus Marine (gratuit) : https://data.marine.copernicus.eu
- Compte AVISO+ (optionnel) : https://www.aviso.altimetry.fr

### Configuration

Créer un fichier `.env` dans le répertoire du projet :

```env
COPERNICUS_USERNAME=votre_email@example.com
COPERNICUS_PASSWORD=votre_mot_de_passe
AVISO_USERNAME=votre_email@example.com     # optionnel
AVISO_PASSWORD=votre_mot_de_passe          # optionnel
TELEGRAM_TOKEN=votre_bot_token             # obtenu via @BotFather
TELEGRAM_CHAT=votre_chat_id                # ID du canal/groupe Telegram
ADMIN_PASSWORD=mot_de_passe_fort           # /admin + Basic Auth du dashboard
FLASK_SECRET_KEY=64_hex_aleatoires         # signature des sessions du portail
```

### Dépendances

```bash
pip install -r requirements.txt
```

### Exécution manuelle

```bash
# Collecte complète (toutes sources)
python sargassum_collector.py

# Simulation de dérive OpenDrift (5 jours)
python sargassum_collector.py --simulate

# Calcul des scores par plage
python beaches.py

# Afficher le dernier rapport
python beaches.py --report

# Envoyer une alerte Telegram (test)
python sargassum_alert.py --test

# Forcer l'envoi d'une alerte
python sargassum_alert.py --force

# Pipeline complet (équivalent au cron)
bash sargassum_run_linux.sh
```

### Démarrer le dashboard

```bash
streamlit run sargassum_dashboard.py --server.port 8501 --server.address 0.0.0.0
```

## Structure du projet

```
/opt/sargassum/
├── sargassum_collector.py    # Collecte multi-sources + simulation OpenDrift
├── beaches.py                # Scoring par plage (algorithme gaussien)
├── beaches_db.py             # Table beaches_config (gestion via /admin)
├── sargassum_alert.py        # Alertes Telegram anti-spam
├── sargassum_alert_subscribers.py # Alertes par plage (abonnés du bot)
├── sargassum_bot.py          # Bot Telegram (abonnements aux plages)
├── sargassum_web.py          # Site public Flask (carte + API JSON)
├── sargassum_admin_routes.py # /admin : gestion des plages (Basic Auth)
├── sargassum_contributor_routes.py # Portail bénévoles /contribuer
├── contributors_db.py        # Comptes bénévoles + file de modération
├── contrib_i18n.py           # Traductions FR/EN (portail + carte)
├── sargassum_dashboard.py    # Dashboard Streamlit (8 pages)
├── sargassum_healthcheck.py  # Surveillance du pipeline (cron horaire)
├── sarga_news_scraper.py     # Scraping news caribéennes
├── sarga_claude_intel.py     # Extraction d'observations par IA
├── sarga_calibration_spatial.py # Biais spatial prédit vs observé
├── sargassum_webcam_capture.py  # Capture webcams
├── sargassum_run_linux.sh    # Pipeline cron (Linux/VPS)
├── test_beaches.py           # Tests unitaires beaches.py
├── test_alert.py             # Tests unitaires sargassum_alert.py
├── test_contributors.py      # Tests unitaires portail contributeurs
├── templates/                # index (carte), stats, admin, contrib_*
├── requirements.txt          # Dépendances Python figées
├── .env                      # Identifiants (non versionné)
├── sargassum_data.db         # Base SQLite (non versionnée)
├── contrib_photos/           # Photos des signalements (non versionné)
└── captures/                 # Images webcam (non versionné)
```

## Base de données SQLite

25 tables. Les principales :

| Table | Description | Rétention |
|---|---|---|
| `noaa_afai` | Données satellite AFAI 7 jours | 48 entrées max |
| `noaa_sir_reports` | Rapports PDF SIR hebdomadaires | 48 entrées max |
| `foresea_forecasts` | Prévisions FORESEA CNRS | 48 entrées max |
| `sargassum_monitoring` | Articles Sargassum Monitoring | 48 entrées max |
| `copernicus_currents` | Courants totaux Copernicus | 48 entrées max |
| `aviso_geostrophic` | Courants géostrophiques AVISO+ | 48 entrées max |
| `drift_predictions` | Snapshots dérive OpenDrift, pas 3h (`hour_offset`) | Toutes simulations |
| `beach_risk_scores` | Scores de risque par plage × jour | 60 derniers `computed_at` |
| `beach_timeline` | Scoring 3h (heure d'arrivée prévue, `/api/timeline`) | Dernier run |
| `beaches_config` | Plages surveillées (gérées via `/admin`) | Indéfini |
| `webcam_captures` | Captures webcam (chemin fichier) | 100 entrées |
| `alert_state` | Historique alertes Telegram (anti-spam) | Indéfini |
| `telegram_subscriptions` | Abonnés du bot par plage | Indéfini |
| `beach_observations` | Observations terrain validées (lues par la calibration) | Indéfini |
| `contributors` | Comptes bénévoles (statut pending/active/banned) | Indéfini |
| `contributor_observations` | Signalements bénévoles en file de modération | Indéfini |
| `calibration_spatial` / `_bias` | Erreur km + biais directionnel prédit/observé | Indéfini |
| `news_observations`, `claude_intel_log` | Observations extraites des news / logs IA | Indéfini |

## Tests

```bash
# Tous les tests
python -m pytest test_beaches.py test_alert.py test_contributors.py -v

# Avec couverture de code
python -m pytest test_beaches.py test_alert.py --cov=beaches --cov=sargassum_alert --cov-report=term-missing

# Tests spécifiques
python -m pytest test_beaches.py::TestHaversine -v
python -m pytest test_contributors.py::TestModeration -v
```

## Site public & portail contributeurs (sargassum.villasuite.app)

Site Flask bilingue FR/EN servi par gunicorn derrière nginx/Cloudflare :

- **Carte publique** : risque par plage (J+0→J+5), animation de la dérive (pas 3h),
  heure d'arrivée prévue 48h, météo, abonnement aux alertes Telegram.
- **Portail bénévoles `/contribuer`** : inscription (compte validé manuellement),
  puis signalement de l'état des plages (niveau de sargasses, couverture %,
  photo optionnelle — ré-encodée, EXIF/GPS supprimés). Chaque signalement passe
  par une **file de modération** : approuvé → promu dans `beach_observations`
  (source `contributor`) → pris en compte par la calibration. Rien n'atteint
  le modèle sans validation.
- **Sécurité** : sessions signées, CSRF, mots de passe hachés, rate-limiting,
  CSP par nonce, HSTS ; uploads plafonnés à 12 Mo (Flask + nginx).

## Dashboard Streamlit (8 pages)

| Page | Contenu |
|---|---|
| **Carte** | Carte Folium avec particules de dérive (j+0 à j+5), flèches courants |
| **Métriques** | KPIs + graphiques temporels AFAI, vitesses courants |
| **Actualités** | Rapports NOAA SIR, prévisions FORESEA, articles Sargassum Monitoring |
| **Plages** | Carte risque par île, heatmap `regional_score`, tableau détaillé |
| **Webcams** | Dernières captures + historique 24h |
| **Observations** | Saisie terrain + analyse URL/texte par IA + historique |
| **Contributeurs** | Modération : validation des comptes et des signalements (+ photos) |
| **Calibration** | Erreur spatiale prédit/observé (km), biais directionnels |

Accès : `https://sargassum.villasuite.app/dashboard/` (HTTP Basic Auth nginx,
fichier `/etc/nginx/.htpasswd-sargassum`).

## Alertes Telegram

- Déclenchement : plage ≥ `medium` (regional_score ≥ 25)
- Fenêtre d'envoi : 06h00 UTC ±1h (cron de 06h)
- Anti-spam : hash MD5 du payload des plages en alerte — envoi uniquement si changement
- Île prioritaire (Saint-Barth) : détail plage par plage
- Autres îles : résumé compact (comptage par niveau)

## Service systemd

```bash
systemctl status sargassum-dashboard.service
systemctl restart sargassum-dashboard.service
journalctl -u sargassum-dashboard.service -f
```

## Licence

Usage privé / monitoring personnel.
