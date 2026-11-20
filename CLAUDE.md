# CLAUDE.md — Sargassum Monitoring

Instructions pour Claude Code sur ce projet. Lis ce fichier avant toute modification.

## Contexte du projet

Système de surveillance automatisée des échouages de sargasses aux Antilles françaises.
Déployé sur VPS `45.55.239.73`. Site public : `https://sargassum.villasuite.app` (bilingue
FR/EN). Dashboard admin : `https://sargassum.villasuite.app/dashboard/` (Basic Auth nginx,
user `sam`, mot de passe = `ADMIN_PASSWORD` du `.env` — ne JAMAIS retirer cette auth).

Le projet vise le **"dernier kilomètre"** : combiner données satellites/océanographiques
avec des observations terrain réelles pour produire des alertes précises au niveau de la plage.
Les observations terrain viennent de Sam (dashboard) ET de bénévoles via le portail
public `/contribuer` (voir section Portail contributeurs).

---

## Architecture

```
/opt/sargassum/
├── sargassum_collector.py      # Collecte 6 sources + simulation OpenDrift
├── beaches.py                  # Scoring gaussien par plage (local + régional)
├── beaches_db.py               # Table beaches_config (source des plages, gérée via /admin)
├── sargassum_alert.py          # Alertes Telegram anti-spam
├── sargassum_alert_subscribers.py # Alertes par plage pour les abonnés du bot
├── sargassum_bot.py            # Bot Telegram (abonnements)
├── sargassum_web.py            # Site public Flask (carte bilingue + API JSON)
├── sargassum_admin_routes.py   # /admin gestion plages (Basic Auth durcie)
├── sargassum_contributor_routes.py # Portail bénévoles /contribuer (blueprint)
├── contributors_db.py          # Comptes bénévoles + file de modération
├── contrib_i18n.py             # Traductions FR/EN (portail + carte publique)
├── sargassum_dashboard.py      # Dashboard Streamlit 8 pages
├── sargassum_healthcheck.py    # Surveillance pipeline (cron horaire)
├── sargassum_webcam_capture.py # Captures webcams automatiques
├── sarga_news_scraper.py       # Scraping news caribéennes + calibration
├── sarga_claude_intel.py       # Collecteur IA (Claude Haiku) — URLs + texte + web
├── sarga_calibration_spatial.py # Calibration spatiale prédit vs observé (cron lundi)
├── sarga_verification.py       # Vérification prévisions plage × jour vs terrain, avec témoins (cron lundi)
├── sarga_exposure.py           # Fenêtre d'exposition par plage (lancer de rayons sur le trait de côte GSHHG)
├── sarga_webcam_truth.py       # Vérité webcam : un verdict par jour lu sur l'image (sans IA), cron 14h10
├── sargassum_run_linux.sh      # Pipeline cron complet
├── test_beaches.py             # Tests unitaires beaches.py
├── test_alert.py               # Tests unitaires sargassum_alert.py
├── test_contributors.py        # Tests unitaires portail (couche DB, base temporaire)
├── test_webcam_truth.py        # Tests vérité webcam (images de synthèse) + juge webcam de la vérification
├── test_seed_mask.py           # Tests du masque côtier des points de départ (distance à la côte)
├── templates/                  # index (carte), stats, admin, contrib_*
├── contrib_photos/             # Photos signalements (jamais committer)
├── requirements.txt            # Dépendances figées
└── .env                        # Secrets (jamais committer)
```

### Base de données SQLite (`sargassum_data.db`)

25 tables. Les principales :

| Table | Contenu |
|---|---|
| `noaa_afai` | Satellite AFAI 7 jours (NOAA ERDDAP) |
| `noaa_sir_reports` | Rapports PDF SIR hebdomadaires |
| `foresea_forecasts` | Prévisions FORESEA CNRS |
| `sargassum_monitoring` | Articles API WordPress |
| `copernicus_currents` | Courants totaux surface Copernicus Marine |
| `aviso_geostrophic` | Courants géostrophiques SSH AVISO+ |
| `drift_predictions` | Snapshots dérive OpenDrift (pas 3h, purgé à 90 runs). `positions_region_blob` = jeu RÉGIONAL COMPLET [[lon,lat,flag]] zlib, flag 1 = particule échouée gardée à son point d'impact (depuis 2026-11-09) |
| `beach_risk_scores` | Scores risque par plage × jour, **purgé à 60 runs** (~2 semaines) ; colonne `n_stranded` = échouées dans le rayon |
| `forecast_archive` | **Archive permanente, jamais purgée** : chaque prévision plage × échéance × `target_date` (présence + régional + n_stranded). Matière première de la vérification |
| `verification_matches` / `_summary` | Prévu vs observé par échéance (POD, FAR, PSS) + témoins climatologie/persistance (`sarga_verification.py`) |
| `webcam_captures` | Captures webcam (chemins fichiers) |
| `webcam_truth` | Un verdict par caméra × jour (`radeaux` / `rien` / `douteux` / `illisible`) lu sur l'image par `sarga_webcam_truth.py`. Seuls `radeaux` et `rien` servent de vérité. N'alimente JAMAIS `beach_observations` |
| `verification_webcam_summary` | Prévu vs verdict webcam par échéance (mêmes métriques et témoins que le juge terrain, stocké à part) |
| `alert_state` | Historique alertes Telegram (anti-spam) |
| `beach_observations` | Observations terrain + imports + IA (513+ lignes) |
| `news_observations` | Observations extraites du scraper news |
| `claude_intel_log` | Logs des collectes Claude Haiku |
| `calibration_matches` | Matchs obs. terrain ↔ prédictions |
| `calibration_bias` | Biais de calibration par île/mois |
| `calibration_spatial` / `_bias` | Erreur spatiale (km) + biais directionnel — appliqué par beaches.py si n_obs ≥ 3 |
| `beach_timeline` | Scoring 3h (heure d'arrivée prévue, consommé par /api/timeline) |
| `beaches_config` | Plages surveillées — SOURCE DE VÉRITÉ (beaches.BEACHES chargé depuis cette table). `exposure_json` = fenêtre d'exposition (72 secteurs de 5°, eau libre en km, œil 300 m au large) calculée par `sarga_exposure.py --all` ; une plage ajoutée via /admin est complétée automatiquement au prochain scoring |
| `telegram_subscriptions` | Abonnés du bot par plage |
| `contributors` | Comptes bénévoles (pending/active/rejected/banned) |
| `contributor_observations` | Signalements bénévoles en modération (photo_path éventuel) |

---

## Crons actifs

| Heure UTC | Script | Description |
|---|---|---|
| `0 */6 * * *` | `sargassum_run_linux.sh` | Pipeline complet : collecte + drift + scoring + alerte |
| `0 7 * * *` | `sarga_news_scraper.py` | Scraping news caribéennes quotidien |
| `0 8 * * 1` | `sarga_claude_intel.py` | Collecte IA web (Claude Haiku) — chaque lundi |
| `0 9 * * 1` | `sarga_calibration_spatial.py` | Recalibration spatiale prédit vs observé — chaque lundi. ⚠️ Produit 0 match depuis l'été 2026 (obs trop anciennes vs dérive purgée à 90 runs) → aucune correction de biais n'est appliquée en pratique |
| `10 9 * * 1` | `sarga_verification.py` | Vérification plage × jour vs observations terrain, avec témoins hasard/persistance (archive depuis 2026-11-09) |
| `0 14 * * *` | `sargassum_webcam_capture.py --once` | Capture webcams |
| `10 14 * * *` | `sarga_webcam_truth.py --today` | Verdict du jour lu sur la capture de Flamands (log `logs/webcam_truth.log`, code retour 1 si aucune capture) |

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
| **Contributeurs** | Modération : validation des comptes bénévoles et de leurs signalements (photo affichée) |
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
- Les DataFrames s'affichent avec `st.dataframe(..., width="stretch", hide_index=True)`
  (`use_container_width` est déprécié depuis Streamlit 1.54)
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
ADMIN_PASSWORD=...            # /admin Flask + Basic Auth nginx du dashboard
FLASK_SECRET_KEY=...          # Signature des sessions du portail (64 hex)
```

---

## Portail contributeurs (`/contribuer`)

Bénévoles SBH : inscription → compte `pending` → **Sam valide** (dashboard, page
Contributeurs) → signalements possibles → chaque signalement arrive en file de
modération → **Sam approuve** → promotion dans `beach_observations`
(`source='contributor'`) → pris en compte par la calibration.

**Règle d'or : un signalement non approuvé ne doit JAMAIS atteindre
`beach_observations`** (c'est la table lue par la calibration). La promotion se
fait uniquement dans `contributors_db.approve_observation()` — ne pas créer
d'autre chemin d'écriture.

- Bilingue FR/EN : toutes les chaînes UI dans `contrib_i18n.py` (`?lang=` mémorisé en session)
- Photos : ré-encodées par Pillow (EXIF/GPS supprimés, max 1600 px) dans `contrib_photos/`,
  servies uniquement à leur auteur via `/contribuer/photo/<id>` ; 12 Mo max
  (Flask `MAX_CONTENT_LENGTH` + nginx `client_max_body_size`, à garder synchros)
- Sécurité : sessions signées, CSRF sur les POST du portail uniquement, rate-limit en mémoire
  (par worker gunicorn → seuil effectif ~2×)

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

**Depuis le 2026-11-09, le scoring se fait sur le jeu RÉGIONAL COMPLET**
(`positions_region_blob` : toutes les particules de la boîte Antilles
−65.5…−58.5 / 12.5…20.5, `ratio = 1`, pas d'extrapolation). Les particules
**échouées** (statut OpenDrift `stranded`) sont conservées à leur point d'impact
et comptées (`n_stranded` dans le rayon de la plage) : c'est le signal d'arrivée.
Avant : 500 pts uniformes sur tout le bassin ×12 → `local_score` quasi toujours 0,
et les échouées étaient supprimées (NaN) avant le scoring. L'ancien mode
(`scoring_mode='sample'`) reste en repli pour les simulations antérieures.

**Fenêtre d'exposition (2026-11-09, étape 2 du dernier km).** Chaque plage a sa
rose des directions d'où la mer ouverte l'atteint (`beaches_config.exposure_json`,
calculée sur le trait de côte GSHHG depuis un œil 300 m au large). Au scoring :
- `local_score` ne compte, parmi les particules EN MER, que celles visibles par
  la mer ; `local_score_raw` garde la valeur sans fenêtre (pour juger l'apport).
  Les échouées à < 3 km comptent sans test (posées jusqu'à 2-3 km dans les
  terres par le pas de 3 h d'OpenDrift).
- `regional_score` (alertes) n'est PAS filtré ; `regional_exposed` et
  `exposed_frac` (0..1) disent quelle part de la masse régionale FAIT FACE à la
  plage par la mer. C'est ce qui distingue deux plages d'une même île
  (ex. SBH j+1 : Anse des Cayes 0 %, Toiny 45 %). Pas utilisé par le badge
  public ni les alertes (à valider d'abord par la vérification), mais AFFICHÉ
  sur la carte publique depuis le 2026-11-09 : `/api/status` expose
  `exposed_frac` par jour et `facing_deg` + `exposed_3d` par plage ; la bulle
  d'une plage montre « 🧭 Part de la masse au large qui fait face à cette
  plage : 46 % (plage ouverte au S) » dès qu'il y a une masse (régional ≥ faible),
  et le tag « Masse au large » des cartes latérales porte « · 🧭 45 % en face ».
  Chaînes FR/EN : `facing_popup`, `facing_opens`, `facing_short` (contrib_i18n).

**Fond de carte (2026-11-09)** : le calque de noms CartoDB `dark_only_labels`
exige une clé API et imprimait « API KEY REQUIRED » partout → remplacé par le
calque de référence Esri `World_Boundaries_and_Places` (gratuit, même serveur
que le fond satellite). Ne pas revenir à CartoDB sans clé.

Chaque run écrit aussi dans `forecast_archive` (jamais purgé) ; la vérification
hebdo (`sarga_verification.py`) compare prévu vs observé par échéance et contre
deux témoins (climatologie, persistance). Règle : on ne croit un score que s'il
bat les témoins.

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

## Vérité webcam (`sarga_webcam_truth.py`, depuis 2026-11-10)

But : avoir une vérité terrain QUOTIDIENNE et automatique, là où les observations humaines sont rares.
Une seule caméra pour l'instant : `Flamand_Beach` → plage `Flamands` (Saint-Barth).

- **Lecture sans IA** (numpy + PIL, version `couleur-v4`) : sable repéré par sa couleur, trait de côte par colonne,
  puis comptage des taches plus brunes que l'eau voisine, au large (`large_px`).
- **Règle** : `cols < 0,85` → `illisible` ; image identique à la veille (sha1) → `illisible` (webcam figée) ;
  `large_px ≥ 300` → `radeaux` ; `< 100` → `rien` ; entre les deux → `douteux`.
- **Validé par un test à l'aveugle** avant déploiement (42 jours hors échantillon, juge qui a regardé les images sans voir les mesures) :
  `radeaux` juste 9 fois sur 10, `rien` juste 24 fois sur 25, contre un taux de base de 29 % de jours avec arrivage net.
  Réserve : le seuil 0,85 a été choisi après avoir vu le test, ces chiffres sont donc un peu optimistes.
- **Limites connues** : ne voit ni les « un peu » ni les dépôts déjà sur le sable ; les yachts au mouillage
  (novembre à avril) donnent 68 à 191 px, donc des `douteux` ; les masques (`X0, X1, Y0`) sont propres au cadrage
  de cette caméra, à refaire pour toute autre. `Saint-Jean_Plaine` a été écartée (sable illisible 22 fois sur 22).
- **Second juge de la vérification** : `sarga_verification.py` compare `forecast_archive` aux verdicts sûrs
  (`run_webcam`, table `verification_webcam_summary`, visible avec `--report`). Les premiers couples
  arrivent avec la capture du 2026-11-10 (l'archive commence le 2026-11-09).
- **Règle d'or respectée** : la webcam n'écrit que dans `webcam_truth`, jamais dans `beach_observations`.
- **Pas d'alerte** si la webcam tombe : l'échec est seulement journalisé dans `logs/webcam_truth.log`
  (le healthcheck Telegram relance toutes les 6 h, l'y brancher est une décision de Sam).

```bash
venv/bin/python3 sarga_webcam_truth.py --report      # répartition des verdicts
venv/bin/python3 sarga_webcam_truth.py --backfill    # relire toutes les captures (upsert, sans doublon)
venv/bin/python3 sarga_webcam_truth.py --image captures/.../2026-06-17_14-00.jpg   # mesurer une image
```

## Masque côtier des points de départ (`SARGA_SEED_COAST_KM=15`, ACTIF depuis le 2026-11-10 12h UTC)

**Ce qu'il fait** : la simulation ne sème plus les points satellite (AFAI 7 jours > 0,0001) situés
à moins de 15 km d'une côte. Code : `_lire_masque_km()`, `_masque_cotier()` et `_distance_cote_km()`
dans `sargassum_collector.py`. Activé par `export SARGA_SEED_COAST_KM=15` dans `sargassum_run_linux.sh`.

**Pourquoi** : près des côtes, le satellite « voit » des sargasses qui n'en sont pas (fonds peu profonds,
turbidité, bord de terre). Preuves mesurées les 10 et 11 novembre 2026, deux fois de façon indépendante :

| Taux de détection (pixels lisibles) | 0-5 km | 5-10 km | 10-15 km | 15-20 km | au large (> 30 km) |
|---|---|---|---|---|---|
| Petites Antilles, hors saison (déc.-fév.) | 34,1 % | 16,1 % | 5,2 % | 1,4 % | 1,0 % |
| Petites Antilles, en saison (mai-juil.) | 38,1 % | 24,5 % | 18,4 % | 16,5 % | 17,6 % |
| Venezuela (jamais de sargasses), hors saison | 36,5 % | 21,1 % | 10,5 % | 4,5 % | 0,1 % |
| Venezuela, en saison | 32,8 % | 18,5 % | 9,7 % | 5,3 % | 0,1 % |

(boîte Petites Antilles 11,5-18,8°N / 63,8-58,8°O ; Venezuela 10-11,6°N / 67,5-64°O ; 3 composites par période.)
Autres preuves : le côté sous le vent des îles détecte PLUS que le côté au vent (l'inverse d'un vrai échouage) ;
près de 10 % des pixels à moins de 3 km sont « détectés » plus de 80 % des semaines de l'année ;
le produit n'est pas masqué sur terre (62 % de « détection » sur les pixels de terre des Petites Antilles).
Podlejski et al. 2022 (Frontiers in Marine Science, 10.3389/fmars.2022.960939) masquent les fonds de moins
de 500 m pour cette raison. Avant le masque, 64 % des points de départ étaient à moins de 15 km d'une côte,
et tout le risque affiché en novembre 2026 venait de là.

**⚠️ Le seuil de 15 km est juste HORS saison, trop large EN saison.** Hors saison, la bande 10-15 km est
encore à 80 % de faux signal. En saison, elle ne dépasse plus le large (18,4 % contre 17,6 %) : le masque
y jette de vraies sargasses, justement celles qui touchent les plages en premier. Le badge de présence
« sur la plage » ne peut s'allumer qu'avec une particule venue de plus de 15 km.
**À revoir AVANT la saison (février-mars 2027)** : seuil saisonnier (par exemple 15 km hors saison,
5 à 8 km en saison), poids réduit pour les points côtiers au lieu de les jeter, ou masque par persistance
du pixel (écarter les pixels détectés plus de la moitié des semaines sur un an glissant).

**Traçabilité (depuis le 2026-11-11)** :
- chaque simulation note `seed_coast_km` et `n_seed_avant_masque` dans `drift_predictions.raw_metadata` ;
- `beaches.py` recopie le régime dans `forecast_archive.seed_coast_km` (0 = sans masque, NULL = inconnu) ;
- l'historique a été rattrapé : 0 jusqu'à la simulation de 2026-11-10T06:01:54Z, 15 à partir de 12:02:16Z ;
- `sarga_verification.py` ne juge que le régime de la prévision la plus récente, et l'écrit dans le rapport.
  `--masque 0` juge l'ancien régime, `--tous-regimes` mélange tout (comparaison seulement).

**Si le masque échoue** (variable illisible, mémoire, landmask cassé), la simulation **échoue** : code 1,
et `sargassum_run_linux.sh` pose `logs/ECHEC_SIMULATION`. La dernière dérive (masquée) reste en base.
Avant le 2026-11-11, toute exception Python de la simulation était avalée et le script sortait en succès :
`ECHEC_SIMULATION` n'était posé que si le processus était tué.
Si le masque écarte TOUS les points, la simulation échoue aussi plutôt que de semer la grille de repli 5°.
Une simulation sur grille de repli (ERDDAP sans aucun point) est notée `seed_coast_km` NULL (régime inconnu)
avec `grille_repli: true`, et ne fait pas basculer le régime jugé par la vérification.

**Signalement d un échec (depuis le 2026-11-14)** : à chaque simulation ratée, `sargassum_run_linux.sh`
ajoute une ligne horodatée à `logs/echecs_simulation.log` (jamais effacé). Le watchdog Telegram horaire
`sargassum_healthcheck.py` (contrôle `sim_echec`) échoue si une ligne a moins de 6 h : une alerte dans la
demi-heure, puis « système rétabli » au passage réussi suivant. Chaîne prouvée de bout en bout sur une copie
(variable illisible → code 1 → journal → contrôle en échec). Tests : `test_healthcheck.py`.
Le contrôle de santé quotidien (`/opt/claude-tools/sante-quotidienne.sh`), lui, est toujours sauté chaque
jour depuis le 2026-10-30 (texte d exemple grisé pris pour une saisie) : réparation dans une session à part.

**Vérification honnête dans le temps (depuis le 2026-11-11)** : `sarga_verification.py` ne juge qu'une
prévision calculée AVANT l'observation (`observed_at` pour le terrain, `captured_at` de la capture pour
la webcam). Avant, une échéance j+0 pouvait être une prévision faite le soir pour une capture de 14h UTC.

**Revenir en arrière** : commenter `export SARGA_SEED_COAST_KM=15` dans `sargassum_run_linux.sh`.
Effet au passage suivant (toutes les 6 h). La vérification basculera seule sur le régime « sans masque ».
Sauvegardes : `sargassum_run_linux.sh.bak-20261110-082218`, `*.bak-20261111-002105`, `*.bak-20261111-003314`,
base `sargassum_data.db.bak-masque-20261111-002105`.

**Limites connues** :
- les îlots de moins de ~1,7 km (Sombrero, Redonda, Tintamarre, île Fourchue) ne sont pas vus par la grille
  de 0,015° : le faux signal à leurs abords n'est pas masqué (1 particule sur 54 dans la région, le 10/11) ;
- autour des grandes îles (Hispaniola, Cuba, Yucatán) le halo dépasse 20 km : un masque de distance fixe
  ne suffit pas partout, sans effet aujourd'hui sur les plages suivies ;
- le calcul du masque monte à ~1,35 Go transitoires (landmask chargé une fois), pic total de la simulation inchangé
  (~2,6 Go, car 2,7 fois moins de particules) ;
- la webcam de Flamands (19 jours « radeaux ») ne permet pas encore de trancher entre 10 et 15 km.

**Effet mesuré à l'activation** : 6225 → 2270 particules ; plages × jours en « moyen » 159 → 0.
Le groupe Telegram principal reçoit un seul message « situation dégagée » au passage de 6h UTC du 11/11,
puis plus rien tant que rien n'atteint « moyen ». Les 5 abonnés par plage ne sont pas touchés
(leurs alertes portent sur la présence locale, déjà à zéro avant le masque).

**Ce qui a été écarté sur preuve : le « semis daté »**. Témoin du 2026-11-10 : détections 1 jour du 3 au 8 novembre,
transportées par le modèle (mêmes réglages que la prod, vent GFS des jours passés) et comparées aux images suivantes.
Au large (plus de 30 km, 568 graines, rayon 15 km, j+2 à j+6 cumulés) : retrouvées dans 32 % des cas après dérive,
contre 64 % en les laissant sur place, et la dérive ne fait pas mieux qu'une direction tirée au sort.
Courant seul : 37 % ; ajouter vent et vagues n'améliore rien. Faire dériver les vieux pixels aurait donc dégradé le semis.
Réserve : une seule semaine, en fin de saison, où le signal du large est au niveau du bruit de fond.
**Témoin rejoué en pleine saison le 2026-11-16 : le verdict s'inverse.** Six fenêtres de 9 jours réparties du
9 mai au 26 juillet 2026, 21 737 graines à 15 km et plus des côtes, mêmes réglages que la prod sauf le vent
(analyse L4 horaire de Copernicus `cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H`, NOMADS ne gardant que 10 jours).
Au large (30 km et plus, rayon 15 km, j+2 à j+6 cumulés, 13 924 comparaisons appariées) : retrouvées dans 71,0 %
des cas après dérive, 67,1 % sur place, 63,5 % avec la même distance dans une direction tirée au sort.
Écart dérive moins hasard : +7,5 points [+5,5 ; +10,0]. Écart dérive moins sur place : +3,9 points [+0,9 ; +7,4]
(intervalles à 95 % par rééchantillonnage des jours de semis). L'avantage sur l'immobilité grandit avec l'échéance
(+5,5 points de j+4 à j+6) et la dérive n'est derrière dans aucune des six fenêtres (deux égalités, quatre gains).
Lecture : en saison le signal du large est fait de vraies nappes qui bougent et le modèle les suit, modestement ;
en novembre il était fait de détections fixes, donc de bruit. **Le « semis daté » redevient donc une piste pour la
saison** (gain attendu de l'ordre de 4 à 5 points sur les pixels vieux de 4 à 6 jours), à décider avec le seuil
saisonnier du masque avant mars 2027. Limites : taux de retrouvailles peu discriminant quand la mer est couverte
de nappes, vent d'une autre source que la prod, une seule saison.
Scripts et résultats bruts : sur le Mac de Sam, `~/sargassum-temoins/2026-11-16-derive-saison/`.

**Semis daté : codé le 2026-11-20, ÉTEINT par défaut** (`SARGA_SEED_DATED`, ligne commentée dans `sargassum_run_linux.sh`).
Allumé, `simulate_drift` appelle `_semis_date` juste après le masque côtier :
1. chaque point du composite 7 jours est daté par la DERNIÈRE image 1 jour de la même semaine où son pixel dépasse le seuil
   (`_fetch_afai_1j` demandé par dates, `_fenetre_du_composite`, `_dernier_jour_vu`) ; passage satellite pris à 16 h UTC ;
2. forçages du passé téléchargés à part : courants Copernicus, vagues, et vent GFS des jours passés sur NOMADS
   (`_fetch_gfs_wind_passe`, environ 60 requêtes, analyses de chaque run) ;
3. dérive « de rattrapage » OpenDrift du jour de l'observation jusqu'à maintenant (`_deriver_graines`, mêmes réglages
   que la prévision) ; une particule échouée en route est rendue à sa dernière position en mer ;
4. la prévision de 5 jours repart de ces positions, sans autre changement.
Aucun repli silencieux : réglage mal écrit, composite de plus de 72 h (panne du jeu AFAI, déjà arrivée 10 jours en
juillet 2025), image du jour du composite absente, plus de 50 % de points introuvables, vent passé troué ou dérive
arrêtée avant l'heure font ÉCHOUER la simulation, donc alerte Telegram et dernière dérive gardée.
Trace : `raw_metadata.seed_dated` (true, false, null sur la grille de repli) et `seed_dated_bilan` (points dérivés,
âges, échouées en route, déplacement médian), recopié dans `forecast_archive.seed_dated` (1, 0, NULL avant le 20/11).
Essai réel sur copie le 20/11 : 1025 points, âge médian 99 h, déplacement médian 60 km, 89 échouées en route,
3 min contre 1 min 40, pic mémoire 2,76 Go contre 2,69 Go éteint. Tests : `test_seed_date.py` (43 tests, 25 mutations attrapées).
**Avant de l'allumer** : (a) comparer semis daté et semis actuel sur des semaines de mai-juillet, avec ce code ;
(b) séparer les régimes dans `sarga_verification.py` (aujourd'hui seul `seed_coast_km` l'est) ; (c) mesurer la mémoire
avec le nombre de particules de la saison ; (d) décider en même temps du seuil saisonnier du masque.
Limite connue, hors de ce changement : le semis habituel ne contrôle pas la fraîcheur du composite 7 jours.

---

### Constats satellite du 2026-11-09 (rien changé en prod)

- **Âge des points de départ** : le composite 7 jours (`AFAI_7D`, graine de la simulation) garde des pixels
  que l'image 1 jour n'a plus revus depuis longtemps. Sur 34 818 graines (6 jours, même zone, même pas de 3) :
  39 % revues à 0-1 jour, 26 % à 2-3 jours, **35 % à 4-6 jours**.
  ⚠️ L'idée du « semis daté » (faire dériver chaque vieux pixel jusqu'à aujourd'hui) a été TESTÉE le 2026-11-10
  et **écartée hors saison**, puis rejouée le 2026-11-16 sur mai-juillet où la dérive bat l'immobilité :
  voir la section « Masque côtier » ci-dessus.
- **Piège ERDDAP** : `AFAI_1D` et `AFAI_7D` n'ont pas les mêmes décimales de coordonnées. Toujours arrondir
  (3 décimales) avant de comparer deux jeux, sinon aucune clé ne correspond et on obtient un faux « 0 % ».
- **AVISO+ GOES journalier** (`GLO_ABDM_AAAAMMJJ_v0.nc`, 14 Mo, J+1) : plus lisible que `AFAI_1D` près de SBH
  (22 % et 52 % contre 6 % et 26 % sur deux jours), mais en désaccord avec la NOAA le 11-08 (amas à ~80 km au SSO).
  Non tranché : ne pas le brancher comme graine sans validation.

---

## Tests

```bash
cd /opt/sargassum
venv/bin/python3 -m pytest test_beaches.py test_alert.py test_contributors.py test_webcam_truth.py test_seed_mask.py test_healthcheck.py -v
# 183 tests (2026-11-14). test_contributors couvre la garantie de modération du portail,
# test_webcam_truth la règle de verdict et le fait que la webcam n'écrit jamais dans beach_observations.
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
