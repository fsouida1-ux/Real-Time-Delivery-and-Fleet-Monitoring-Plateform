# Real-Time Delivery & Fleet Monitoring Platform

Pipeline de données temps réel simulant le suivi de livraisons et de flotte
de livreurs : événements de commandes et positions GPS ingérés via Kafka,
traités avec garantie d'idempotence, puis agrégés périodiquement pour un
dashboard de reporting Metabase.

Projet personnel construit pour apprendre et démontrer une architecture de
streaming de données production-representative (Transactional Outbox
Pattern, consumer group Kafka avec commit manuel, séparation traitement
temps réel / traitement par lot).

## Ce que ce projet démontre

- Un événement traverse tout le système de bout en bout : source → Kafka → traitement → PostgreSQL → dashboard.
- **Le système survit à un crash du Consumer sans créer de doublons** (idempotence transactionnelle, validée par un test de crash dédié — voir plus bas).
- **Le traitement temps réel (par événement) est séparé du traitement analytique (par lot)** : les règles qui détectent une absence ou un dépassement de délai (livreur inactif, retard de livraison) ne peuvent pas se déclencher au passage d'un événement — elles sont donc évaluées par un scan périodique, pas dans le flux Kafka.

## Architecture

Schéma complet dans [`docs/architecture.md`](docs/architecture.md).

En résumé :

```
Sources (PostgreSQL + GPS Simulator)
    → outbox_events → db-producer → Kafka (orders-events, gps-events)
    → processing-consumer (validation → dédup → cleaning → règle rapide → commit manuel)
    → PostgreSQL (driver_positions, alerts, processed_events)
    → metrics-aggregator (agrégations + scans périodiques toutes les 30s)
    → SQL Views → Metabase
```

## Stack technique

| Composant | Technologie |
|---|---|
| Message broker | Apache Kafka 3.8.0 (mode KRaft, sans Zookeeper) |
| Base de données | PostgreSQL 16 (Alpine) |
| Langage des services | Python (`kafka-python`, `psycopg2`) |
| Orchestration | Docker Compose (volumes nommés pour la persistance) |
| UI Kafka | Kafbat v1.1.0 |
| Dashboard BI | Metabase v0.51.4 |

## Lancer le projet

Prérequis : Docker + Docker Compose installés.

```powershell
git clone https://github.com/fsouida1-ux/Real-Time-Delivery-and-Fleet-Monitoring-Plateform.git
cd delivery-platform
copy .env.example .env   # ajuster les valeurs si besoin
docker compose up --build
```

Aucune intervention manuelle nécessaire après cette commande : tables PostgreSQL, topics Kafka, et tous les services démarrent automatiquement (topics créés via un service `kafka-init` dédié).

Accès une fois démarré :
- **Kafbat** (observation Kafka) : http://localhost:8080
- **Metabase** (dashboard) : http://localhost:3000

## Structure du repo

```
delivery-platform/
├── infrastructure/postgres/          # SQL numéroté (001_ à 011_+), exécuté à l'init du conteneur
├── reporting/views/                  # Vues SQL pour Metabase, appliquées manuellement (isolées du schéma transactionnel)
├── shared/python/delivery_shared/    # Package Python partagé, installable en editable (pip install -e)
│   └── config.py                     # Lecture centralisée des variables d'environnement
├── services/
│   ├── db-producer/                  # Poll outbox_events → publie sur orders-events
│   ├── gps-simulator/                # Génère les positions GPS + publie sur gps-events (Producer intégré)
│   ├── order-simulator/              # Fait avancer des commandes à travers leurs statuts
│   ├── processing-consumer/          # Validation, déduplication, cleaning, écriture PostgreSQL, commit manuel
│   └── metrics-aggregator/           # Boucle 30s : agrégations + scans périodiques (DRIVER_INACTIVE, DELIVERY_DELAY)
├── tests/
│   ├── unit/
│   ├── integration/
│   └── crash_test/
├── docs/architecture.md              # Diagramme d'architecture (Mermaid)
├── docker-compose.yml
├── pyproject.toml                    # Config du package delivery_shared
└── .env.example                      # Variables d'environnement attendues (copier en .env)
```

## Preuve de robustesse — test de crash

Protocole exécuté (section 13 du cahier des charges technique) :

1. Publication d'un lot de 60 événements GPS.
2. Démarrage du `processing-consumer`, traitement d'une partie du lot.
3. Arrêt brutal du process (`Stop-Process -Force`, équivalent Windows d'un `kill -9`) après écriture DB mais avant certains commits Kafka.
4. Redémarrage du consumer.

**Résultat :** les événements non committés ont été rejoués par Kafka depuis le dernier offset committé, et **zéro doublon** créé — ni dans `processed_events` (contrainte `UNIQUE(event_id)`), ni dans `driver_positions`. 60/60 événements traités au total.

C'est la contrainte `ON CONFLICT (event_id) DO NOTHING` sur `processed_events`, combinée à la vérification et l'écriture métier dans **la même transaction PostgreSQL**, qui rend ce résultat garanti et pas seulement observé par chance.

## Dashboard

![Dashboard Metabase](docs/dashboard-screenshot.png)

5 KPI (Total Orders, Delivered Orders, Active Drivers, Delayed Deliveries, Total Alerts), répartition des commandes par statut, répartition des alertes par type (temps réel vs. scans périodiques), activité par livreur, et temps moyen de livraison — tous calculés depuis des vues SQL dédiées (`reporting/views/`), jamais depuis les tables internes directement.

## Limitations connues

- **Configuration Metabase non versionnée** : la connexion à la base, les questions et le dashboard vivent uniquement dans le store interne de Metabase (persistant via le volume Docker `metabase-data`). Si ce volume est perdu, la configuration doit être recréée manuellement via l'UI — ce n'est pas reproductible par `docker compose up` seul.
- **Pas d'optimisation de performance** : l'objectif du MVP est la compréhension de l'architecture, pas le débit (cf. section 1.2 du cahier des charges).
- **Un seul producer/consumer par service** : pas de scaling horizontal démontré (voir Extensions ci-dessous).

## Extensions possibles

Non implémentées dans ce MVP, mais prévues par la spécification (section 12) :

- **Outbox Pattern complet** : gestion des échecs de publication, retry avec backoff, purge périodique des lignes `published = true`.
- **Routing API / OSRM** : enrichissement des événements GPS avec distance restante et ETA, en appel asynchrone/best-effort pour ne jamais bloquer le pipeline.
- **Dead Letter Queue** : rediriger les événements invalides vers un topic dédié plutôt que de les stocker en base, pour analyse a posteriori.
- **Consumers séparés** : scinder `processing-consumer` en consumers Tracking / Analytics / Alerting distincts, chacun dans son propre Consumer Group, pour démontrer le découplage.