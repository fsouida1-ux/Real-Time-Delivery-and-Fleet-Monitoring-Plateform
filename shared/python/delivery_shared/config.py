"""
Configuration partagée, chargée depuis les variables d'environnement (.env).

Pourquoi ce module existe :
Plusieurs services (db-producer, gps-producer, ...) ont besoin des mêmes
informations de connexion (Postgres, Kafka). Centraliser leur lecture ici
évite que chaque service réinvente sa propre logique de parsing, avec le
risque de divergence (ex: un nom de variable différent d'un service à l'autre).
"""

import os

from dotenv import load_dotenv

# Charge le fichier .env s'il existe (no-op si les variables sont déjà
# injectées autrement, par exemple via docker-compose `environment:`).
load_dotenv()


def get_postgres_dsn() -> str:
    """
    Construit la chaîne de connexion PostgreSQL (DSN) à partir des variables
    d'environnement.

    Un DSN (Data Source Name) est simplement l'URL de connexion qu'attend
    psycopg2, au format : postgresql://user:password@host:port/dbname
    """
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    # En dehors de Docker (tests locaux), on retombe sur localhost.
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def get_kafka_bootstrap_servers() -> str:
    """
    Retourne l'adresse du broker Kafka.

    Attention : à l'intérieur de Docker Compose, cette valeur doit être
    kafka:19092 (le listener interne). En local (hors conteneur), ce sera
    localhost:9092 (le listener externe défini au Jour 1).
    """
    return os.environ["KAFKA_BOOTSTRAP_SERVERS"]


def get_speed_threshold_kmh() -> float:
    """
    Retourne le seuil de vitesse (km/h) au-delà duquel un événement GPS
    déclenche une alerte ANOMALOUS_SPEED (section 6.1 / 7.1 du cahier des
    charges).

    Valeur par défaut de secours (120) alignée sur .env (annexe 15.3), au
    cas où la variable ne serait pas définie dans l'environnement.
    """
    return float(os.getenv("GPS_SPEED_THRESHOLD_KMH", "120"))