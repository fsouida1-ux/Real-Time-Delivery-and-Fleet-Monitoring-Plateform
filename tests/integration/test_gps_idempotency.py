"""
Test d'intégration — idempotence de process_gps_event() (section 6.3).

Contrairement à tests/unit/, ce test a besoin d'une vraie connexion
PostgreSQL : il vérifie que la contrainte UNIQUE(event_id) sur
processed_events, combinée à la transaction unique de la section 6.3,
empêche réellement un doublon en base — pas seulement en théorie.

Prérequis : le conteneur delivery-postgres doit tourner
(docker compose up postgres), et driver_id=1 doit exister (seed du Jour 1).

Lancer : pytest tests/integration/test_gps_idempotency.py -v
"""

import uuid
from datetime import datetime, timezone

import psycopg2
import pytest

from delivery_shared.config import get_postgres_dsn
from consumer import process_gps_event

DRIVER_ID = 1  # doit exister dans la table drivers (FK)


@pytest.fixture
def db_conn():
    """
    Connexion PostgreSQL réutilisée pour tout le test, avec nettoyage
    automatique à la fin (yield + code après = teardown, exécuté même
    si le test échoue).

    On ne peut pas se contenter d'un rollback global : le code testé
    committe lui-même en interne. Le nettoyage se fait donc par
    suppression explicite des lignes créées par CE test.
    """
    conn = psycopg2.connect(get_postgres_dsn())
    yield conn

    conn.rollback()  # annule toute transaction restée ouverte par erreur
    conn.close()


def test_duplicate_gps_event_creates_no_duplicate_row(db_conn):
    """
    Rejoue exactement le scénario de tests/manual/test_duplicate.py,
    mais avec des assertions automatiques plutôt qu'une vérification
    manuelle des logs.
    """
    event_id = f"gps-test-idempotency-{uuid.uuid4().hex[:8]}"
    event_timestamp = datetime.now(timezone.utc).isoformat()
    event = {
        "event_id": event_id,
        "event_type": "GPS_UPDATE",
        "driver_id": DRIVER_ID,
        "latitude": 33.5731,
        "longitude": -7.5898,
        "speed": 42.0,
        "timestamp": event_timestamp,
    }

    try:
        # 1er traitement : doit réussir et écrire une position.
        result_1 = process_gps_event(db_conn, event)
        assert result_1 is True

        # 2e traitement du MÊME event_id : doit être détecté comme
        # doublon (ON CONFLICT DO NOTHING) et ne rien écrire de plus.
        result_2 = process_gps_event(db_conn, event)
        assert result_2 is True  # le doublon n'est pas une erreur, juste ignoré

        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            assert cur.fetchone()[0] == 1

            # Identification par recorded_at (précision microseconde,
            # dérivé du timestamp ISO 8601 unique à ce test) plutôt que
            # par speed : gps-simulator tourne en continu sur cette même
            # base et peut, par coïncidence sur des milliers d'événements,
            # générer la même valeur de speed arrondie.
            cur.execute(
                """
                SELECT COUNT(*) FROM driver_positions
                WHERE driver_id = %s AND recorded_at = %s
                """,
                (DRIVER_ID, event_timestamp),
            )
            assert cur.fetchone()[0] == 1

    finally:
        with db_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM processed_events WHERE event_id = %s", (event_id,)
            )
            cur.execute(
                "DELETE FROM driver_positions WHERE driver_id = %s AND recorded_at = %s",
                (DRIVER_ID, event_timestamp),
            )
        db_conn.commit()