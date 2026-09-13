"""
Processing Consumer — chemin heureux (Jour 3)

Rôle : consommer gps-events, valider, dédupliquer (en mémoire, limitation
connue), nettoyer les types, écrire dans PostgreSQL, puis committer
manuellement l'offset Kafka.

Ce qui N'EST PAS encore fait ici (arrive au Jour 4) :
- déduplication PERSISTANTE via processed_events (contrainte UNIQUE)
- écriture idempotente transactionnelle
- règle ANOMALOUS_SPEED
"""

import json
import logging

from kafka import KafkaConsumer
import psycopg2

from delivery_shared.config import get_postgres_dsn, get_kafka_bootstrap_servers
from validation import validate_gps_event
from cleaning import clean_gps_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TOPIC = "gps-events"
CONSUMER_GROUP = "delivery-processing-group"


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=get_kafka_bootstrap_servers(),
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def process_message(conn, event: dict, seen_event_ids: set[str]) -> bool:
    """
    Pipeline complet (section 6.1) :
    Validation -> Déduplication -> Cleaning -> Écriture -> (commit fait par l'appelant).
    """
    # 1. Validation (section 6.2)
    is_valid, reason = validate_gps_event(event)

    if not is_valid:
        logger.warning("Événement invalide rejeté: %s (%s)", event.get("event_id"), reason)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO invalid_events (event_id, topic, raw_payload, reason)
                VALUES (%s, %s, %s, %s)
                """,
                (event.get("event_id"), TOPIC, json.dumps(event), reason),
            )
        conn.commit()
        return True

    # 2. Déduplication (en mémoire, limitation connue)
    event_id = event["event_id"]
    if event_id in seen_event_ids:
        logger.warning("Doublon détecté (mémoire), ignoré: %s", event_id)
        return True

    # 3. Cleaning (normalisation des types)
    cleaned = clean_gps_event(event)

    # 4. Écriture PostgreSQL
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO driver_positions (driver_id, latitude, longitude, speed, recorded_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                cleaned["driver_id"],
                cleaned["latitude"],
                cleaned["longitude"],
                cleaned["speed"],
                cleaned["recorded_at"],
            ),
        )
    conn.commit()

    seen_event_ids.add(event_id)
    return True


def main():
    consumer = build_consumer()
    conn = psycopg2.connect(get_postgres_dsn())

    seen_event_ids: set[str] = set()

    logger.info("Processing Consumer démarré, écoute sur '%s'...", TOPIC)

    for message in consumer:
        event = message.value
        logger.info("Message reçu: %s", event)

        success = process_message(conn, event, seen_event_ids)

        if success:
            consumer.commit()
            logger.info("Offset committé pour driver_id=%s", event.get("driver_id"))


if __name__ == "__main__":
    main()