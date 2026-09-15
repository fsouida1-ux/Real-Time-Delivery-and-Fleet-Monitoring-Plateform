"""
Processing Consumer — robustesse (Jour 4, bloc 2/3)

Rôle : consommer gps-events, valider, dédupliquer de façon PERSISTANTE
(table processed_events + contrainte UNIQUE), nettoyer les types, écrire
dans PostgreSQL de façon transactionnelle (section 6.3), puis committer
manuellement l'offset Kafka.

Ce qui N'EST PAS encore fait ici :
- règle ANOMALOUS_SPEED (bloc 3/3 du Jour 4)
- test de crash formel (section 13)
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


def process_message(conn, event: dict) -> bool:
    """
    Pipeline complet (section 6.1) :
    Validation -> Déduplication persistante + Écriture (même transaction) -> (commit fait par l'appelant).

    Note : le paramètre `seen_event_ids` a disparu. La dédup ne vit plus dans
    la mémoire du processus, elle vit dans PostgreSQL (table processed_events),
    donc elle survit à un redémarrage ou un crash du Consumer.
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

    event_id = event["event_id"]

    # 2. Cleaning (normalisation des types) — fait AVANT la transaction,
    # c'est un calcul pur, pas la peine de le faire tenir dans le bloc
    # transactionnel qui doit rester le plus court possible.
    cleaned = clean_gps_event(event)

    # 3. Déduplication + 4. Écriture PostgreSQL — MÊME TRANSACTION (section 6.3)
    #
    # Pourquoi une seule transaction : si on committait processed_events
    # séparément, un crash juste après committerait "traité" sans que la
    # donnée métier existe -> perte silencieuse et définitive (l'événement
    # ne serait jamais rejoué car déjà marqué comme traité).
    # En gardant tout dans le même bloc, soit les deux écritures passent
    # ensemble, soit un rollback annule tout, et l'événement pourra être
    # rejoué au prochain passage du Consumer.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO processed_events (event_id, event_type)
            VALUES (%s, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (event_id, event.get("event_type", "GPS_UPDATE")),
        )
        inserted = cur.fetchone()

        if inserted is None:
            # event_id déjà présent dans processed_events -> doublon réel,
            # confirmé par PostgreSQL, pas par une mémoire volatile.
            logger.warning("Doublon détecté (processed_events), ignoré: %s", event_id)
            conn.rollback()
            return True

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

    # Les deux INSERT ci-dessus ne sont réellement gravés qu'ici, ensemble.
    conn.commit()
    return True


def main():
    consumer = build_consumer()
    conn = psycopg2.connect(get_postgres_dsn())

    logger.info("Processing Consumer démarré, écoute sur '%s'...", TOPIC)

    for message in consumer:
        event = message.value
        logger.info("Message reçu: %s", event)

        success = process_message(conn, event)

        if success:
            consumer.commit()
            logger.info("Offset committé pour driver_id=%s", event.get("driver_id"))


if __name__ == "__main__":
    main()