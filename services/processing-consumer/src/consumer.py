"""
Processing Consumer — robustesse (Jour 4) + orders-events (Jour 5/6, reprise)

Rôle : consommer gps-events ET orders-events, valider, dédupliquer de façon
PERSISTANTE (table processed_events + contrainte UNIQUE), nettoyer les types,
écrire dans PostgreSQL de façon transactionnelle (section 6.3), puis committer
manuellement l'offset Kafka.

orders-events : traitement volontairement minimal. orders (table
transactionnelle) reste la SEULE source de vérité sur le statut d'une
commande — le trigger PostgreSQL l'a déjà mise à jour avant même que le
message Kafka ne soit publié. Le rôle du Consumer ici n'est donc pas
d'écrire une donnée manquante (contrairement à gps-events), mais de
prouver que le pipeline consomme, déduplique et committe correctement.
"""

import json
import logging

from kafka import KafkaConsumer
import psycopg2

from delivery_shared.config import (
    get_postgres_dsn,
    get_kafka_bootstrap_servers,
    get_speed_threshold_kmh,
)
from validation import validate_gps_event
from cleaning import clean_gps_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GPS_TOPIC = "gps-events"
ORDERS_TOPIC = "orders-events"
CONSUMER_GROUP = "delivery-processing-group"

SPEED_THRESHOLD_KMH = get_speed_threshold_kmh()


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        GPS_TOPIC,
        ORDERS_TOPIC,
        bootstrap_servers=get_kafka_bootstrap_servers(),
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def process_gps_event(conn, event: dict) -> bool:
    """Pipeline complet pour un événement GPS (section 6.1). Inchangé."""
    is_valid, reason = validate_gps_event(event)

    if not is_valid:
        logger.warning("Événement GPS invalide rejeté: %s (%s)", event.get("event_id"), reason)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO invalid_events (event_id, topic, raw_payload, reason)
                VALUES (%s, %s, %s, %s)
                """,
                (event.get("event_id"), GPS_TOPIC, json.dumps(event), reason),
            )
        conn.commit()
        return True

    event_id = event["event_id"]
    cleaned = clean_gps_event(event)

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

        if cleaned["speed"] > SPEED_THRESHOLD_KMH:
            logger.warning(
                "ANOMALOUS_SPEED détectée: driver_id=%s speed=%.2f (seuil=%.0f)",
                cleaned["driver_id"], cleaned["speed"], SPEED_THRESHOLD_KMH,
            )
            cur.execute(
                """
                INSERT INTO alerts (alert_type, driver_id, details)
                VALUES (%s, %s, %s)
                """,
                (
                    "ANOMALOUS_SPEED",
                    cleaned["driver_id"],
                    json.dumps({
                        "speed": cleaned["speed"],
                        "threshold": SPEED_THRESHOLD_KMH,
                        "event_id": event_id,
                    }),
                ),
            )

    conn.commit()
    return True


def process_order_event(conn, event: dict) -> bool:
    """
    Traitement d'un événement orders-events.

    Volontairement minimal : orders reste la seule source de vérité, déjà
    à jour via le trigger PostgreSQL au moment où ce message est publié.
    On se contente de prouver que le pipeline consomme et déduplique
    correctement — pas de réécriture ni de revalidation du statut, ce qui
    créerait une double source de vérité (anti-pattern).
    """
    event_id = event.get("event_id")

    if not event_id:
        logger.error("Événement orders-events malformé (event_id manquant), ignoré: %s", event)
        return True

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO processed_events (event_id, event_type)
            VALUES (%s, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (event_id, event.get("event_type", "ORDER_UNKNOWN")),
        )
        inserted = cur.fetchone()

        if inserted is None:
            logger.warning("Doublon détecté (processed_events), ignoré: %s", event_id)
            conn.rollback()
            return True

    conn.commit()
    logger.info(
        "Événement order traité: order_id=%s type=%s status=%s",
        event.get("order_id"), event.get("event_type"), event.get("status"),
    )
    return True


def process_message(conn, topic: str, event: dict) -> bool:
    """Dispatch selon le topic Kafka — métadonnée fiable, pas déduite du contenu."""
    if topic == GPS_TOPIC:
        return process_gps_event(conn, event)
    elif topic == ORDERS_TOPIC:
        return process_order_event(conn, event)
    else:
        logger.warning("Topic inconnu, événement ignoré: %s (%s)", topic, event)
        return True


def main():
    consumer = build_consumer()
    conn = psycopg2.connect(get_postgres_dsn())

    logger.info("Processing Consumer démarré, écoute sur '%s' et '%s'...", GPS_TOPIC, ORDERS_TOPIC)

    try:
        for message in consumer:
            event = message.value
            # DEBUG plutôt que INFO : utile en investigation ponctuelle,
            # mais trop verbeux pour tourner en continu sur un flux GPS
            # haute fréquence (section 4 du cahier des charges).
            logger.debug("Message reçu [%s]: %s", message.topic, event)

            success = process_message(conn, message.topic, event)

            if success:
                consumer.commit()
                logger.info("Offset committé (%s, event_id=%s)", message.topic, event.get("event_id"))
    except KeyboardInterrupt:
        logger.info("Arrêt demandé (Ctrl+C).")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()