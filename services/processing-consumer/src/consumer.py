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

from delivery_shared.config import (
    get_postgres_dsn,
    get_kafka_bootstrap_servers,
    get_speed_threshold_kmh,
)
from validation import validate_gps_event
from cleaning import clean_gps_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TOPIC = "gps-events"
CONSUMER_GROUP = "delivery-processing-group"

# Lu une seule fois au démarrage du module plutôt qu'à chaque message —
# c'est une constante pour la durée de vie du process (pas besoin de
# relire l'environnement à chaque événement traité).
SPEED_THRESHOLD_KMH = get_speed_threshold_kmh()


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

        # 5. Business Rule rapide : ANOMALOUS_SPEED (section 6.1, étape 4)
        #
        # "Rapide" car elle ne dépend que de l'événement courant (cleaned),
        # sans avoir besoin d'interroger l'historique en base — contrairement
        # à DRIVER_INACTIVE / DELIVERY_DELAY (Jour 5, scan périodique) qui
        # portent sur une absence ou un délai et ne peuvent pas être évaluées
        # au passage d'un seul événement.
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

    # Les INSERT ci-dessus (processed_events, driver_positions, et
    # éventuellement alerts) ne sont réellement gravés qu'ici, ensemble.
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