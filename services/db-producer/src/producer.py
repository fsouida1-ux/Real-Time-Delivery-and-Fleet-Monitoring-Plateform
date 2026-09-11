"""
DB Producer — section 5.1 du cahier des charges.

Rôle : faire le pont entre PostgreSQL (table outbox_events, alimentée par
le trigger du Jour 1) et Kafka (topic orders-events).

Boucle :
    1. Lire les lignes non publiées de outbox_events.
    2. Publier chacune sur Kafka avec key = order_id.
    3. Marquer la ligne published = true SEULEMENT après confirmation Kafka.
"""

import json
import logging
import time

import psycopg2
import psycopg2.extras
from kafka import KafkaProducer

from delivery_shared.config import get_kafka_bootstrap_servers, get_postgres_dsn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [db-producer] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

TOPIC = "orders-events"
POLL_INTERVAL_SECONDS = 2

# Requête au coeur du polling : on s'appuie sur l'index partiel
# idx_outbox_events_unpublished créé au Jour 1 (002_indexes.sql), qui ne
# couvre justement que les lignes published = false. FOR UPDATE SKIP LOCKED
# n'est pas nécessaire ici (un seul producer pour le MVP), mais on le
# mentionne comme piste si on scale à plusieurs instances plus tard.
SELECT_UNPUBLISHED_SQL = """
    SELECT id, aggregate_id, event_type, payload
    FROM outbox_events
    WHERE published = false
    ORDER BY id
    LIMIT 100;
"""

MARK_PUBLISHED_SQL = """
    UPDATE outbox_events SET published = true WHERE id = %s;
"""


def build_kafka_producer() -> KafkaProducer:
    """
    Crée le client Kafka producer.

    value_serializer : convertit automatiquement le dict Python en JSON
    encodé UTF-8 avant envoi, pour ne pas le refaire à chaque appel de send().

    key_serializer : la clé Kafka (order_id) doit aussi être envoyée en
    bytes ; on l'encode simplement en string UTF-8.
    """
    return KafkaProducer(
        bootstrap_servers=get_kafka_bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )


def poll_and_publish(conn, producer: KafkaProducer) -> int:
    """
    Exécute un cycle de polling complet. Retourne le nombre d'événements publiés.
    """
    published_count = 0

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SELECT_UNPUBLISHED_SQL)
        rows = cur.fetchall()

        for row in rows:
            # Le payload publié sur Kafka. On y intègre event_type et
            # aggregate_id (= order_id) en plus du contenu déjà présent
            # dans la colonne JSONB payload, pour que le message soit
            # auto-suffisant (le consumer n'a pas besoin de retourner en DB
            # pour savoir de quel type d'événement il s'agit).
            message = {
                "event_type": row["event_type"],
                "order_id": row["aggregate_id"],
                **row["payload"],
            }

            try:
                # .send() est asynchrone : il renvoie immédiatement un
                # "future". .get(timeout=...) le rend synchrone et bloque
                # jusqu'à confirmation (ou exception) du broker Kafka.
                # C'est cette confirmation qui nous autorise à marquer
                # published = true juste après.
                future = producer.send(
                    TOPIC, key=row["aggregate_id"], value=message
                )
                future.get(timeout=10)
            except Exception:
                logger.exception(
                    "Échec de publication pour outbox_events.id=%s — "
                    "la ligne reste published=false, elle sera retentée "
                    "au prochain cycle.",
                    row["id"],
                )
                # On ne propage pas l'exception : un échec sur une ligne
                # ne doit pas bloquer le traitement des suivantes.
                continue

            with conn.cursor() as update_cur:
                update_cur.execute(MARK_PUBLISHED_SQL, (row["id"],))
            conn.commit()

            published_count += 1
            logger.info(
                "Publié: outbox_events.id=%s -> orders-events (key=%s, event_type=%s)",
                row["id"],
                row["aggregate_id"],
                row["event_type"],
            )

    return published_count


def main() -> None:
    logger.info("Démarrage du DB Producer...")

    conn = psycopg2.connect(get_postgres_dsn())
    producer = build_kafka_producer()

    logger.info("Connecté à Postgres et Kafka. Début du polling (intervalle=%ss).", POLL_INTERVAL_SECONDS)

    try:
        while True:
            count = poll_and_publish(conn, producer)
            if count:
                logger.info("Cycle terminé : %s événement(s) publié(s).", count)
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Arrêt demandé (Ctrl+C).")
    finally:
        producer.close()
        conn.close()


if __name__ == "__main__":
    main()