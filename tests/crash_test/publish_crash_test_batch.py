"""
Script de test — Jour 4, test de crash obligatoire (section 13, étape 1).

Rôle : publier un lot de N événements GPS valides d'un coup dans
gps-events, pour ensuite tuer le Processing Consumer en plein traitement
et vérifier la robustesse du mécanisme d'idempotence.

Usage :
    python publish_crash_test_batch.py
"""

import json
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from delivery_shared.config import get_kafka_bootstrap_servers

TOPIC = "gps-events"
DRIVER_ID = 3          # utilise un driver_id que tu sais exister (FK)
BATCH_SIZE = 60         # > 50 comme demandé par la section 13


def main():
    producer = KafkaProducer(
        bootstrap_servers=get_kafka_bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )

    batch_tag = uuid.uuid4().hex[:6]  # pour identifier facilement ce lot en base

    for i in range(BATCH_SIZE):
        event = {
            "event_id": f"gps-CRASHTEST-{batch_tag}-{i:03d}",
            "event_type": "GPS_UPDATE",
            "driver_id": DRIVER_ID,
            "latitude": 33.5731 + (i * 0.0001),
            "longitude": -7.5898 + (i * 0.0001),
            "speed": 40.0,  # vitesse normale, on ne teste pas ANOMALOUS_SPEED ici
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(TOPIC, key=DRIVER_ID, value=event)

    producer.flush()
    print(f"Lot publié: {BATCH_SIZE} événements, tag='{batch_tag}'")
    print(f"Requête de vérification: WHERE event_id LIKE 'gps-CRASHTEST-{batch_tag}-%'")


if __name__ == "__main__":
    main()