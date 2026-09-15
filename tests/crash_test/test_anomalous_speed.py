"""
Script de test ponctuel — Jour 4, bloc 3/3.

Rôle : publier un seul événement GPS avec une vitesse au-dessus du seuil
ANOMALOUS_SPEED (120 km/h par défaut) directement dans le topic gps-events,
pour vérifier que le Processing Consumer insère bien une ligne dans `alerts`.

Usage :
    python test_anomalous_speed.py

Pré-requis : le Processing Consumer doit être en cours d'exécution dans un
autre terminal pour consommer et traiter cet événement.
"""

import json
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from delivery_shared.config import get_kafka_bootstrap_servers

TOPIC = "gps-events"
DRIVER_ID = 3        # doit exister dans la table drivers (FK)
TEST_SPEED = 180.0      # au-dessus du seuil de 120 km/h


def main():
    producer = KafkaProducer(
        bootstrap_servers=get_kafka_bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )

    event = {
        "event_id": f"gps-TEST-ANOMALOUS-{uuid.uuid4().hex[:8]}",
        "event_type": "GPS_UPDATE",
        "driver_id": DRIVER_ID,
        "latitude": 33.5731,
        "longitude": -7.5898,
        "speed": TEST_SPEED,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Clé = driver_id, comme le fait le vrai GPS Producer (section 5.3)
    producer.send(TOPIC, key=DRIVER_ID, value=event)
    producer.flush()

    print(f"Événement publié: {event['event_id']} (speed={TEST_SPEED} km/h)")


if __name__ == "__main__":
    main()