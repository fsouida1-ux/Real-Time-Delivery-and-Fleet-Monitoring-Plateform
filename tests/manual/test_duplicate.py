"""
Script de test ponctuel — envoie 2x le même event_id sur gps-events
pour vérifier que la déduplication en mémoire du Processing Consumer
fonctionne (Jour 3, section 6.1).

Usage : python test_duplicate.py
"""

import json
from kafka import KafkaProducer

from delivery_shared.config import get_kafka_bootstrap_servers

producer = KafkaProducer(
    bootstrap_servers=get_kafka_bootstrap_servers(),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

duplicate_event = {
    "event_id": "gps-TEST-DUPLICATE-001",
    "event_type": "GPS_UPDATE",
    "driver_id": 1,
    "latitude": 33.5731,
    "longitude": -7.5898,
    "speed": 42.0,
    "timestamp": "2026-09-13T19:35:22.000000+00:00",
}

print("Envoi du 1er exemplaire...")
producer.send("gps-events", key=str(duplicate_event["driver_id"]), value=duplicate_event)
producer.flush()

print("Envoi du 2e exemplaire (identique)...")
producer.send("gps-events", key=str(duplicate_event["driver_id"]), value=duplicate_event)
producer.flush()

print("Terminé. Vérifie les logs du Processing Consumer.")