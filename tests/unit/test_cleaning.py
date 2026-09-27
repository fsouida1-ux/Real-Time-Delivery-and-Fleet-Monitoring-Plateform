"""
Tests unitaires — cleaning.py (section 6.1 du cahier des charges).

clean_gps_event() suppose un événement déjà validé : ces tests vérifient
la transformation de types (str -> datetime, int/str -> float), pas la
détection d'erreurs (ça, c'est le rôle de validation.py).
"""

from datetime import datetime

from cleaning import clean_gps_event


def test_types_are_normalized():
    raw_event = {
        "event_id": "gps-test-0001",
        "driver_id": 1,
        "latitude": "33.5731",   # string -> doit devenir float
        "longitude": -7.5898,
        "speed": 42,             # int -> doit devenir float
        "timestamp": "2026-09-27T10:00:00+00:00",
    }

    cleaned = clean_gps_event(raw_event)

    assert cleaned["latitude"] == 33.5731
    assert isinstance(cleaned["latitude"], float)
    assert isinstance(cleaned["speed"], float)
    assert isinstance(cleaned["recorded_at"], datetime)


def test_original_event_is_not_mutated():
    """
    clean_gps_event() doit renvoyer un NOUVEAU dict, sans modifier
    l'original — le docstring de cleaning.py l'annonce explicitement
    (utile si l'appelant réutilise l'événement brut pour le logging ou
    invalid_events).
    """
    raw_event = {
        "event_id": "gps-test-0002",
        "driver_id": 1,
        "latitude": 33.5731,
        "longitude": -7.5898,
        "speed": 42.0,
        "timestamp": "2026-09-27T10:00:00+00:00",
    }
    original_timestamp = raw_event["timestamp"]

    clean_gps_event(raw_event)

    # Le dict original garde son timestamp en string, pas en datetime
    assert raw_event["timestamp"] == original_timestamp
    assert isinstance(raw_event["timestamp"], str)