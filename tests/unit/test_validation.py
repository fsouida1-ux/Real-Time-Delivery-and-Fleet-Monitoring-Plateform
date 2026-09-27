"""
Tests unitaires — validation.py (section 6.2 du cahier des charges).

Contrairement aux scripts de tests/manual/, ces tests ne dépendent ni de
Kafka ni de PostgreSQL : validate_gps_event() est une fonction pure, donc
testable entièrement en mémoire, avec des assertions automatiques.

Lancer : pytest tests/unit/test_validation.py -v
"""

from datetime import datetime, timedelta, timezone

from validation import validate_gps_event


def _valid_event(**overrides) -> dict:
    """
    Construit un événement GPS valide de référence, avec la possibilité
    de surcharger un seul champ par test (**overrides). Évite de répéter
    les 6 champs dans chaque test alors qu'on ne veut en casser qu'un.
    """
    base = {
        "event_id": "gps-test-0001",
        "driver_id": 1,
        "latitude": 33.5731,
        "longitude": -7.5898,
        "speed": 42.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    base.update(overrides)
    return base


def test_valid_event_passes():
    is_valid, reason = validate_gps_event(_valid_event())
    assert is_valid is True
    assert reason is None


def test_missing_event_id_rejected():
    is_valid, reason = validate_gps_event(_valid_event(event_id=""))
    assert is_valid is False
    assert "event_id" in reason


def test_negative_driver_id_rejected():
    is_valid, reason = validate_gps_event(_valid_event(driver_id=-1))
    assert is_valid is False
    assert "driver_id" in reason


def test_non_integer_driver_id_rejected():
    is_valid, reason = validate_gps_event(_valid_event(driver_id="abc"))
    assert is_valid is False
    assert "driver_id" in reason


def test_latitude_out_of_range_rejected():
    is_valid, reason = validate_gps_event(_valid_event(latitude=91.0))
    assert is_valid is False
    assert "latitude" in reason


def test_longitude_out_of_range_rejected():
    is_valid, reason = validate_gps_event(_valid_event(longitude=-181.0))
    assert is_valid is False
    assert "longitude" in reason


def test_negative_speed_rejected():
    is_valid, reason = validate_gps_event(_valid_event(speed=-5.0))
    assert is_valid is False
    assert "speed" in reason


def test_speed_above_250_rejected():
    is_valid, reason = validate_gps_event(_valid_event(speed=251.0))
    assert is_valid is False
    assert "speed" in reason


def test_unparseable_timestamp_rejected():
    is_valid, reason = validate_gps_event(_valid_event(timestamp="pas-une-date"))
    assert is_valid is False
    assert "timestamp" in reason


def test_future_timestamp_rejected():
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    is_valid, reason = validate_gps_event(_valid_event(timestamp=future))
    assert is_valid is False
    assert "futur" in reason