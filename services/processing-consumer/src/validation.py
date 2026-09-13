"""
Validation des événements GPS — section 6.2 du cahier des charges.

Règles :
- event_id : présent, non vide
- driver_id : présent, entier positif
- latitude : entre -90 et 90
- longitude : entre -180 et 180
- speed : entre 0 et 250 (km/h)
- timestamp : parseable ISO 8601, pas dans le futur
"""

from datetime import datetime, timezone


def validate_gps_event(event: dict) -> tuple[bool, str | None]:
    """
    Valide un événement GPS selon les règles de la section 6.2.

    Retourne (True, None) si l'événement est valide,
    ou (False, "raison lisible") s'il est invalide.

    On s'arrête à la première règle violée (pas besoin de toutes les
    vérifier si la première échoue déjà).
    """
    event_id = event.get("event_id")
    if not event_id:
        return False, "event_id manquant ou vide"

    driver_id = event.get("driver_id")
    if not isinstance(driver_id, int) or driver_id <= 0:
        return False, f"driver_id invalide: {driver_id!r}"

    latitude = event.get("latitude")
    if not isinstance(latitude, (int, float)) or not (-90 <= latitude <= 90):
        return False, f"latitude hors bornes: {latitude!r}"

    longitude = event.get("longitude")
    if not isinstance(longitude, (int, float)) or not (-180 <= longitude <= 180):
        return False, f"longitude hors bornes: {longitude!r}"

    speed = event.get("speed")
    if not isinstance(speed, (int, float)) or not (0 <= speed <= 250):
        return False, f"speed hors bornes: {speed!r}"

    timestamp_str = event.get("timestamp")
    try:
        event_time = datetime.fromisoformat(timestamp_str)
    except (TypeError, ValueError):
        return False, f"timestamp non parseable: {timestamp_str!r}"

    if event_time > datetime.now(timezone.utc):
        return False, f"timestamp dans le futur: {timestamp_str!r}"

    return True, None