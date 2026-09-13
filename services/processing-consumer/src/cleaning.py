"""
Cleaning des événements GPS — section 6.1 du cahier des charges.

Normalise les types bruts (JSON) en types Python propres, prêts pour
l'insertion PostgreSQL. Suppose que l'événement a DÉJÀ été validé
(validation.py) — cette fonction ne revalide rien, elle transforme.
"""

from datetime import datetime


def clean_gps_event(event: dict) -> dict:
    """
    Retourne un nouveau dict avec les types normalisés :
    - timestamp : str ISO 8601 -> datetime
    - latitude / longitude / speed : -> float explicite

    Ne modifie pas le dict original (event) pour éviter les effets de bord
    surprenants si l'appelant réutilise l'événement brut ailleurs (ex: pour
    le logging ou l'écriture dans invalid_events).
    """
    return {
        "event_id": event["event_id"],
        "driver_id": event["driver_id"],
        "latitude": float(event["latitude"]),
        "longitude": float(event["longitude"]),
        "speed": float(event["speed"]),
        "recorded_at": datetime.fromisoformat(event["timestamp"]),
    }