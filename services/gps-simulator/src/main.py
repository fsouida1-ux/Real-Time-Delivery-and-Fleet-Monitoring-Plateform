"""
GPS Simulator + GPS Producer — sections 5.2 et 5.3 du cahier des charges.

Choix d'architecture : un seul processus. Le simulateur génère une position
et appelle directement la fonction de publication Kafka, sans file d'attente
intermédiaire. La section 5.3 du cahier des charges autorise explicitement
"l'appel direct de fonction" comme mécanisme de communication — c'est le bon
niveau de complexité pour un MVP dont l'objectif est la compréhension, pas
le débit (section 1.2).

Le code reste organisé en deux responsabilités distinctes (génération vs
publication), même si les deux tournent dans le même processus : si on
décide plus tard de les séparer en deux services Docker, ce sera surtout
un travail de découpage de fichiers, pas de réécriture de logique.
"""

import json
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from kafka import KafkaProducer

from delivery_shared.config import get_kafka_bootstrap_servers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [gps-simulator] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

TOPIC = "gps-events"
TICK_SECONDS = 1  # une position générée par livreur toutes les 1 seconde

# Probabilité d'injecter un événement invalide (section 5.2 : 2-5%).
INVALID_EVENT_PROBABILITY = 0.03

# Bornes de validation attendues par le futur Processing Consumer
# (section 6.2) — on s'en sert ici pour rester dans les clous en usage
# normal, et pour savoir quelles bornes violer volontairement.
LATITUDE_RANGE = (-90.0, 90.0)
LONGITUDE_RANGE = (-180.0, 180.0)
SPEED_RANGE = (0.0, 250.0)


# ---------------------------------------------------------------------------
# Modèle des livreurs simulés
# ---------------------------------------------------------------------------

@dataclass
class DriverProfile:
    """
    Représente l'état courant d'un livreur simulé.

    latitude/longitude : position courante, mise à jour à chaque tick par une
    petite marche aléatoire (random walk) pour simuler un déplacement réaliste.
    """
    driver_id: int
    profile_type: str  # "normal" | "speed_spikes" | "goes_inactive"
    latitude: float
    longitude: float
    active: bool = True
    ticks_elapsed: int = 0
    inactive_after_ticks: int = field(default=0)

    def step_position(self) -> None:
        """Fait légèrement bouger le livreur (simule un déplacement réel)."""
        self.latitude += random.uniform(-0.001, 0.001)
        self.longitude += random.uniform(-0.001, 0.001)

    def compute_speed(self) -> float:
        """
        Calcule la vitesse de ce tick selon le profil du livreur.

        - normal : vitesse réaliste de livraison urbaine.
        - speed_spikes : vitesse normale la plupart du temps, avec un pic
          anormal (>120 km/h, cf. GPS_SPEED_THRESHOLD_KMH) environ 1 fois
          sur 10, pour exercer la règle ANOMALOUS_SPEED plus tard.
        - goes_inactive : vitesse normale tant qu'il émet encore.
        """
        if self.profile_type == "speed_spikes" and random.random() < 0.1:
            return round(random.uniform(130, 180), 2)
        return round(random.uniform(20, 60), 2)


def build_driver_profiles() -> list[DriverProfile]:
    """
    Construit les 3 profils demandés par la section 5.2. Les driver_id
    correspondent aux 4 livreurs déjà créés par le seed du Jour 1
    (001_tables.sql / 004_seed.sql) — on en utilise 3 sur les 4.

    Point de départ géographique : Casablanca, pour rester dans une zone
    cohérente (coordonnées arbitraires mais réalistes).
    """
    return [
        DriverProfile(driver_id=1, profile_type="normal", latitude=33.5731, longitude=-7.5898),
        DriverProfile(driver_id=2, profile_type="speed_spikes", latitude=33.5731, longitude=-7.5898),
        DriverProfile(
            driver_id=3,
            profile_type="goes_inactive",
            latitude=33.5731,
            longitude=-7.5898,
            # Arrête d'émettre après ~30 ticks (30 secondes avec TICK_SECONDS=1),
            # pour simuler un livreur qui "disparaît" en pleine course.
            inactive_after_ticks=30,
        ),
    ]


# ---------------------------------------------------------------------------
# Génération d'un événement GPS (valide ou volontairement invalide)
# ---------------------------------------------------------------------------

def build_valid_event(driver: DriverProfile) -> dict:
    """Construit un événement GPS conforme aux règles de la section 6.2."""
    return {
        "event_id": f"gps-{uuid.uuid4().hex[:12]}",
        "event_type": "GPS_UPDATE",
        "driver_id": driver.driver_id,
        "latitude": round(driver.latitude, 6),
        "longitude": round(driver.longitude, 6),
        "speed": driver.compute_speed(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def corrupt_event(event: dict) -> dict:
    """
    Corrompt volontairement un événement pour exercer la validation du
    futur Processing Consumer (section 5.2 : 2-5% d'événements invalides).

    On choisit aléatoirement un seul type de défaut par événement corrompu,
    plutôt que de tout casser à la fois — plus proche d'une vraie anomalie
    de capteur GPS.
    """
    defect = random.choice(["latitude_out_of_range", "negative_speed"])

    if defect == "latitude_out_of_range":
        # Hors bornes [-90, 90] (section 6.2).
        event["latitude"] = round(random.uniform(91, 120), 6)
    elif defect == "negative_speed":
        event["speed"] = round(random.uniform(-50, -1), 2)

    return event


# ---------------------------------------------------------------------------
# Publication Kafka (rôle du "GPS Producer", section 5.3)
# ---------------------------------------------------------------------------

def build_kafka_producer() -> KafkaProducer:
    """
    Crée le client Kafka. Même logique de sérialisation que le DB Producer :
    JSON encodé en UTF-8 pour la valeur, clé encodée en UTF-8 pour driver_id.
    """
    return KafkaProducer(
        bootstrap_servers=get_kafka_bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )


def publish_event(producer: KafkaProducer, event: dict) -> None:
    """
    Publie un événement GPS sur le topic gps-events, avec key = driver_id
    (garantit l'ordre des positions d'un même livreur sur la même partition,
    section 4).
    """
    future = producer.send(TOPIC, key=event["driver_id"], value=event)
    future.get(timeout=10)


# ---------------------------------------------------------------------------
# Boucle principale
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Démarrage du GPS Simulator...")

    producer = build_kafka_producer()
    drivers = build_driver_profiles()

    logger.info(
        "Connecté à Kafka. Simulation de %d livreurs (profils: %s).",
        len(drivers),
        ", ".join(f"{d.driver_id}={d.profile_type}" for d in drivers),
    )

    try:
        while True:
            for driver in drivers:
                if not driver.active:
                    continue

                driver.ticks_elapsed += 1

                # Profil "goes_inactive" : on arrête définitivement d'émettre
                # une fois le seuil de ticks atteint, pour simuler un
                # livreur qui disparaît en pleine course.
                if (
                    driver.profile_type == "goes_inactive"
                    and driver.ticks_elapsed >= driver.inactive_after_ticks
                ):
                    driver.active = False
                    logger.info(
                        "Driver %s (profil=%s) cesse d'émettre (simulation DRIVER_INACTIVE).",
                        driver.driver_id,
                        driver.profile_type,
                    )
                    continue

                driver.step_position()
                event = build_valid_event(driver)

                is_invalid = random.random() < INVALID_EVENT_PROBABILITY
                if is_invalid:
                    event = corrupt_event(event)

                try:
                    publish_event(producer, event)
                except Exception:
                    logger.exception(
                        "Échec de publication pour driver_id=%s (event_id=%s).",
                        driver.driver_id,
                        event["event_id"],
                    )
                    continue

                tag = " [INVALIDE]" if is_invalid else ""
                logger.info(
                    "Publié%s: driver_id=%s speed=%.2f -> gps-events",
                    tag,
                    driver.driver_id,
                    event["speed"],
                )

            time.sleep(TICK_SECONDS)
    except KeyboardInterrupt:
        logger.info("Arrêt demandé (Ctrl+C).")
    finally:
        producer.close()


if __name__ == "__main__":
    main()