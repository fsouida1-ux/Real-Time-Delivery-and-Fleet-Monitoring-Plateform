"""
Order Simulator — fait avancer des commandes à travers leurs statuts
pour exercer le trigger PostgreSQL (outbox_events) et, en aval,
le DB Producer déjà en place (section 5.1).

Contrairement à gps-simulator, ce service ne publie JAMAIS dans Kafka
directement : il se contente de modifier orders.status. C'est le trigger
PostgreSQL (010_create_orders_outbox_trigger.sql) qui écrit dans
outbox_events, et db-producer qui s'occupe de la publication.
"""

import logging
import random
import time
from dataclasses import dataclass, field

import psycopg2

from delivery_shared.config import get_postgres_dsn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [order-simulator] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

TICK_SECONDS = 3  # une vérification/action toutes les 3 secondes

# Statuts dans l'ordre du chemin nominal.
STATUS_SEQUENCE = ["CREATED", "ACCEPTED", "PICKED_UP", "DELIVERING", "DELIVERED"]

# driver_id disponibles (seed du Jour 1) — assignés à l'étape ACCEPTED.
AVAILABLE_DRIVER_IDS = [1, 2, 3, 4]

# Zone de Casablanca, cohérent avec gps-simulator.
LAT_RANGE = (33.55, 33.60)
LON_RANGE = (-7.65, -7.55)


@dataclass
class SimulatedOrder:
    """
    Représente une commande en cours de simulation.

    order_id : rempli après l'INSERT initial (retourné par PostgreSQL).
    profile_type : "normal" | "slow_delivery" | "cancelled" — détermine
    le comportement une fois la commande créée.
    next_transition_tick : tick auquel la prochaine transition doit avoir lieu
    (délai aléatoire, différent selon le profil).
    """
    order_id: int
    profile_type: str
    status_index: int = 0  # position actuelle dans STATUS_SEQUENCE
    finished: bool = False
    next_transition_tick: int = field(default=0)
    current_tick: int = field(default=0)


def create_order(cur, profile_type: str) -> int:
    """Insère une nouvelle commande CREATED. Déclenche trg_orders_insert_outbox."""
    pickup_lat = round(random.uniform(*LAT_RANGE), 6)
    pickup_lon = round(random.uniform(*LON_RANGE), 6)
    delivery_lat = round(random.uniform(*LAT_RANGE), 6)
    delivery_lon = round(random.uniform(*LON_RANGE), 6)

    cur.execute(
        """
        INSERT INTO orders (customer_id, status, pickup_lat, pickup_lon, delivery_lat, delivery_lon)
        VALUES (%s, 'CREATED', %s, %s, %s, %s)
        RETURNING order_id
        """,
        (random.randint(1000, 9999), pickup_lat, pickup_lon, delivery_lat, delivery_lon),
    )
    order_id = cur.fetchone()[0]
    logger.info("Commande créée: order_id=%s (profil=%s)", order_id, profile_type)
    return order_id


def advance_order(cur, order: SimulatedOrder) -> None:
    """
    Fait avancer une commande au statut suivant. Déclenche
    trg_orders_update_outbox (via la clause WHEN sur le changement de status).
    """
    next_status = STATUS_SEQUENCE[order.status_index + 1]

    # À l'étape ACCEPTED, on assigne un livreur (driver_id passe de NULL à une valeur).
    if next_status == "ACCEPTED":
        driver_id = random.choice(AVAILABLE_DRIVER_IDS)
        cur.execute(
            "UPDATE orders SET status = %s, driver_id = %s, updated_at = now() WHERE order_id = %s",
            (next_status, driver_id, order.order_id),
        )
    else:
        cur.execute(
            "UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s",
            (next_status, order.order_id),
        )

    order.status_index += 1
    logger.info("Commande order_id=%s -> %s", order.order_id, next_status)

    if next_status == STATUS_SEQUENCE[-1]:
        order.finished = True


def cancel_order(cur, order: SimulatedOrder) -> None:
    """Annule une commande (profil 'cancelled'). Déclenche trg_orders_update_outbox."""
    cur.execute(
        "UPDATE orders SET status = 'CANCELLED', updated_at = now() WHERE order_id = %s",
        (order.order_id,),
    )
    order.finished = True
    logger.info("Commande order_id=%s -> CANCELLED", order.order_id)


def schedule_next_transition(order: SimulatedOrder) -> None:
    """
    Détermine dans combien de ticks la prochaine transition doit avoir lieu,
    selon le profil de la commande.
    """
    if order.profile_type == "slow_delivery" and order.status_index + 1 == len(STATUS_SEQUENCE) - 1:
        # Juste avant DELIVERING -> DELIVERED : délai volontairement long
        # pour rester "coincée" en DELIVERING (exercera DELIVERY_DELAY plus tard).
        order.next_transition_tick = order.current_tick + 999  # ne se déclenchera jamais dans ce test
    else:
        order.next_transition_tick = order.current_tick + random.randint(1, 4)


def main() -> None:
    logger.info("Démarrage du Order Simulator...")
    conn = psycopg2.connect(get_postgres_dsn())
    conn.autocommit = True  # chaque INSERT/UPDATE est indépendant, pas de transaction longue ici
    orders: list[SimulatedOrder] = []
    tick = 0

    try:
        with conn.cursor() as cur:
            logger.info("Connecté à PostgreSQL.")

            while True:
                tick += 1

                # Crée occasionnellement une nouvelle commande (~1 fois sur 2 ticks).
                if random.random() < 0.5:
                    profile_type = random.choices(
                        ["normal", "slow_delivery", "cancelled"],
                        weights=[0.6, 0.2, 0.2],
                    )[0]
                    order_id = create_order(cur, profile_type)
                    new_order = SimulatedOrder(order_id=order_id, profile_type=profile_type, current_tick=tick)
                    schedule_next_transition(new_order)
                    orders.append(new_order)

                # Fait avancer les commandes actives dont c'est le tour.
                for order in orders:
                    if order.finished:
                        continue
                    order.current_tick = tick
                    if tick < order.next_transition_tick:
                        continue

                    if order.profile_type == "cancelled" and order.status_index == 1:
                        # S'annule juste après ACCEPTED.
                        cancel_order(cur, order)
                    elif order.status_index + 1 < len(STATUS_SEQUENCE):
                        advance_order(cur, order)
                        schedule_next_transition(order)

                time.sleep(TICK_SECONDS)
    except KeyboardInterrupt:
        logger.info("Arrêt demandé (Ctrl+C).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()