"""
Metrics Aggregator — service indépendant tournant en boucle.

Contrairement au Processing Consumer, ce service ne lit PAS Kafka.
Il scanne périodiquement l'état stocké en PostgreSQL pour calculer
des métriques agrégées et détecter des situations anormales qui ne
peuvent PAS être détectées au fil de l'eau (absence, délai).

Section 7 du cahier des charges.
"""
import time
import json
import logging
from datetime import datetime, timedelta

from db import get_connection
from delivery_shared.config import get_driver_inactive_minutes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("metrics-aggregator")

METRICS_INTERVAL_SECONDS = 30  # cohérent avec METRICS_INTERVAL_SECONDS du .env


def compute_driver_metrics(cur, window_start, window_end):
    """Agrège driver_positions par livreur sur la fenêtre [window_start, window_end)."""
    cur.execute(
        """
        INSERT INTO driver_metrics (driver_id, window_start, window_end, gps_events_count, avg_speed)
        SELECT driver_id, %s, %s, COUNT(*), AVG(speed)
        FROM driver_positions
        WHERE recorded_at >= %s AND recorded_at < %s
        GROUP BY driver_id
        """,
        (window_start, window_end, window_start, window_end),
    )
    logger.info("driver_metrics : %s ligne(s) insérée(s)", cur.rowcount)


def scan_driver_inactive(cur, inactive_threshold_minutes):
    """Détecte les livreurs inactifs et crée une alerte — une seule par épisode d'inactivité."""
    cur.execute(
        """
        WITH last_positions AS (
            SELECT driver_id, MAX(recorded_at) AS last_seen
            FROM driver_positions
            GROUP BY driver_id
        )
        SELECT lp.driver_id, lp.last_seen
        FROM last_positions lp
        WHERE lp.last_seen < now() - (%s || ' minutes')::interval
          AND NOT EXISTS (
              SELECT 1 FROM alerts a
              WHERE a.driver_id = lp.driver_id
                AND a.alert_type = 'DRIVER_INACTIVE'
                AND a.created_at > lp.last_seen
          )
        """,
        (inactive_threshold_minutes,),
    )
    inactive_drivers = cur.fetchall()

    for driver_id, last_seen in inactive_drivers:
        cur.execute(
            "INSERT INTO alerts (alert_type, driver_id, details) VALUES (%s, %s, %s)",
            ("DRIVER_INACTIVE", driver_id, json.dumps({"last_seen": last_seen.isoformat()})),
        )
        logger.info(
            "Alerte DRIVER_INACTIVE créée — driver_id=%s, dernière position: %s",
            driver_id, last_seen,
        )

    return len(inactive_drivers)


def run_cycle():
    """Exécute un seul cycle : agrégation driver_metrics + scan DRIVER_INACTIVE.

    Les deux opérations partagent la même transaction : si l'une échoue,
    l'autre est annulée aussi (rollback), pour ne jamais laisser un cycle
    à moitié appliqué.
    """
    conn = get_connection()
    try:
        window_end = datetime.utcnow()
        window_start = window_end - timedelta(seconds=METRICS_INTERVAL_SECONDS)

        with conn.cursor() as cur:
            logger.info("Cycle démarré — fenêtre %s → %s", window_start, window_end)
            compute_driver_metrics(cur, window_start, window_end)
            scan_driver_inactive(cur, get_driver_inactive_minutes())

        conn.commit()
        logger.info("Cycle terminé")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    logger.info("Metrics Aggregator démarré (intervalle = %ss)", METRICS_INTERVAL_SECONDS)
    while True:
        try:
            run_cycle()
        except Exception:
            # Un cycle qui échoue ne doit JAMAIS arrêter le service.
            # On logue l'erreur complète et on continue — le prochain
            # cycle réessaiera dans 30s.
            logger.exception("Erreur pendant le cycle — on continue")
        time.sleep(METRICS_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()