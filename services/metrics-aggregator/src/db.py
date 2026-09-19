"""
Gestion de la connexion PostgreSQL pour le Metrics Aggregator.
Réutilise la config partagée (delivery_shared) déjà utilisée
par les autres services — même host, mêmes credentials.
"""
import psycopg2
from delivery_shared.config import get_postgres_dsn


def get_connection():
    """Ouvre une nouvelle connexion PostgreSQL.

    On ouvre une connexion FRAÎCHE à chaque cycle plutôt que de garder
    une connexion unique ouverte en permanence. Raison : ce service tourne
    en boucle infinie sur plusieurs heures/jours — une connexion unique
    trop longtemps ouverte peut être coupée silencieusement par
    PostgreSQL ou un firewall, ce qui planterait le service au cycle
    suivant. Une connexion par cycle est plus robuste, et le coût
    (quelques ms) est négligeable sur un intervalle de 30s.
    """
    return psycopg2.connect(get_postgres_dsn())