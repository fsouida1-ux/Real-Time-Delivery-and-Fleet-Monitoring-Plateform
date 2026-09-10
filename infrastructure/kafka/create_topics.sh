#!/bin/sh
# ============================================================
# create_topics.sh
# Crée les 2 topics Kafka du projet, avec les paramètres exacts
# de la section 4 du cahier des charges.
# ============================================================
set -e

BOOTSTRAP="kafka:19092"

echo "Création du topic orders-events..."
/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic orders-events \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000

echo "Création du topic gps-events..."
/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic gps-events \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=172800000

echo "Topics créés. Liste actuelle :"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list