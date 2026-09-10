-- ============================================================
-- 001_tables.sql
-- Tables transactionnelles (source) — Jour 1 uniquement.
-- Cahier des charges section 3.1.
--
-- Portée volontairement limitée : drivers, orders, outbox_events.
-- Les tables de traitement/analytics (processed_events,
-- driver_positions, alerts, delivery_metrics, driver_metrics)
-- arrivent aux Jours 4 et 5 (voir 002_..., futurs scripts).
-- ============================================================

-- ------------------------------------------------------------
-- drivers : les livreurs. Table simple, peu de contraintes car
-- ce n'est pas le cœur du MVP (le cœur, c'est le flux d'événements).
-- ------------------------------------------------------------
CREATE TABLE drivers (
    driver_id   SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    phone       VARCHAR(20),
    status      VARCHAR(20) NOT NULL DEFAULT 'OFFLINE'
                CHECK (status IN ('AVAILABLE', 'BUSY', 'OFFLINE')),
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------
-- orders : les commandes. C'est LA table qui déclenchera notre
-- trigger outbox (003_triggers.sql) à chaque changement de statut.
--
-- Remarque volontaire : driver_id peut être NULL (une commande
-- CREATED n'a pas encore de livreur assigné).
-- ------------------------------------------------------------
CREATE TABLE orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL,
    driver_id       INTEGER REFERENCES drivers(driver_id),
    status          VARCHAR(20) NOT NULL DEFAULT 'CREATED'
                    CHECK (status IN (
                        'CREATED', 'ACCEPTED', 'PICKED_UP',
                        'DELIVERING', 'DELIVERED', 'CANCELLED'
                    )),
    pickup_lat      NUMERIC(9,6),
    pickup_lon      NUMERIC(9,6),
    delivery_lat    NUMERIC(9,6),
    delivery_lon    NUMERIC(9,6),
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------
-- outbox_events : mécanisme du DB Producer (section 5.1).
-- Rempli automatiquement par le trigger de 003_triggers.sql,
-- lu par le DB Producer (Jour 2) qui publie vers Kafka puis
-- marque published = true.
-- ------------------------------------------------------------
CREATE TABLE outbox_events (
    id              SERIAL PRIMARY KEY,
    aggregate_type  VARCHAR(20) NOT NULL,
    aggregate_id    INTEGER NOT NULL,
    event_type      VARCHAR(30) NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    published       BOOLEAN NOT NULL DEFAULT false
);