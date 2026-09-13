-- 005_create_driver_positions.sql
-- Table de destination pour les positions GPS validées et nettoyées
-- Alimentée par le Processing Consumer (section 6, Jour 3)

CREATE TABLE driver_positions (
    id           SERIAL PRIMARY KEY,
    driver_id    INTEGER NOT NULL REFERENCES drivers(driver_id),
    latitude     NUMERIC(9,6) NOT NULL,
    longitude    NUMERIC(9,6) NOT NULL,
    speed        NUMERIC(5,2) NOT NULL,
    recorded_at  TIMESTAMP NOT NULL
);

-- Index pour accélérer les requêtes du Metrics Aggregator (Jour 5)
-- qui va scanner "la dernière position par driver" pour détecter DRIVER_INACTIVE
CREATE INDEX idx_driver_positions_driver_id_recorded_at
    ON driver_positions (driver_id, recorded_at DESC);