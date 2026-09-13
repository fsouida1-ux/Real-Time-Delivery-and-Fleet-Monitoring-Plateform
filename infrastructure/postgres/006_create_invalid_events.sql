-- 006_create_invalid_events.sql
-- Stocke les événements GPS rejetés par la validation (section 6.2)
-- pour analyse a posteriori, sans bloquer le flux principal.

CREATE TABLE invalid_events (
    id            SERIAL PRIMARY KEY,
    event_id      VARCHAR(50),
    topic         VARCHAR(50) NOT NULL,
    raw_payload   JSONB NOT NULL,
    reason        TEXT NOT NULL,
    received_at   TIMESTAMP DEFAULT now()
);