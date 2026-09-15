CREATE TABLE IF NOT EXISTS processed_events (
    event_id      VARCHAR(50) PRIMARY KEY,
    processed_at  TIMESTAMP NOT NULL DEFAULT now(),
    event_type    VARCHAR(30) NOT NULL
);