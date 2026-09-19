CREATE TABLE IF NOT EXISTS driver_metrics (
    id                SERIAL PRIMARY KEY,
    driver_id         INTEGER NOT NULL REFERENCES drivers(driver_id),
    window_start      TIMESTAMP NOT NULL,
    window_end        TIMESTAMP NOT NULL,
    gps_events_count  INTEGER NOT NULL,
    avg_speed         NUMERIC(5,2),
    created_at        TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_driver_metrics_driver_window
    ON driver_metrics (driver_id, window_start DESC);