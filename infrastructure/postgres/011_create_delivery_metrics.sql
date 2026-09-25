CREATE TABLE IF NOT EXISTS delivery_metrics (
    id                SERIAL PRIMARY KEY,
    window_start      TIMESTAMP NOT NULL,
    window_end        TIMESTAMP NOT NULL,
    total_orders      INTEGER NOT NULL,
    delivered_orders  INTEGER NOT NULL,
    avg_delivery_min  NUMERIC(10,2),
    created_at        TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_delivery_metrics_window
    ON delivery_metrics (window_start DESC);