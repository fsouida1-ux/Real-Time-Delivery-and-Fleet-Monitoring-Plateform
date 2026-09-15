CREATE TABLE IF NOT EXISTS alerts (
    id          SERIAL PRIMARY KEY,
    alert_type  VARCHAR(30) NOT NULL,
    driver_id   INTEGER REFERENCES drivers(driver_id),
    order_id    INTEGER REFERENCES orders(order_id),
    details     JSONB,
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);