-- Extension nécessaire pour gen_random_uuid() (event_id unique, cohérent
-- avec le pattern déjà utilisé côté gps-simulator avec uuid.uuid4()).
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- DROP explicite avant recréation : rend ce script réexécutable sans
-- erreur "already exists" si on doit le relancer après une correction
-- (comme c'est le cas ici).
DROP TRIGGER IF EXISTS trg_orders_insert_outbox ON orders;
DROP TRIGGER IF EXISTS trg_orders_update_outbox ON orders;
DROP FUNCTION IF EXISTS fn_orders_to_outbox();

CREATE OR REPLACE FUNCTION fn_orders_to_outbox()
RETURNS TRIGGER AS $$
DECLARE
    v_event_type VARCHAR(30);
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_event_type := 'ORDER_CREATED';
    ELSE
        v_event_type := 'ORDER_' || NEW.status;
    END IF;

    INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
    VALUES (
        'order',
        NEW.order_id,
        v_event_type,
        jsonb_build_object(
            'event_id', 'order-' || NEW.order_id || '-' || replace(gen_random_uuid()::text, '-', ''),
            'event_type', v_event_type,
            'order_id', NEW.order_id,
            'driver_id', NEW.driver_id,
            'status', NEW.status,
            'timestamp', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS."000Z"')
        )
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_insert_outbox
    AFTER INSERT ON orders
    FOR EACH ROW
    EXECUTE FUNCTION fn_orders_to_outbox();

CREATE TRIGGER trg_orders_update_outbox
    AFTER UPDATE ON orders
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION fn_orders_to_outbox();