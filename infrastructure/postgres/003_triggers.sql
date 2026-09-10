-- ============================================================
-- 003_triggers.sql
-- Trigger outbox — Jour 1 (décision architecturale validée :
-- garantie structurelle plutôt que discipline applicative,
-- voir section 5.1 du cahier des charges).
--
-- Portée :
--   - INSERT sur orders  -> événement ORDER_CREATED
--   - UPDATE de status   -> événement ORDER_<NOUVEAU_STATUT>
--     (uniquement si le statut a réellement changé)
-- ============================================================

CREATE OR REPLACE FUNCTION fn_orders_to_outbox()
RETURNS TRIGGER AS $$
BEGIN
    -- Cas 1 : nouvelle commande (TG_OP = 'INSERT')
    IF TG_OP = 'INSERT' THEN
        INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
        VALUES (
            'order',
            NEW.order_id,
            'ORDER_' || NEW.status,
            jsonb_build_object(
                'order_id',    NEW.order_id,
                'customer_id', NEW.customer_id,
                'driver_id',   NEW.driver_id,
                'status',      NEW.status,
                'created_at',  NEW.created_at
            )
        );
        RETURN NEW;
    END IF;

    -- Cas 2 : commande existante, on ne réagit QUE si le statut
    -- a réellement changé (évite de spammer outbox_events si on
    -- met à jour une commande sans toucher à son statut, ex: un
    -- futur champ non lié au cycle de vie).
    IF TG_OP = 'UPDATE' AND OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
        VALUES (
            'order',
            NEW.order_id,
            'ORDER_' || NEW.status,
            jsonb_build_object(
                'order_id',    NEW.order_id,
                'customer_id', NEW.customer_id,
                'driver_id',   NEW.driver_id,
                'status',      NEW.status,
                'updated_at',  NEW.updated_at
            )
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Un seul trigger couvre les deux cas (INSERT et UPDATE) grâce
-- au IF TG_OP à l'intérieur de la fonction ci-dessus.
CREATE TRIGGER trg_orders_outbox
    AFTER INSERT OR UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION fn_orders_to_outbox();