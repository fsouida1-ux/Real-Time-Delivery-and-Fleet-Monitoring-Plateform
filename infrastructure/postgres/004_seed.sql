-- ============================================================
-- 004_seed.sql
-- Données de démonstration — Jour 1.
--
-- But : avoir immédiatement de quoi tester manuellement le
-- trigger (003_triggers.sql) et vérifier dans outbox_events que
-- les event_type générés sont corrects, sans attendre le Jour 2
-- (GPS Simulator, DB Producer).
--
-- Astuce volontaire : les orders ci-dessous ont des statuts
-- VARIÉS (pas tous 'CREATED'), pour observer plusieurs
-- event_type différents dès le premier docker compose up.
-- ============================================================

-- ------------------------------------------------------------
-- drivers : 4 profils couvrant les 3 statuts possibles.
-- ------------------------------------------------------------
INSERT INTO drivers (name, phone, status) VALUES
    ('Ahmed Bennani',   '+212600000001', 'AVAILABLE'),
    ('Youssef El Amrani','+212600000002', 'BUSY'),
    ('Karim Ziani',     '+212600000003', 'AVAILABLE'),
    ('Sara Idrissi',    '+212600000004', 'OFFLINE');

-- ------------------------------------------------------------
-- orders : chaque INSERT déclenche automatiquement le trigger
-- (AFTER INSERT -> ORDER_CREATED dans outbox_events), car le
-- statut par défaut de la colonne est 'CREATED'.
--
-- On force ensuite certaines commandes à un statut plus avancé
-- via UPDATE, ce qui déclenche un DEUXIÈME événement outbox
-- pour ces lignes (ORDER_ACCEPTED, ORDER_DELIVERING, ...) —
-- exactement le comportement qu'on veut vérifier.
-- ------------------------------------------------------------

-- Commande 1 : reste au statut par défaut CREATED (pas de driver assigné)
INSERT INTO orders (customer_id, pickup_lat, pickup_lon, delivery_lat, delivery_lon)
VALUES (101, 33.5731, -7.5898, 33.5892, -7.6034);

-- Commande 2 : créée puis assignée et acceptée par un livreur
INSERT INTO orders (customer_id, pickup_lat, pickup_lon, delivery_lat, delivery_lon)
VALUES (102, 33.5650, -7.6114, 33.5731, -7.5898);

UPDATE orders
SET driver_id = 1, status = 'ACCEPTED'
WHERE customer_id = 102;

-- Commande 3 : créée, puis avancée jusqu'à DELIVERING
INSERT INTO orders (customer_id, pickup_lat, pickup_lon, delivery_lat, delivery_lon)
VALUES (103, 33.5928, -7.6192, 33.5731, -7.5898);

UPDATE orders SET driver_id = 2, status = 'ACCEPTED' WHERE customer_id = 103;
UPDATE orders SET status = 'PICKED_UP' WHERE customer_id = 103;
UPDATE orders SET status = 'DELIVERING' WHERE customer_id = 103;

-- Commande 4 : cycle complet jusqu'à DELIVERED
INSERT INTO orders (customer_id, pickup_lat, pickup_lon, delivery_lat, delivery_lon)
VALUES (104, 33.5850, -7.6300, 33.5920, -7.6100);

UPDATE orders SET driver_id = 3, status = 'ACCEPTED'   WHERE customer_id = 104;
UPDATE orders SET status = 'PICKED_UP'                 WHERE customer_id = 104;
UPDATE orders SET status = 'DELIVERING'                WHERE customer_id = 104;
UPDATE orders SET status = 'DELIVERED'                 WHERE customer_id = 104;