-- ============================================================
-- 002_indexes.sql
-- Index de performance — Jour 1.
--
-- Philosophie : un index seulement s'il correspond à un accès
-- réel et déjà identifié dans le cahier des charges. Pas
-- d'indexation "au cas où".
-- ============================================================

-- ------------------------------------------------------------
-- Index PARTIEL sur outbox_events.published.
--
-- Le DB Producer exécute en boucle (toutes les 1-2s, section 5.1) :
--   SELECT * FROM outbox_events WHERE published = false;
--
-- Un index partiel n'indexe QUE les lignes published = false,
-- donc son coût reste faible en permanence : les lignes déjà
-- publiées (qui s'accumulent avec le temps) ne l'alourdissent
-- jamais, puisqu'elles ne sont plus jamais recherchées ainsi.
-- ------------------------------------------------------------
CREATE INDEX idx_outbox_events_unpublished
    ON outbox_events (id)
    WHERE published = false;

-- ------------------------------------------------------------
-- Index sur orders.driver_id (clé étrangère vers drivers).
--
-- PostgreSQL n'indexe PAS automatiquement les clés étrangères
-- (contrairement à la PRIMARY KEY, qui l'est toujours). Sans cet
-- index, retrouver "les commandes d'un livreur donné" oblige un
-- scan complet de orders.
-- ------------------------------------------------------------
CREATE INDEX idx_orders_driver_id
    ON orders (driver_id);