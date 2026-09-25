-- Vue 1 : répartition des commandes par statut
-- Alimente le KPI "Total Orders" / "Delivered Orders" et le bar chart "Commandes par statut"
CREATE OR REPLACE VIEW vw_delivery_summary AS
SELECT
    status,
    COUNT(*) AS total
FROM orders
GROUP BY status;

-- Vue 2 : activité des livreurs (nombre de positions GPS + vitesse moyenne)
-- Alimente le tableau/bar chart "Activité des livreurs"
CREATE OR REPLACE VIEW vw_driver_activity AS
SELECT
    driver_id,
    COUNT(*) AS gps_events,
    AVG(speed) AS avg_speed
FROM driver_positions
GROUP BY driver_id;

-- Vue 3 : synthèse des alertes par type
-- Alimente le KPI "Total Alerts" et le bar/pie chart "Alertes par type"
CREATE OR REPLACE VIEW vw_alert_summary AS
SELECT
    alert_type,
    COUNT(*) AS total,
    MAX(created_at) AS last_seen
FROM alerts
GROUP BY alert_type;