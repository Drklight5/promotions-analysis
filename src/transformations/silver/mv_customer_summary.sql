CREATE OR REFRESH MATERIALIZED VIEW mv_customer_summary AS
SELECT
  customer_id,
  COUNT(*) AS total_orders
FROM st_orders_cleaned
GROUP BY customer_id;
