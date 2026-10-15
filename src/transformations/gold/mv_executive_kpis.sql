CREATE OR REFRESH MATERIALIZED VIEW mv_executive_kpis AS
SELECT
  SUM(revenue) AS total_revenue,
  AVG(revenue) AS avg_daily_revenue
FROM st_daily_revenue;
