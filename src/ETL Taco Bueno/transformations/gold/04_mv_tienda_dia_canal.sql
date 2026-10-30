-- ════════════════════════════════════════════════════════════════
-- Capa GOLD — Taco Bueno
-- Vista materializada: mv_tienda_dia_canal
-- Grano: 1 fila por store_id + fecha + canal_grupo.
-- Fuente: tacobueno.gold.mv_tickets_base.
-- Origen de la lógica: notebook 02_promo_react, PARTE 2 (validada V1).
-- Misma regla de exclusión que mv_tienda_dia: solo los tickets NO
-- excluidos entran a las métricas.
-- ════════════════════════════════════════════════════════════════
CREATE OR REFRESH MATERIALIZED VIEW tacobueno.gold.mv_tienda_dia_canal
COMMENT 'Gold: 1 fila por tienda, día y canal_grupo. Métricas de tickets y promoción por canal con regla de exclusión validada.'
AS
WITH base0 AS (
    SELECT
        *,
        (flag_inconsistente = 1 OR (flag_total_cero = 1 AND sin_ecuacion)) AS excluir,
        CASE
            WHEN fecha < DATE '2025-04-01'                                 THEN 'P1_lanzamiento'
            WHEN fecha BETWEEN DATE '2025-04-01' AND DATE '2025-10-31'    THEN 'P1_estable'
            WHEN fecha BETWEEN DATE '2025-11-01' AND DATE '2026-01-31'   THEN 'P2'
            WHEN fecha >= DATE '2026-02-01'                               THEN 'P3'
        END AS periodo_prov
    FROM tacobueno.gold.mv_tickets_base
),
base AS (
    SELECT
        *,
        (sin_ecuacion AND NOT excluir) AS sin_ec_residual
    FROM base0
),
agg AS (
    SELECT
        store_id,
        fecha,
        canal_grupo,
        anio_mes,
        periodo_prov,
        MAX(dow_spark)                 AS dow_spark,
        SUM(CASE WHEN NOT excluir THEN 1 ELSE 0 END)                     AS tickets,
        SUM(CASE WHEN NOT excluir AND tiene_promo THEN 1 ELSE 0 END)     AS tickets_promo,
        SUM(CASE WHEN sin_ec_residual THEN 1 ELSE 0 END)                 AS t_sin_ecuacion,
        COALESCE(SUM(CASE WHEN NOT excluir THEN sub_prod END), 0)          AS ventas_lista,
        COALESCE(SUM(CASE WHEN NOT excluir THEN venta_neta_prod END), 0)   AS venta_neta,
        COALESCE(SUM(CASE WHEN NOT excluir THEN n_items END), 0)           AS n_items,
        COALESCE(SUM(CASE WHEN NOT excluir THEN monto_desc_total END), 0)  AS monto_desc_total,
        percentile_approx(CASE WHEN NOT excluir THEN venta_neta_prod END, 0.5) AS ticket_mediano
    FROM base
    GROUP BY store_id, fecha, canal_grupo, anio_mes, periodo_prov
)
SELECT * FROM agg