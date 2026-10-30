-- ════════════════════════════════════════════════════════════════
-- Capa GOLD — Taco Bueno
-- Vista materializada: mv_promo_resumen_mes
-- Grano: 1 fila por anio_mes.
-- Fuente: tacobueno.gold.mv_tickets_base.
-- Resumen mensual de promociones para consumo directo
-- (dashboards / analistas). Deriva de las métricas ya validadas
-- (V3-V4 del notebook 000_promos) y aplica la misma regla de
-- exclusión que mv_tienda_dia.
-- ════════════════════════════════════════════════════════════════
CREATE OR REFRESH MATERIALIZED VIEW tacobueno.gold.mv_promo_resumen_mes
COMMENT 'Gold: resumen mensual de promociones (tickets, % promo, montos y descuento por ticket promo). Regla de exclusión validada.'
AS
WITH base AS (
    SELECT
        anio_mes,
        (flag_inconsistente = 1 OR (flag_total_cero = 1 AND sin_ecuacion)) AS excluir,
        tiene_promo,
        promo_td,
        promo_adj,
        venta_neta_prod,
        monto_desc_total,
        n_items,
        sin_ecuacion
    FROM tacobueno.gold.mv_tickets_base
),
agg AS (
    SELECT
        anio_mes,
        COUNT(*)                                                        AS tickets_totales,
        SUM(CASE WHEN NOT excluir THEN 1 ELSE 0 END)                    AS tickets,
        SUM(CASE WHEN excluir THEN 1 ELSE 0 END)                        AS tickets_excluidos,
        SUM(CASE WHEN NOT excluir AND tiene_promo THEN 1 ELSE 0 END)    AS tickets_promo,
        SUM(CASE WHEN NOT excluir AND promo_td THEN 1 ELSE 0 END)       AS tickets_promo_td,
        SUM(CASE WHEN NOT excluir AND promo_adj AND NOT promo_td THEN 1 ELSE 0 END) AS tickets_promo_solo_adj,
        SUM(CASE WHEN NOT excluir AND promo_adj AND promo_td THEN 1 ELSE 0 END)     AS tickets_promo_ambos,
        SUM(CASE WHEN sin_ecuacion AND NOT excluir THEN 1 ELSE 0 END)  AS t_sin_ecuacion,
        COALESCE(SUM(CASE WHEN NOT excluir THEN venta_neta_prod END), 0)   AS venta_neta,
        COALESCE(SUM(CASE WHEN NOT excluir THEN monto_desc_total END), 0)  AS monto_desc_total,
        COALESCE(SUM(CASE WHEN NOT excluir THEN n_items END), 0)            AS n_items
    FROM base
    GROUP BY anio_mes
)
SELECT
    anio_mes,
    tickets_totales,
    tickets,
    tickets_excluidos,
    tickets_promo,
    tickets_promo_td,
    tickets_promo_solo_adj,
    tickets_promo_ambos,
    CASE WHEN tickets > 0 THEN ROUND(tickets_promo * 100.0 / tickets, 2) END     AS pct_promo,
    CASE WHEN tickets > 0 THEN ROUND(tickets_promo_td * 100.0 / tickets, 2) END  AS pct_promo_td,
    CASE WHEN tickets_promo > 0 THEN ROUND(monto_desc_total / tickets_promo, 4) END AS desc_por_ticket_promo,
    venta_neta,
    monto_desc_total,
    n_items,
    t_sin_ecuacion,
    CASE WHEN tickets > 0 THEN ROUND(t_sin_ecuacion * 100.0 / tickets, 3) END AS pct_sin_ecuacion
FROM agg