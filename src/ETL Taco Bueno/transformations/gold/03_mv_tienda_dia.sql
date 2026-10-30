-- ════════════════════════════════════════════════════════════════
-- Capa GOLD — Taco Bueno
-- Vista materializada: mv_tienda_dia
-- Grano: 1 fila por store_id + fecha.
-- Fuente: tacobueno.gold.mv_tickets_base.
-- Origen de la lógica: notebook 02_promo_react, PARTE 1 (validada V1-V7).
--
-- Regla de exclusión (`excluir`): flag_inconsistente = 1
--   OR (flag_total_cero = 1 AND sin_ecuacion). Solo los tickets NO
--   excluidos entran a las métricas; los excluidos se cuentan aparte.
-- periodo_prov: P1_lanzamiento (< 2025-04-01), P1_estable (2025-04-01 a
--   2025-10-31), P2 (2025-11-01 a 2026-01-31), P3 (>= 2026-02-01).
-- franja: sin_hora (hora=0), desayuno (5-10), almuerzo (11-13),
--   tarde (14-16), cena (17-20), noche (resto).
-- formato_nuevo: len_check_number >= 14.
-- ════════════════════════════════════════════════════════════════
CREATE OR REFRESH MATERIALIZED VIEW tacobueno.gold.mv_tienda_dia
COMMENT 'Gold: 1 fila por tienda y día. Métricas de tickets, promoción, canales y franjas con regla de exclusión validada.'
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
        END AS periodo_prov,
        CASE
            WHEN hora = 0                 THEN 'sin_hora'
            WHEN hora BETWEEN 5 AND 10    THEN 'desayuno'
            WHEN hora BETWEEN 11 AND 13   THEN 'almuerzo'
            WHEN hora BETWEEN 14 AND 16   THEN 'tarde'
            WHEN hora BETWEEN 17 AND 20   THEN 'cena'
            ELSE 'noche'                  -- 21 a 23 y 1 a 4
        END AS franja,
        (len_check_number >= 14) AS formato_nuevo
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
        anio_mes,
        periodo_prov,
        MAX(dow_spark)                  AS dow_spark,
        BOOL_OR(is_partial_day)         AS is_partial_day,
        BOOL_OR(is_low_coverage_store)  AS is_low_coverage_store,
        -- conteos
        SUM(CASE WHEN NOT excluir THEN 1 ELSE 0 END)                     AS tickets,
        SUM(CASE WHEN excluir THEN 1 ELSE 0 END)                          AS tickets_excluidos,
        SUM(CASE WHEN NOT excluir AND tiene_promo THEN 1 ELSE 0 END)     AS tickets_promo,
        SUM(CASE WHEN NOT excluir AND promo_td THEN 1 ELSE 0 END)        AS tickets_promo_td,
        SUM(CASE WHEN NOT excluir AND promo_adj THEN 1 ELSE 0 END)       AS tickets_promo_adj,
        SUM(CASE WHEN sin_ec_residual THEN 1 ELSE 0 END)                AS t_sin_ecuacion,
        SUM(CASE WHEN NOT excluir AND formato_nuevo THEN 1 ELSE 0 END)  AS t_formato_nuevo,
        -- montos
        COALESCE(SUM(CASE WHEN NOT excluir THEN sub_prod END), 0)          AS ventas_lista,
        COALESCE(SUM(CASE WHEN NOT excluir THEN venta_neta_prod END), 0)   AS venta_neta,
        COALESCE(SUM(CASE WHEN NOT excluir THEN n_items END), 0)           AS n_items,
        COALESCE(SUM(CASE WHEN NOT excluir THEN monto_desc_total END), 0) AS monto_desc_total,
        percentile_approx(CASE WHEN NOT excluir THEN venta_neta_prod END, 0.5) AS ticket_mediano,
        -- tickets por canal_grupo
        SUM(CASE WHEN NOT excluir AND canal_grupo = 'drive_thru'          THEN 1 ELSE 0 END) AS t_drive_thru,
        SUM(CASE WHEN NOT excluir AND canal_grupo = 'para_llevar'        THEN 1 ELSE 0 END) AS t_para_llevar,
        SUM(CASE WHEN NOT excluir AND canal_grupo = 'salon'              THEN 1 ELSE 0 END) AS t_salon,
        SUM(CASE WHEN NOT excluir AND canal_grupo = 'digital_propio'     THEN 1 ELSE 0 END) AS t_digital_propio,
        SUM(CASE WHEN NOT excluir AND canal_grupo = 'delivery_terceros'  THEN 1 ELSE 0 END) AS t_delivery_terceros,
        SUM(CASE WHEN NOT excluir AND canal_grupo = 'desconocido'        THEN 1 ELSE 0 END) AS t_desconocido,
        SUM(CASE WHEN NOT excluir AND canal_grupo = 'otro'               THEN 1 ELSE 0 END) AS t_otro,
        -- tickets por franja
        SUM(CASE WHEN NOT excluir AND franja = 'desayuno'  THEN 1 ELSE 0 END) AS t_desayuno,
        SUM(CASE WHEN NOT excluir AND franja = 'almuerzo'  THEN 1 ELSE 0 END) AS t_almuerzo,
        SUM(CASE WHEN NOT excluir AND franja = 'tarde'     THEN 1 ELSE 0 END) AS t_tarde,
        SUM(CASE WHEN NOT excluir AND franja = 'cena'      THEN 1 ELSE 0 END) AS t_cena,
        SUM(CASE WHEN NOT excluir AND franja = 'noche'     THEN 1 ELSE 0 END) AS t_noche,
        SUM(CASE WHEN NOT excluir AND franja = 'sin_hora'  THEN 1 ELSE 0 END) AS t_sin_hora,
        -- pedidos grandes y total cero (dentro de los no excluidos)
        SUM(CASE WHEN NOT excluir AND max_qty >= 10        THEN 1 ELSE 0 END) AS t_pedido_grande,
        SUM(CASE WHEN NOT excluir AND flag_total_cero = 1  THEN 1 ELSE 0 END) AS t_total_cero
    FROM base
    GROUP BY store_id, fecha, anio_mes, periodo_prov
)
SELECT
    store_id, fecha, anio_mes, periodo_prov,
    dow_spark, is_partial_day, is_low_coverage_store,
    tickets, tickets_excluidos,
    tickets_promo, tickets_promo_td, tickets_promo_adj,
    CASE WHEN tickets > 0 THEN ROUND(tickets_promo / tickets, 4) END    AS pct_promo,
    CASE WHEN tickets > 0 THEN ROUND(t_formato_nuevo / tickets, 4) END AS pct_formato_nuevo,
    t_sin_ecuacion, t_formato_nuevo,
    ventas_lista, venta_neta, n_items, monto_desc_total, ticket_mediano,
    t_drive_thru, t_para_llevar, t_salon, t_digital_propio, t_delivery_terceros, t_desconocido, t_otro,
    t_desayuno, t_almuerzo, t_tarde, t_cena, t_noche, t_sin_hora,
    t_pedido_grande, t_total_cero
FROM agg