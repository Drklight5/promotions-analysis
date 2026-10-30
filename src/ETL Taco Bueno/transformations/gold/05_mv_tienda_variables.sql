-- ════════════════════════════════════════════════════════════════
-- Capa GOLD — Taco Bueno
-- Vista materializada: mv_tienda_variables
-- Grano: 1 fila por store_id + periodo_prov (solo P1_estable y P3).
-- Fuente: tacobueno.gold.mv_tienda_dia.
-- Origen de la lógica: notebook 02_promo_react, "tablón tienda_variables"
-- (validada V1-V5).
--
-- Solo días completos: NOT is_partial_day AND NOT is_low_coverage_store.
-- Se conservan todas las tiendas; en_universo = dias_completos >= 90
-- AND tickets_promo >= 300 (umbral base validado en la sección "universo").
-- Variables para agrupar: pct_promo, desc_por_ticket_promo, razon_desc,
-- p_desayuno. El resto describe o controla calidad.
-- cv_ventas: CV de la venta_neta diaria sobre residuos por media de su dow.
-- ════════════════════════════════════════════════════════════════
CREATE OR REFRESH MATERIALIZED VIEW tacobueno.gold.mv_tienda_variables
COMMENT 'Gold: tablón de variables por tienda y periodo (P1_estable, P3), solo días completos. Universo: >= 90 días y >= 300 tickets promo.'
AS
WITH filtro AS (
    SELECT *
    FROM tacobueno.gold.mv_tienda_dia
    WHERE periodo_prov IN ('P1_estable', 'P3')
      AND NOT is_partial_day
      AND NOT is_low_coverage_store
),
sp AS (
    SELECT
        store_id,
        periodo_prov,
        COUNT(*)                 AS dias_completos,
        SUM(tickets)             AS tickets,
        SUM(tickets_promo)       AS tickets_promo,
        SUM(tickets_promo_td)    AS tickets_promo_td,
        SUM(tickets_promo_adj)   AS tickets_promo_adj,
        SUM(monto_desc_total)    AS monto_desc,
        SUM(ventas_lista)        AS ventas_lista,
        SUM(venta_neta)          AS venta_neta,
        SUM(t_desayuno)          AS t_desayuno,
        SUM(t_almuerzo)          AS t_almuerzo,
        SUM(t_tarde)             AS t_tarde,
        SUM(t_cena)              AS t_cena,
        SUM(t_noche)             AS t_noche,
        SUM(t_sin_hora)          AS t_sin_hora,
        SUM(t_drive_thru)        AS t_drive_thru,
        SUM(t_para_llevar)       AS t_para_llevar,
        SUM(t_salon)             AS t_salon,
        SUM(t_digital_propio)    AS t_digital_propio,
        SUM(t_delivery_terceros) AS t_delivery_terceros,
        SUM(t_desconocido)       AS t_desconocido,
        SUM(t_otro)              AS t_otro,
        SUM(t_sin_ecuacion)      AS t_sin_ecuacion,
        SUM(t_pedido_grande)     AS t_pedido_grande,
        percentile_approx(ticket_mediano, 0.5) AS ticket_mediano_tienda
    FROM filtro
    GROUP BY store_id, periodo_prov
),
-- pivote de tickets por dow_spark (1 = domingo, 7 = sábado)
dowp AS (
    SELECT
        store_id,
        periodo_prov,
        SUM(CASE WHEN dow_spark = 1 THEN tickets ELSE 0 END) AS t_dow_1,
        SUM(CASE WHEN dow_spark = 2 THEN tickets ELSE 0 END) AS t_dow_2,
        SUM(CASE WHEN dow_spark = 3 THEN tickets ELSE 0 END) AS t_dow_3,
        SUM(CASE WHEN dow_spark = 4 THEN tickets ELSE 0 END) AS t_dow_4,
        SUM(CASE WHEN dow_spark = 5 THEN tickets ELSE 0 END) AS t_dow_5,
        SUM(CASE WHEN dow_spark = 6 THEN tickets ELSE 0 END) AS t_dow_6,
        SUM(CASE WHEN dow_spark = 7 THEN tickets ELSE 0 END) AS t_dow_7
    FROM filtro
    GROUP BY store_id, periodo_prov
),
-- cv_ventas: CV de la venta_neta diaria tras dividir cada día entre
-- la media de venta_neta de su dow_spark en la tienda (residuos)
cv AS (
    SELECT
        store_id,
        periodo_prov,
        CASE WHEN AVG(r) > 0 THEN STDDEV(r) / AVG(r) END AS cv_ventas
    FROM (
        SELECT
            store_id,
            periodo_prov,
            venta_neta / AVG(venta_neta) OVER (PARTITION BY store_id, periodo_prov, dow_spark) AS r
        FROM filtro
    )
    GROUP BY store_id, periodo_prov
),
m AS (
    SELECT
        s.store_id,
        s.periodo_prov,
        (s.dias_completos >= 90 AND s.tickets_promo >= 300) AS en_universo,
        s.dias_completos,
        s.tickets,
        s.tickets_promo,
        -- uso de promoción (agrupar)
        CASE WHEN s.tickets > 0        THEN s.tickets_promo / s.tickets END         AS pct_promo,
        CASE WHEN s.tickets > 0        THEN s.tickets_promo_td / s.tickets END      AS pct_promo_td,
        CASE WHEN s.tickets > 0        THEN s.tickets_promo_adj / s.tickets END     AS pct_promo_adj,
        CASE WHEN s.tickets_promo > 0  THEN s.monto_desc / s.tickets_promo END     AS desc_por_ticket_promo,
        CASE WHEN s.ventas_lista > 0   THEN s.monto_desc / s.ventas_lista END       AS razon_desc,
        -- patrón semanal (agrupar); suman 1
        CASE WHEN s.tickets > 0 THEN d.t_dow_1 / s.tickets END AS p_dow_1,
        CASE WHEN s.tickets > 0 THEN d.t_dow_2 / s.tickets END AS p_dow_2,
        CASE WHEN s.tickets > 0 THEN d.t_dow_3 / s.tickets END AS p_dow_3,
        CASE WHEN s.tickets > 0 THEN d.t_dow_4 / s.tickets END AS p_dow_4,
        CASE WHEN s.tickets > 0 THEN d.t_dow_5 / s.tickets END AS p_dow_5,
        CASE WHEN s.tickets > 0 THEN d.t_dow_6 / s.tickets END AS p_dow_6,
        CASE WHEN s.tickets > 0 THEN d.t_dow_7 / s.tickets END AS p_dow_7,
        -- patrón horario (agrupar); suman 1 sobre tickets - t_sin_hora
        CASE WHEN s.tickets - s.t_sin_hora > 0 THEN s.t_desayuno / (s.tickets - s.t_sin_hora) END AS p_desayuno,
        CASE WHEN s.tickets - s.t_sin_hora > 0 THEN s.t_almuerzo / (s.tickets - s.t_sin_hora) END AS p_almuerzo,
        CASE WHEN s.tickets - s.t_sin_hora > 0 THEN s.t_tarde    / (s.tickets - s.t_sin_hora) END AS p_tarde,
        CASE WHEN s.tickets - s.t_sin_hora > 0 THEN s.t_cena     / (s.tickets - s.t_sin_hora) END AS p_cena,
        CASE WHEN s.tickets - s.t_sin_hora > 0 THEN s.t_noche    / (s.tickets - s.t_sin_hora) END AS p_noche,
        -- mezcla de canales (describir); suman 1
        CASE WHEN s.tickets > 0 THEN s.t_drive_thru        / s.tickets END AS p_drive_thru,
        CASE WHEN s.tickets > 0 THEN s.t_para_llevar      / s.tickets END AS p_para_llevar,
        CASE WHEN s.tickets > 0 THEN s.t_salon            / s.tickets END AS p_salon,
        CASE WHEN s.tickets > 0 THEN s.t_digital_propio    / s.tickets END AS p_digital_propio,
        CASE WHEN s.tickets > 0 THEN s.t_delivery_terceros / s.tickets END AS p_delivery_terceros,
        CASE WHEN s.tickets > 0 THEN s.t_desconocido       / s.tickets END AS p_desconocido,
        CASE WHEN s.tickets > 0 THEN s.t_otro              / s.tickets END AS p_otro,
        -- estabilidad (describir)
        c.cv_ventas,
        -- tamaño (solo describir)
        s.tickets / s.dias_completos        AS tickets_por_dia,
        s.venta_neta / s.dias_completos     AS venta_neta_por_dia,
        s.ticket_mediano_tienda,
        -- calidad
        CASE WHEN s.tickets > 0 THEN s.t_sin_ecuacion  / s.tickets END AS p_sin_ecuacion,
        CASE WHEN s.tickets > 0 THEN s.t_pedido_grande / s.tickets END AS p_pedido_grande
    FROM sp s
    JOIN dowp d USING (store_id, periodo_prov)
    LEFT JOIN cv c USING (store_id, periodo_prov)
)
SELECT * FROM m