-- ════════════════════════════════════════════════════════════════
-- Capa GOLD — Taco Bueno
-- Vista materializada: mv_tickets_base
-- Grano: 1 fila por ticket (ticket_id = store_id|fecha|check_number)
-- Fuente: tacobueno.tickets.tickets_silver (1 fila = 1 línea de menú)
-- Origen de la lógica: notebook 000_promos, celda "Crear tickets_base"
-- (validada con V1-V7 en el propio notebook).
--
-- Reglas de negocio (confirmadas con el usuario):
--   * Campos de ticket (total, total_discount, tax, pop, begin_time):
--     MAX() por ticket; ~0.0005% tiene >1 valor y se marca flag_inconsistente.
--   * promo_td    = total_discount > 0 (única medida comparable en toda la serie).
--   * promo_adj   = líneas price_adjustment con monto negativo.
--   * tiene_promo = promo_td OR promo_adj.
--   * flag_total_cero = total = 0 AND sub_prod > 0 (tickets comped de store 5026).
--   * sin_ecuacion = no cumple ninguna ecuación E1-E5 (tolerancia $0.015).
--   * periodo: P1 (< 2025-11-01), P2 (2025-11-01 a 2026-01-31), P3 (>= 2026-02-01).
-- ════════════════════════════════════════════════════════════════
CREATE OR REPLACE MATERIALIZED VIEW tacobueno.gold.mv_tickets_base
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 DAY
COMMENT 'Gold: 1 fila por ticket. Agregado de líneas de menú con banderas de calidad y promoción. Fuente: tickets_silver.'
AS
WITH ticket_agg AS (
    SELECT
        CONCAT_WS('|', store_id, CAST(fecha AS STRING), check_number) AS ticket_id,
        store_id,
        fecha,
        check_number,
        anio_mes,
        MAX(hora)                                          AS hora,
        MAX(begin_time)                                    AS begin_time,
        MIN(transaction_ts)                                AS ts_ticket,
        MAX(dia_semana)                                    AS dia_semana_orig,
        MAX(LENGTH(check_number))                          AS len_check_number,
        TRIM(MAX(pop))                                     AS pop,
        MAX(total)                                         AS total,
        MAX(total_discount)                                AS total_discount,
        MAX(tax)                                           AS tax,
        COALESCE(SUM(CASE WHEN row_type = 'product' THEN quantity * menu_price ELSE 0 END), 0) AS sub_prod,
        COALESCE(SUM(CASE WHEN row_type IN ('donation','delivery_fee','tip') THEN quantity * menu_price ELSE 0 END), 0) AS sub_otros,
        COALESCE(SUM(CASE WHEN row_type = 'price_adjustment' THEN quantity * menu_price ELSE 0 END), 0) AS adj,
        COALESCE(SUM(CASE WHEN row_type = 'product' THEN item_discount ELSE 0 END), 0) AS item_disc_prod,
        COALESCE(SUM(item_discount), 0)                    AS item_disc_all,
        COALESCE(SUM(CASE WHEN row_type = 'price_adjustment' AND LOWER(regexp_extract(pos_code, '[(]([^)]+)[)]$', 1)) = 'coupon' THEN quantity * menu_price ELSE 0 END), 0) AS adj_coupon,
        COALESCE(SUM(CASE WHEN row_type = 'price_adjustment' AND LOWER(regexp_extract(pos_code, '[(]([^)]+)[)]$', 1)) = 'orderreduction' THEN quantity * menu_price ELSE 0 END), 0) AS adj_orderreduction,
        COALESCE(SUM(CASE WHEN row_type = 'price_adjustment' AND LOWER(regexp_extract(pos_code, '[(]([^)]+)[)]$', 1)) = 'bogo' THEN quantity * menu_price ELSE 0 END), 0) AS adj_bogo,
        SUM(CASE WHEN row_type = 'price_adjustment' AND LOWER(regexp_extract(pos_code, '[(]([^)]+)[)]$', 1)) = 'coupon' THEN 1 ELSE 0 END) AS n_adj_coupon,
        SUM(CASE WHEN row_type = 'price_adjustment' AND LOWER(regexp_extract(pos_code, '[(]([^)]+)[)]$', 1)) = 'orderreduction' THEN 1 ELSE 0 END) AS n_adj_orderreduction,
        SUM(CASE WHEN row_type = 'price_adjustment' AND LOWER(regexp_extract(pos_code, '[(]([^)]+)[)]$', 1)) = 'bogo' THEN 1 ELSE 0 END) AS n_adj_bogo,
        SUM(CASE WHEN row_type = 'price_adjustment' THEN 1 ELSE 0 END) AS n_adj,
        COALESCE(SUM(CASE WHEN row_type = 'product' THEN quantity ELSE 0 END), 0) AS n_items,
        MAX(CASE WHEN row_type = 'product' THEN quantity END) AS max_qty,
        COUNT(*) AS n_lineas,
        SUM(CASE WHEN row_type = 'product' THEN 1 ELSE 0 END) AS n_lineas_prod,
        SUM(CASE WHEN row_type = 'modifier' THEN 1 ELSE 0 END) AS n_modifier,
        SUM(CASE WHEN row_type = 'void' THEN 1 ELSE 0 END) AS n_void,
        SUM(CASE WHEN row_type = 'donation' THEN 1 ELSE 0 END) AS n_donation,
        SUM(CASE WHEN row_type = 'delivery_fee' THEN 1 ELSE 0 END) AS n_delivery_fee,
        SUM(CASE WHEN row_type = 'tip' THEN 1 ELSE 0 END) AS n_tip,
        SUM(CASE WHEN transaction_type = 'Discount' THEN 1 ELSE 0 END) AS n_lineas_discount,
        COALESCE(BOOL_OR(is_partial_day), FALSE) AS is_partial_day,
        COALESCE(BOOL_OR(is_low_coverage_store), FALSE) AS is_low_coverage_store,
        COUNT(DISTINCT total)          AS n_distintos_total,
        COUNT(DISTINCT tax)            AS n_distintos_tax,
        COUNT(DISTINCT pop)            AS n_distintos_pop,
        COUNT(DISTINCT begin_time)     AS n_distintos_begin_time,
        COUNT(DISTINCT total_discount) AS n_distintos_total_discount
    FROM tacobueno.tickets.tickets_silver
    GROUP BY CONCAT_WS('|', store_id, CAST(fecha AS STRING), check_number), store_id, fecha, check_number, anio_mes
)
SELECT
    ticket_id,
    store_id,
    fecha,
    check_number,
    anio_mes,
    hora,
    begin_time,
    ts_ticket,
    dia_semana_orig,
    DAYOFWEEK(fecha) AS dow_spark,
    len_check_number,
    pop,
    CASE
        WHEN pop = 'Drive Thru' THEN 'drive_thru'
        WHEN pop IN ('To Go', 'Carry Out') THEN 'para_llevar'
        WHEN pop IN ('Inside', 'Dine-In') THEN 'salon'
        WHEN pop IN ('Mobile', 'Online') THEN 'digital_propio'
        WHEN pop IN ('Online: DoorDash', 'Online: UberEats', 'Online: GrubHub', 'Online: Zipline') THEN 'delivery_terceros'
        WHEN pop = '3' THEN 'desconocido'
        ELSE 'otro'
    END AS canal_grupo,
    total,
    total_discount,
    tax,
    sub_prod,
    sub_otros,
    adj,
    item_disc_prod,
    item_disc_all,
    adj_coupon,
    adj_orderreduction,
    adj_bogo,
    adj - adj_coupon - adj_orderreduction - adj_bogo AS adj_otro,
    n_adj_coupon,
    n_adj_orderreduction,
    n_adj_bogo,
    n_adj - n_adj_coupon - n_adj_orderreduction - n_adj_bogo AS n_adj_otro,
    n_adj,
    sub_prod + adj - total_discount AS venta_neta_prod,
    total_discount + ABS(adj) AS monto_desc_total,
    n_items,
    max_qty,
    n_lineas,
    n_lineas_prod,
    n_modifier,
    n_void,
    n_donation,
    n_delivery_fee,
    n_tip,
    n_lineas_discount,
    total_discount > 0 AS promo_td,
    n_adj > 0 AND adj < 0 AS promo_adj,
    (total_discount > 0) OR (n_adj > 0 AND adj < 0) AS tiene_promo,
    total - (sub_prod + sub_otros + adj) AS d_e1,
    total - (sub_prod + sub_otros + adj + tax - total_discount) AS d_e3,
    ABS(total - (sub_prod + sub_otros + adj)) < 0.015 AS cumple_e1,
    ABS(total - (sub_prod + sub_otros + adj + tax - total_discount)) < 0.015 AS cumple_e3,
    (
        ABS(total - (sub_prod + sub_otros + adj)) < 0.015
        OR ABS(total - (sub_prod + sub_otros + adj + tax)) < 0.015
        OR ABS(total - (sub_prod + sub_otros + adj + tax - total_discount)) < 0.015
        OR ABS(total - (sub_prod + sub_otros + adj - total_discount)) < 0.015
        OR ABS(total - (sub_prod + sub_otros - item_disc_prod)) < 0.015
    ) AS cumple_alguna,
    NOT (
        ABS(total - (sub_prod + sub_otros + adj)) < 0.015
        OR ABS(total - (sub_prod + sub_otros + adj + tax)) < 0.015
        OR ABS(total - (sub_prod + sub_otros + adj + tax - total_discount)) < 0.015
        OR ABS(total - (sub_prod + sub_otros + adj - total_discount)) < 0.015
        OR ABS(total - (sub_prod + sub_otros - item_disc_prod)) < 0.015
    ) AS sin_ecuacion,
    CASE WHEN n_distintos_total > 1
              OR n_distintos_tax > 1
              OR n_distintos_pop > 1
              OR n_distintos_begin_time > 1
              OR n_distintos_total_discount > 1
         THEN 1 ELSE 0 END AS flag_inconsistente,
    CASE WHEN total = 0 AND sub_prod > 0 THEN 1 ELSE 0 END AS flag_total_cero,
    is_partial_day,
    is_low_coverage_store,
    CASE
        WHEN fecha < '2025-11-01' THEN 'P1'
        WHEN fecha >= '2025-11-01' AND fecha <= '2026-01-31' THEN 'P2'
        WHEN fecha >= '2026-02-01' THEN 'P3'
    END AS periodo
FROM ticket_agg