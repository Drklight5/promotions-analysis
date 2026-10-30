-- ════════════════════════════════════════════════════════════════
-- Capa SILVER (evaluación de cambios) — Taco Bueno
-- Vista materializada: mv_silver_reconciliacion
-- Grano: 1 fila por anio_mes
-- Fuentes: tacobueno.tickets.tickets_bronze (todo string)
--          tacobueno.tickets.tickets_silver (tipada)
--
-- Justifica el paso 3 del silver (dedup de filas con menu_price = 0
-- OR quantity = 0): los duplicados eliminados aportan $0 a
-- SUM(quantity * menu_price), por lo que el monto agregado por mes
-- debe ser IDÉNTICO entre bronze y silver; solo cambia el conteo de
-- filas (~14.3% eliminadas según EDA).
--
-- Reglas de comparación (equivalente al clean_nulls del silver):
--   * Literales nulos en bronze: '', 'NULL', 'null', 'N/A', 'NA', '-'.
--   * anio_mes de bronze se deriva con to_date(fecha, 'M/d/yyyy');
--     fechas no parseables caen en el bucket 'sin_fecha'.
--   * Valores no casteables aportan $0 en ambos lados (equivalente
--     al try_cast -> NULL del silver).
--   * dif_monto ≈ $0 en todos los meses valida que el dedup fue
--     inocuo para los montos.
--
-- Nota: el refresco lo hace el update del pipeline SDP "ETL Taco Bueno"
-- una vez portado a la carpeta de librerías del pipeline.
-- ════════════════════════════════════════════════════════════════
CREATE OR REFRESH MATERIALIZED VIEW tacobueno.tickets.mv_silver_reconciliacion
-- Nota: el refresco lo hace el update del pipeline SDP "ETL Taco Bueno".
COMMENT 'Evaluación silver: reconciliación bronze vs silver por mes. Filas eliminadas por dedup de precio/cantidad cero y monto SUM(quantity*menu_price) que debe coincidir.'
AS
WITH bronze AS (
    SELECT
        COALESCE(date_format(to_date(fecha, 'M/d/yyyy'), 'yyyy-MM'), 'sin_fecha') AS anio_mes,
        COUNT(*) AS filas_bronze,
        SUM(
            COALESCE(try_cast(quantity AS DECIMAL(20,6)), 0)
          * COALESCE(try_cast(menu_price AS DECIMAL(20,6)), 0)
        ) AS monto_bronze
    FROM tacobueno.tickets.tickets_bronze
    GROUP BY 1
),
silver AS (
    SELECT
        COALESCE(anio_mes, 'sin_fecha') AS anio_mes,
        COUNT(*) AS filas_silver,
        SUM(COALESCE(quantity, 0) * COALESCE(menu_price, 0)) AS monto_silver
    FROM tacobueno.tickets.tickets_silver
    GROUP BY 1
)
SELECT
    COALESCE(b.anio_mes, s.anio_mes) AS anio_mes,
    b.filas_bronze,
    s.filas_silver,
    b.filas_bronze - s.filas_silver AS filas_eliminadas_dedup,
    ROUND((b.filas_bronze - s.filas_silver) / NULLIF(b.filas_bronze, 0) * 100, 2) AS pct_filas_eliminadas,
    ROUND(b.monto_bronze, 2) AS monto_bronze,
    ROUND(s.monto_silver, 2) AS monto_silver,
    ROUND(b.monto_bronze - s.monto_silver, 2) AS dif_monto
FROM bronze b
FULL OUTER JOIN silver s ON b.anio_mes = s.anio_mes
ORDER BY 1
