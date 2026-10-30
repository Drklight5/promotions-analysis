-- ════════════════════════════════════════════════════════════════
-- Capa SILVER (evaluación de cambios) — Taco Bueno
-- Vista materializada: mv_silver_dq_columnas
-- Grano: 1 fila por columna evaluada
-- Columnas: COLS_NUMERICAS (total, total_discount, tax, quantity,
--           menu_price, item_discount) + fecha
-- Fuentes: tacobueno.tickets.tickets_bronze (todo string)
--          tacobueno.tickets.tickets_silver (tipada)
--
-- Justifica el paso 1 del silver (tipado con try_cast):
--   * nulos_bronze: cuántos valores eran literales nulos en bronze
--     ('', 'NULL', 'null', 'N/A', 'NA', '-') — equivalente al
--     clean_nulls del silver.
--   * fallos_casteo: cuántos valores NO vacíos fallan el casteo
--     (números inválidos para DECIMAL(20,6) o fechas inválidas para
--     'M/d/yyyy'). Estos son los valores que el try_cast convierte
--     en NULL.
--   * nulos_silver: nulos resultantes en la tabla tipada.
--     Esperado: nulos_silver ≈ nulos_bronze + fallos_casteo
--     (modulado por las filas que elimina el dedup).
--   * valor_min / valor_max: rango sanity-check de la columna tipada.
--
-- Nota: el refresco lo hace el update del pipeline SDP "ETL Taco Bueno"
-- una vez portado a la carpeta de librerías del pipeline.
-- ════════════════════════════════════════════════════════════════
CREATE OR REFRESH MATERIALIZED VIEW tacobueno.tickets.mv_silver_dq_columnas
-- Nota: el refresco lo hace el update del pipeline SDP "ETL Taco Bueno".
COMMENT 'Evaluación silver: perfil de calidad por columna — nulos antes (bronze), fallos de casteo y nulos después (silver tipada).'
AS
WITH bronze_largo AS (
    SELECT columna, valor
    FROM (
        SELECT total, total_discount, tax, quantity, menu_price, item_discount, fecha
        FROM tacobueno.tickets.tickets_bronze
    )
    UNPIVOT INCLUDE NULLS (valor FOR columna IN
        (total, total_discount, tax, quantity, menu_price, item_discount, fecha))
),
bronze_stats AS (
    SELECT
        columna,
        COUNT(*) AS filas_bronze,
        SUM(CASE WHEN valor IS NULL
                  OR LOWER(TRIM(valor)) IN ('', 'null', 'n/a', 'na', '-')
                 THEN 1 ELSE 0 END) AS nulos_bronze,
        SUM(CASE WHEN valor IS NOT NULL
                  AND LOWER(TRIM(valor)) NOT IN ('', 'null', 'n/a', 'na', '-')
                  AND CASE WHEN columna = 'fecha'
                           THEN to_date(valor, 'M/d/yyyy') IS NULL
                           ELSE try_cast(valor AS DECIMAL(20,6)) IS NULL
                      END
                 THEN 1 ELSE 0 END) AS fallos_casteo
    FROM bronze_largo
    GROUP BY columna
),
silver_stats AS (
    SELECT 'total' AS columna, COUNT(*) AS filas_silver,
           SUM(CASE WHEN total IS NULL THEN 1 ELSE 0 END) AS nulos_silver,
           CAST(MIN(total) AS STRING) AS valor_min, CAST(MAX(total) AS STRING) AS valor_max
    FROM tacobueno.tickets.tickets_silver
    UNION ALL
    SELECT 'total_discount', COUNT(*),
           SUM(CASE WHEN total_discount IS NULL THEN 1 ELSE 0 END),
           CAST(MIN(total_discount) AS STRING), CAST(MAX(total_discount) AS STRING)
    FROM tacobueno.tickets.tickets_silver
    UNION ALL
    SELECT 'tax', COUNT(*),
           SUM(CASE WHEN tax IS NULL THEN 1 ELSE 0 END),
           CAST(MIN(tax) AS STRING), CAST(MAX(tax) AS STRING)
    FROM tacobueno.tickets.tickets_silver
    UNION ALL
    SELECT 'quantity', COUNT(*),
           SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END),
           CAST(MIN(quantity) AS STRING), CAST(MAX(quantity) AS STRING)
    FROM tacobueno.tickets.tickets_silver
    UNION ALL
    SELECT 'menu_price', COUNT(*),
           SUM(CASE WHEN menu_price IS NULL THEN 1 ELSE 0 END),
           CAST(MIN(menu_price) AS STRING), CAST(MAX(menu_price) AS STRING)
    FROM tacobueno.tickets.tickets_silver
    UNION ALL
    SELECT 'item_discount', COUNT(*),
           SUM(CASE WHEN item_discount IS NULL THEN 1 ELSE 0 END),
           CAST(MIN(item_discount) AS STRING), CAST(MAX(item_discount) AS STRING)
    FROM tacobueno.tickets.tickets_silver
    UNION ALL
    SELECT 'fecha', COUNT(*),
           SUM(CASE WHEN fecha IS NULL THEN 1 ELSE 0 END),
           CAST(MIN(fecha) AS STRING), CAST(MAX(fecha) AS STRING)
    FROM tacobueno.tickets.tickets_silver
)
SELECT
    b.columna,
    b.filas_bronze,
    b.nulos_bronze,
    ROUND(b.nulos_bronze / b.filas_bronze * 100, 2) AS pct_nulos_bronze,
    b.fallos_casteo,
    ROUND(b.fallos_casteo / b.filas_bronze * 100, 2) AS pct_fallos_casteo,
    s.filas_silver,
    s.nulos_silver,
    ROUND(s.nulos_silver / s.filas_silver * 100, 2) AS pct_nulos_silver,
    s.valor_min,
    s.valor_max
FROM bronze_stats b
FULL OUTER JOIN silver_stats s ON b.columna = s.columna
ORDER BY b.columna
