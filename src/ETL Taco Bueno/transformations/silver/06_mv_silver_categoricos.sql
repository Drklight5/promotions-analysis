-- ════════════════════════════════════════════════════════════════
-- Capa SILVER (evaluación de cambios) — Taco Bueno
-- Vista materializada: mv_silver_categoricos
-- Grano: 1 fila por anio_mes
-- Fuentes: tacobueno.tickets.tickets_bronze (todo string)
--          tacobueno.tickets.tickets_silver (tipada)
--
-- Justifica el paso 6 del silver (estandarización de categóricos):
--   * Bronze: nulos de report_group (EDA: 29%, solo versión B)
--     y tender_type (EDA: 87.6%, 131 valores mixtos).
--   * Silver los etiqueta explícitamente como 'unknown' (COALESCE),
--     sin descartar filas.
--   * pct_unknown_silver ≈ pct_nulos_bronze valida que la
--     estandarización cubrió los nulos originales; crecimientos
--     adicionales vendrían de valores nuevos no parseables.
--
-- Nota: el refresco lo hace el update del pipeline SDP "ETL Taco Bueno"
-- una vez portado a la carpeta de librerías del pipeline.
-- ════════════════════════════════════════════════════════════════
CREATE OR REFRESH MATERIALIZED VIEW tacobueno.tickets.mv_silver_categoricos
-- Nota: el refresco lo hace el update del pipeline SDP "ETL Taco Bueno".
COMMENT 'Evaluación silver: nulos de report_group/tender_type en bronze vs etiqueta unknown en silver por mes.'
AS
WITH bronze AS (
    SELECT
        COALESCE(date_format(to_date(fecha, 'M/d/yyyy'), 'yyyy-MM'), 'sin_fecha') AS anio_mes,
        COUNT(*) AS filas_bronze,
        SUM(CASE WHEN report_group IS NULL
                  OR LOWER(TRIM(report_group)) IN ('', 'null', 'n/a', 'na', '-')
                 THEN 1 ELSE 0 END) AS nulos_report_group,
        SUM(CASE WHEN tender_type IS NULL
                  OR LOWER(TRIM(tender_type)) IN ('', 'null', 'n/a', 'na', '-')
                 THEN 1 ELSE 0 END) AS nulos_tender_type
    FROM tacobueno.tickets.tickets_bronze
    GROUP BY 1
),
silver AS (
    SELECT
        COALESCE(anio_mes, 'sin_fecha') AS anio_mes,
        COUNT(*) AS filas_silver,
        SUM(CASE WHEN report_group = 'unknown' THEN 1 ELSE 0 END) AS unknown_report_group,
        SUM(CASE WHEN tender_type = 'unknown' THEN 1 ELSE 0 END) AS unknown_tender_type
    FROM tacobueno.tickets.tickets_silver
    GROUP BY 1
)
SELECT
    COALESCE(b.anio_mes, s.anio_mes) AS anio_mes,
    b.filas_bronze,
    b.nulos_report_group,
    ROUND(b.nulos_report_group / NULLIF(b.filas_bronze, 0) * 100, 2) AS pct_nulos_report_group,
    s.unknown_report_group,
    ROUND(s.unknown_report_group / NULLIF(s.filas_silver, 0) * 100, 2) AS pct_unknown_report_group,
    b.nulos_tender_type,
    ROUND(b.nulos_tender_type / NULLIF(b.filas_bronze, 0) * 100, 2) AS pct_nulos_tender_type,
    s.unknown_tender_type,
    ROUND(s.unknown_tender_type / NULLIF(s.filas_silver, 0) * 100, 2) AS pct_unknown_tender_type
FROM bronze b
FULL OUTER JOIN silver s ON b.anio_mes = s.anio_mes
ORDER BY 1
