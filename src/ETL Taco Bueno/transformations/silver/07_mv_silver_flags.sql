-- ════════════════════════════════════════════════════════════════
-- Capa SILVER (evaluación de cambios) — Taco Bueno
-- Vista materializada: mv_silver_flags
-- Grano: 1 fila por store_id
-- Fuentes: tacobueno.tickets.tickets_bronze (todo string)
--          tacobueno.tickets.tickets_silver (tipada)
--
-- Justifica el paso 7 del silver (banderas, sin eliminar filas):
--   * is_partial_day: días con datos incompletos detectados en EDA
--     (2025-11-26 y 2026-01-25). Aquí se reporta cuántos días
--     parciales afectan a cada tienda.
--   * is_low_coverage_store: tiendas con < 6 meses de datos
--     (UMBRAL_MESES_MIN del silver). meses_bronze (de la fuente)
--     vs meses_silver debe coincidir: prueba que la cobertura se
--     calculó sobre los mismos datos y que las banderas no eliminan
--     información.
--
-- Nota: el refresco lo hace el update del pipeline SDP "ETL Taco Bueno"
-- una vez portado a la carpeta de librerías del pipeline.
-- ════════════════════════════════════════════════════════════════
CREATE OR REFRESH MATERIALIZED VIEW tacobueno.tickets.mv_silver_flags
-- Nota: el refresco lo hace el update del pipeline SDP "ETL Taco Bueno".
COMMENT 'Evaluación silver: cobertura de meses por tienda bronze vs silver, bandera de baja cobertura (<6 meses) y días parciales por tienda.'
AS
WITH bronze AS (
    SELECT
        store_id,
        COUNT(DISTINCT date_format(to_date(fecha, 'M/d/yyyy'), 'yyyy-MM')) AS meses_bronze,
        MIN(date_format(to_date(fecha, 'M/d/yyyy'), 'yyyy-MM'))           AS primer_mes_bronze,
        MAX(date_format(to_date(fecha, 'M/d/yyyy'), 'yyyy-MM'))           AS ultimo_mes_bronze
    FROM tacobueno.tickets.tickets_bronze
    GROUP BY store_id
),
silver AS (
    SELECT
        store_id,
        COUNT(DISTINCT anio_mes)                                    AS meses_silver,
        COUNT(*)                                                    AS filas_silver,
        COALESCE(BOOL_OR(is_low_coverage_store), FALSE)             AS baja_cobertura,
        COUNT(DISTINCT CASE WHEN is_partial_day THEN fecha END)      AS dias_parciales
    FROM tacobueno.tickets.tickets_silver
    GROUP BY store_id
)
SELECT
    COALESCE(b.store_id, s.store_id) AS store_id,
    b.meses_bronze,
    s.meses_silver,
    b.meses_bronze - s.meses_silver AS dif_meses,
    s.baja_cobertura,
    s.dias_parciales,
    s.filas_silver,
    b.primer_mes_bronze,
    b.ultimo_mes_bronze
FROM bronze b
FULL OUTER JOIN silver s ON b.store_id = s.store_id
ORDER BY store_id
