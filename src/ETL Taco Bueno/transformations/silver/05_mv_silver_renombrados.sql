-- ════════════════════════════════════════════════════════════════
-- Capa SILVER (evaluación de cambios) — Taco Bueno
-- Vista materializada: mv_silver_renombrados
-- Grano: 1 fila por anio_mes
-- Fuentes: tacobueno.tickets.tickets_bronze (todo string)
--          tacobueno.tickets.tickets_silver (tipada)
--
-- Justifica el paso 4 del silver (renombrado de menu_item nulo):
--   * Bronze: menú nulo = NULL, '', 'none' o literal nulo
--     ('', 'NULL', 'null', 'N/A', 'NA', '-') — EDA: 5.85% de filas.
--   * Silver los renombra por patrón de pos_code:
--       pos_code empieza con 'NO' o '*No' -> MODIFIER_NO
--       pos_code empieza con 'XT'          -> MODIFIER_EXTRA
--       resto                             -> UNNAMED_ITEM
--   * renombradas_silver ≈ menu_nulo_bronze; la diferencia
--     (dif_por_dedup) son filas nulas eliminadas por el dedup de
--     precio/cantidad cero.
--
-- Nota: el refresco lo hace el update del pipeline SDP "ETL Taco Bueno"
-- una vez portado a la carpeta de librerías del pipeline.
-- ════════════════════════════════════════════════════════════════
CREATE OR REFRESH MATERIALIZED VIEW tacobueno.tickets.mv_silver_renombrados
-- Nota: el refresco lo hace el update del pipeline SDP "ETL Taco Bueno".
COMMENT 'Evaluación silver: menús nulos en bronze vs filas renombradas (MODIFIER_NO/MODIFIER_EXTRA/UNNAMED_ITEM) en silver por mes.'
AS
WITH bronze AS (
    SELECT
        COALESCE(date_format(to_date(fecha, 'M/d/yyyy'), 'yyyy-MM'), 'sin_fecha') AS anio_mes,
        COUNT(*) AS menu_nulo_bronze
    FROM tacobueno.tickets.tickets_bronze
    WHERE menu_item IS NULL
       OR LOWER(TRIM(menu_item)) IN ('', 'null', 'n/a', 'na', '-', 'none')
    GROUP BY 1
),
silver AS (
    SELECT
        COALESCE(anio_mes, 'sin_fecha') AS anio_mes,
        SUM(CASE WHEN menu_item = 'MODIFIER_NO' THEN 1 ELSE 0 END)    AS n_modifier_no,
        SUM(CASE WHEN menu_item = 'MODIFIER_EXTRA' THEN 1 ELSE 0 END) AS n_modifier_extra,
        SUM(CASE WHEN menu_item = 'UNNAMED_ITEM' THEN 1 ELSE 0 END)  AS n_unnamed_item
    FROM tacobueno.tickets.tickets_silver
    WHERE menu_item IN ('MODIFIER_NO', 'MODIFIER_EXTRA', 'UNNAMED_ITEM')
    GROUP BY 1
)
SELECT
    COALESCE(b.anio_mes, s.anio_mes) AS anio_mes,
    b.menu_nulo_bronze,
    s.n_modifier_no,
    s.n_modifier_extra,
    s.n_unnamed_item,
    COALESCE(s.n_modifier_no, 0) + COALESCE(s.n_modifier_extra, 0) + COALESCE(s.n_unnamed_item, 0) AS renombradas_silver,
    b.menu_nulo_bronze
      - (COALESCE(s.n_modifier_no, 0) + COALESCE(s.n_modifier_extra, 0) + COALESCE(s.n_unnamed_item, 0)) AS dif_por_dedup
FROM bronze b
FULL OUTER JOIN silver s ON b.anio_mes = s.anio_mes
ORDER BY 1
