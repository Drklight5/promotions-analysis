"""
Silver ETL — Taco Bueno
Transforms tacobueno.tickets.tickets_bronze into tacobueno.tickets.tickets_silver.
Applies: type casting, null-column drop, dedup, menu_item rename, row_type classification,
categorical standardization, partial-day/low-coverage flags, and temporal enrichment.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# ════════════════════════════════════════════════════════════════
# CONFIG (from 00_config)
# ════════════════════════════════════════════════════════════════
CATALOG = "tacobueno"
SCHEMA  = "tickets"

def t(nombre: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{nombre}"

T_BRONZE = t("tickets_bronze")
T_SILVER = t("tickets_silver")

# Column groups
COLS_NUMERICAS   = ["total", "total_discount", "tax", "quantity", "menu_price", "item_discount"]
COLS_CATEGORICAS = ["store_id", "transaction_type", "order_voided", "item_voided", "pop", "pos_code"]
COLS_FECHA_HORA  = ["fecha", "begin_time", "end_time"]
LLAVE_TICKET     = ["store_id", "fecha", "check_number"]

# Formats
FMT_FECHA = "M/d/yyyy"
FMT_HORA  = "h:mm a"

# Null values to treat as NULL when casting
VALORES_NULOS = ["", "NULL", "null", "N/A", "NA", "-"]

# Partial days detected during EDA
PARTIAL_DAYS = ["2025-11-26", "2026-01-25"]

# Low coverage threshold
UMBRAL_MESES_MIN = 6

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# ════════════════════════════════════════════════════════════════
# 1. LOAD BRONZE & CAST TYPES
# ════════════════════════════════════════════════════════════════
print("─" * 60)
print("  SILVER ETL — Limpieza y tipado")
print("─" * 60)
print(f"  Source: {T_BRONZE}")
print(f"  Target: {T_SILVER}")
print("─" * 60)

df_bronze = spark.table(T_BRONZE)
schema = df_bronze.schema

# Metadata columns kept without casting
META_COLS = {"_corrupt", "archivo_origen", "fecha_archivo", "ingesta_ts"}

_NULL_VALUES = [v.lower() for v in VALORES_NULOS]

def clean_nulls(col_name: str):
    """Replace strings in VALORES_NULOS with NULL before casting."""
    return F.when(
        F.col(col_name).isNull() | F.lower(F.trim(F.col(col_name))).isin(_NULL_VALUES),
        F.lit(None)
    ).otherwise(F.col(col_name))

_HAS_TRY_CAST = hasattr(F, 'try_cast')
select_exprs = []

for field in schema.fields:
    name = field.name

    if name in META_COLS:
        select_exprs.append(F.col(name))
        continue

    if name == "fecha":
        select_exprs.append(F.to_date(clean_nulls(name), FMT_FECHA).alias(name))
    elif name in COLS_FECHA_HORA and name != "fecha":
        select_exprs.append(clean_nulls(name).alias(name))
    elif name in COLS_NUMERICAS:
        if _HAS_TRY_CAST:
            select_exprs.append(F.try_cast(clean_nulls(name), DecimalType(20, 6)).alias(name))
        else:
            select_exprs.append(clean_nulls(name).cast(DecimalType(20, 6)).alias(name))
    else:
        select_exprs.append(clean_nulls(name).alias(name))

df = df_bronze.select(*select_exprs)

n_rows_initial = df.count()
n_cols_initial = len(df.columns)
print(f"Tabla bronze cargada: {T_BRONZE}")
print(f"  Filas:    {n_rows_initial:,}")
print(f"  Columnas: {n_cols_initial}")

# ════════════════════════════════════════════════════════════════
# 2. DROP NULL/CONSTANT COLUMNS
# ════════════════════════════════════════════════════════════════
COLS_DROP = ["time_card", "menu_item_ext_ref", "total_rounding",
             "_corrupt", "order_voided", "item_voided"]

cols_to_drop = [c for c in COLS_DROP if c in df.columns]
df = df.drop(*cols_to_drop)

n_cols_after_drop = len(df.columns)
print(f"\nColumnas eliminadas: {len(cols_to_drop)} -> {cols_to_drop}")
print(f"Columnas restantes:  {n_cols_after_drop}")

# ════════════════════════════════════════════════════════════════
# 3. DEDUP ZERO-PRICE/ZERO-QTY ROWS
# ════════════════════════════════════════════════════════════════
# Only deduplicate rows where menu_price = 0 OR quantity = 0.
# These contribute $0 to sum(qty x price), so removing duplicates
# does not affect the ticket total. Rows with price and qty > 0
# are all preserved.

meta_ingesta = {"archivo_origen", "fecha_archivo", "ingesta_ts"}
dedup_cols = [c for c in df.columns if c not in meta_ingesta]

is_zero_row = (F.col("menu_price") == 0) | (F.col("quantity") == 0)

df_zero = df.filter(is_zero_row)
df_nonzero = df.filter(~is_zero_row)

n_before_dedup = df.count()
n_nonzero = df_nonzero.count()
n_zero_before = df_zero.count()

df_zero_dedup = df_zero.dropDuplicates(dedup_cols)
n_zero_after = df_zero_dedup.count()

df = df_nonzero.unionAll(df_zero_dedup)

n_after_dedup = df.count()
n_removed_dedup = n_before_dedup - n_after_dedup

print(f"\nDedup de filas con precio/qty = 0:")
print(f"  Filas antes:          {n_before_dedup:,}")
print(f"  Con precio/qty > 0:   {n_nonzero:,} (conservadas)")
print(f"  Con precio/qty = 0:   {n_zero_before:,} -> {n_zero_after:,} tras dedup")
print(f"  Eliminadas:           {n_removed_dedup:,} ({n_removed_dedup/n_before_dedup*100:.1f}%)")
print(f"  Filas despues:        {n_after_dedup:,}")

# ════════════════════════════════════════════════════════════════
# 4. RENAME NULL menu_item BY pos_code PATTERN
# ════════════════════════════════════════════════════════════════
n_before_menu = df.count()

is_menu_null = (
    F.col("menu_item").isNull()
    | (F.trim(F.col("menu_item")) == "")
    | (F.lower(F.trim(F.col("menu_item"))) == "none")
)

df = df.withColumn(
    "menu_item",
    F.when(~is_menu_null, F.col("menu_item"))
    .when(
        F.col("pos_code").startswith("NO") | F.col("pos_code").startswith("*No"),
        "MODIFIER_NO")
    .when(F.col("pos_code").startswith("XT"), "MODIFIER_EXTRA")
    .otherwise("UNNAMED_ITEM")
)

n_renamed = df.filter(
    F.col("menu_item").isin("MODIFIER_NO", "MODIFIER_EXTRA", "UNNAMED_ITEM")
).count()
print(f"\nmenu_item renombrado: {n_renamed:,} filas")

# ════════════════════════════════════════════════════════════════
# 5. CLASSIFY row_type
# ════════════════════════════════════════════════════════════════
# Classifies each row so downstream analysis can filter by purpose.
# Order matters: price_adjustment is evaluated before modifier so
# negative-price rows don't fall into the modifier bucket.

df = df.withColumn(
    "row_type",
    F.when(F.col("transaction_type") == "Void", "void")
    .when(F.col("menu_item").rlike("(?i)donation"), "donation")
    .when(F.col("menu_item").rlike(r"(?i)\btip\b"), "tip")
    .when(F.col("menu_item").rlike("(?i)delivery"), "delivery_fee")
    .when((F.col("menu_price") < 0) & (F.col("quantity") > 0), "price_adjustment")
    .when((F.col("menu_price") == 0) | (F.col("quantity") == 0), "modifier")
    .otherwise("product")
)

print("\n=== Distribucion de row_type ===")
df.groupBy("row_type").count().orderBy(F.desc("count")).show()

# ════════════════════════════════════════════════════════════════
# 6. STANDARDIZE CATEGORICAL FIELDS
# ════════════════════════════════════════════════════════════════
df = (df
    .withColumn("report_group", F.coalesce(F.col("report_group"), F.lit("unknown")))
    .withColumn("tender_type", F.coalesce(F.col("tender_type"), F.lit("unknown")))
)
print("\nCategoricos estandarizados: report_group y tender_type nulos -> 'unknown'")

# ════════════════════════════════════════════════════════════════
# 7. FLAG PARTIAL DAYS & LOW-COVERAGE STORES
# ════════════════════════════════════════════════════════════════
# No rows are deleted -- boolean flags are added so downstream
# analysis can decide what to exclude per context.

df = df.withColumn(
    "is_partial_day",
    F.col("fecha").cast("string").isin(PARTIAL_DAYS)
)

n_partial = df.filter(F.col("is_partial_day")).count()
print(f"\nDias parciales marcados: {PARTIAL_DAYS}")
print(f"  Filas marcadas: {n_partial:,}")

# Low-coverage stores: < UMBRAL_MESES_MIN months of data
meses_por_tienda = (
    df.withColumn("anio_mes", F.date_format("fecha", "yyyy-MM"))
      .groupBy("store_id")
      .agg(F.countDistinct("anio_mes").alias("n_meses"))
)

tiendas_baja = meses_por_tienda.filter(F.col("n_meses") < UMBRAL_MESES_MIN).select("store_id")

df = df.join(
    tiendas_baja.withColumn("is_low_coverage_store", F.lit(True)),
    on="store_id",
    how="left"
)
df = df.withColumn(
    "is_low_coverage_store",
    F.coalesce(F.col("is_low_coverage_store"), F.lit(False))
)

n_low_cov = df.filter(F.col("is_low_coverage_store")).count()
n_tiendas_low = df.filter(F.col("is_low_coverage_store")).select(F.countDistinct("store_id")).collect()[0][0]
n_tiendas_total = df.select(F.countDistinct("store_id")).collect()[0][0]
print(f"Tiendas de baja cobertura (<{UMBRAL_MESES_MIN} meses): {n_tiendas_low} de {n_tiendas_total}")
print(f"  Filas marcadas: {n_low_cov:,}")

# ════════════════════════════════════════════════════════════════
# 8. TEMPORAL DIMENSIONS
# ════════════════════════════════════════════════════════════════
# Combine fecha + begin_time into a transaction timestamp,
# then extract temporal components for seasonality analysis.

df = df.withColumn(
    "transaction_ts",
    F.to_timestamp(
        F.concat_ws(" ", F.col("fecha").cast("string"), F.col("begin_time")),
        "yyyy-MM-dd h:mm a"
    )
)

df = (df
    .withColumn("hora", F.hour("transaction_ts"))
    .withColumn("dia_semana", F.dayofweek("transaction_ts"))   # 1=Sunday, 7=Saturday
    .withColumn("dia_mes", F.dayofmonth("transaction_ts"))
    .withColumn("mes", F.month("transaction_ts"))
    .withColumn("anio", F.year("transaction_ts"))
    .withColumn("anio_mes", F.date_format("fecha", "yyyy-MM"))
)

n_ts_nulls = df.filter(F.col("transaction_ts").isNull()).count()
print(f"\nTimestamps creados. Nulos en transaction_ts: {n_ts_nulls:,}")
print("Columnas temporales: hora, dia_semana, dia_mes, mes, anio, anio_mes")

# ════════════════════════════════════════════════════════════════
# 9. WRITE TO DELTA TABLE
# ════════════════════════════════════════════════════════════════
n_final = df.count()
n_cols_final = len(df.columns)

(df.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(T_SILVER))

spark.sql(f"""COMMENT ON TABLE {T_SILVER} IS
 'Silver: limpia y tipada. 1 fila = 1 linea de menu.
  Derivada de tickets_bronze tras: drop de columnas nulas/constantes,
  dedup solo filas con precio o cantidad cero (preserva total),
  renombrado de menu_item nulo, clasificacion row_type,
  estandarizacion de categoricos, bandera de dias parciales y
  tiendas de baja cobertura (sin eliminar filas),
  enriquecimiento temporal. Generado por etl/02_silver.py.'""")

# ════════════════════════════════════════════════════════════════
# 10. SUMMARY
# ════════════════════════════════════════════════════════════════
print("=" * 60)
print("  RESUMEN DE LIMPIEZA")
print("=" * 60)
print(f"  Filas iniciales (bronze):   {n_rows_initial:>15,}")
print(f"  Filas tras dedup:           {n_after_dedup:>15,}")
print(f"  Filas renombradas (menu):   {n_renamed:>15,}")
print(f"  Filas finales (silver):     {n_final:>15,}")
print(f"  Columnas iniciales:         {n_cols_initial:>15}")
print(f"  Columnas finales:           {n_cols_final:>15}")
print(f"  Reduccion de filas:         {(1 - n_final/n_rows_initial)*100:>14.1f}%")
print("=" * 60)
print(f"  Tabla: {T_SILVER}")
