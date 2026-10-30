"""
Silver layer — Taco Bueno cleaning and enrichment.
Transforms tickets_bronze into tickets_silver.
Applies: type casting, null-column drop, dedup, menu_item rename,
row_type classification, categorical standardization, partial-day/low-coverage
flags, and temporal enrichment.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# ── Column groups (from 00_config) ──
COLS_NUMERICAS = ["total", "total_discount", "tax", "quantity", "menu_price", "item_discount"]
COLS_FECHA_HORA = ["fecha", "begin_time", "end_time"]

# Null values to treat as NULL when casting
VALORES_NULOS = ["", "NULL", "null", "N/A", "NA", "-"]
_NULL_VALUES = [v.lower() for v in VALORES_NULOS]

# Date format
FMT_FECHA = "M/d/yyyy"

# Partial days detected during EDA
PARTIAL_DAYS = ["2025-11-26", "2026-01-25"]

# Low coverage threshold
UMBRAL_MESES_MIN = 6


def clean_nulls(col_name: str):
    """Replace strings in VALORES_NULOS with NULL before casting."""
    return F.when(
        F.col(col_name).isNull() | F.lower(F.trim(F.col(col_name))).isin(_NULL_VALUES),
        F.lit(None),
    ).otherwise(F.col(col_name))


@dp.materialized_view(
    name="tickets_silver",
    comment=(
        "Silver: cleaned and typed. 1 row = 1 menu line. "
        "Derived from tickets_bronze after: drop null/constant columns, "
        "dedup zero-price/zero-qty rows, rename null menu_item, "
        "classify row_type, standardize categoricals, "
        "flag partial days and low-coverage stores, "
        "temporal enrichment."
    ),
)
def tickets_silver():
    df_bronze = spark.read.table("tickets_bronze")
    schema = df_bronze.schema

    # Metadata columns kept without casting
    META_COLS = {"_corrupt", "archivo_origen", "fecha_archivo", "ingesta_ts"}
    _HAS_TRY_CAST = hasattr(F, "try_cast")

    # ── 1. CAST TYPES ──
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

    # ── 2. DROP NULL/CONSTANT COLUMNS ──
    COLS_DROP = ["time_card", "menu_item_ext_ref", "total_rounding",
                 "_corrupt", "order_voided", "item_voided"]
    cols_to_drop = [c for c in COLS_DROP if c in df.columns]
    df = df.drop(*cols_to_drop)

    # ── 3. DEDUP ZERO-PRICE/ZERO-QTY ROWS ──
    # Only deduplicate rows where menu_price = 0 OR quantity = 0.
    # These contribute $0 to sum(qty x price), so removing duplicates
    # does not affect the ticket total. Rows with price and qty > 0
    # are all preserved.
    meta_ingesta = {"archivo_origen", "fecha_archivo", "ingesta_ts"}
    dedup_cols = [c for c in df.columns if c not in meta_ingesta]

    is_zero_row = (F.col("menu_price") == 0) | (F.col("quantity") == 0)
    df_zero = df.filter(is_zero_row)
    df_nonzero = df.filter(~is_zero_row)
    df_zero_dedup = df_zero.dropDuplicates(dedup_cols)
    df = df_nonzero.unionAll(df_zero_dedup)

    # ── 4. RENAME NULL menu_item BY pos_code PATTERN ──
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
            "MODIFIER_NO",
        )
        .when(F.col("pos_code").startswith("XT"), "MODIFIER_EXTRA")
        .otherwise("UNNAMED_ITEM"),
    )

    # ── 5. CLASSIFY row_type ──
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
        .otherwise("product"),
    )

    # ── 6. STANDARDIZE CATEGORICAL FIELDS ──
    df = (
        df
        .withColumn("report_group", F.coalesce(F.col("report_group"), F.lit("unknown")))
        .withColumn("tender_type", F.coalesce(F.col("tender_type"), F.lit("unknown")))
    )

    # ── 7. FLAG PARTIAL DAYS & LOW-COVERAGE STORES ──
    # No rows are deleted -- boolean flags are added so downstream
    # analysis can decide what to exclude per context.
    df = df.withColumn(
        "is_partial_day",
        F.col("fecha").cast("string").isin(PARTIAL_DAYS),
    )

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
        how="left",
    )
    df = df.withColumn(
        "is_low_coverage_store",
        F.coalesce(F.col("is_low_coverage_store"), F.lit(False)),
    )

    # ── 8. TEMPORAL DIMENSIONS ──
    # Combine fecha + begin_time into a transaction timestamp,
    # then extract temporal components for seasonality analysis.
    df = df.withColumn(
        "transaction_ts",
        F.to_timestamp(
            F.concat_ws(" ", F.col("fecha").cast("string"), F.col("begin_time")),
            "yyyy-MM-dd h:mm a",
        ),
    )
    df = (
        df
        .withColumn("hora", F.hour("transaction_ts"))
        .withColumn("dia_semana", F.dayofweek("transaction_ts"))   # 1=Sunday, 7=Saturday
        .withColumn("dia_mes", F.dayofmonth("transaction_ts"))
        .withColumn("mes", F.month("transaction_ts"))
        .withColumn("anio", F.year("transaction_ts"))
        .withColumn("anio_mes", F.date_format("fecha", "yyyy-MM"))
    )

    return df
