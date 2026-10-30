"""
Bronze layer — Taco Bueno raw CSV ingestion via Auto Loader.
Reads daily sales CSV files from UC volume, handles 3 header versions
(19/21/22 cols) via schema merge. All columns kept as STRING.
1 row = 1 menu line.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ── Column mapping: CSV header name -> snake_case name ──
COLUMN_MAP = {
    "Store #": "store_id",
    "Date": "fecha",
    "Total": "total",
    "Check Number": "check_number",
    "Time Card": "time_card",
    "Begin Time": "begin_time",
    "End Time": "end_time",
    "Total Discount": "total_discount",
    "Order Voided": "order_voided",
    "Tax": "tax",
    "Quantity": "quantity",
    "Transaction Type": "transaction_type",
    "Menu Item": "menu_item",
    "Menu Item Ext Ref": "menu_item_ext_ref",
    "Pop": "pop",
    "POS Code": "pos_code",
    "Menu Price": "menu_price",
    "Item Discount": "item_discount",
    "Item Voided": "item_voided",
    "Report Group": "report_group",
    "Tender Type": "tender_type",
    "Total Rounding": "total_rounding",
}

REGEX_FECHA_ARCHIVO = r"(\d{2}-\d{2}-\d{4})"
FMT_FECHA_ARCHIVO = "MM-dd-yyyy"

VOLUME_PATH = "/Volumes/tacobueno/tickets/ventas_csv"

# Schema hints ensure all 22 columns are present from the start,
# even if the initial inference sample only sees 19-column files.
SCHEMA_HINTS = (
    "`Store #` STRING, `Date` STRING, `Total` STRING, `Check Number` STRING, "
    "`Time Card` STRING, `Begin Time` STRING, `End Time` STRING, "
    "`Total Discount` STRING, `Order Voided` STRING, `Tax` STRING, "
    "`Quantity` STRING, `Transaction Type` STRING, `Menu Item` STRING, "
    "`Menu Item Ext Ref` STRING, `Pop` STRING, `POS Code` STRING, "
    "`Menu Price` STRING, `Item Discount` STRING, `Item Voided` STRING, "
    "`Report Group` STRING, `Tender Type` STRING, `Total Rounding` STRING"
)


@dp.table(
    name="tickets_bronze",
    comment=(
        "Bronze: raw CSV from Taco Bueno daily sales. All columns STRING. "
        "1 row = 1 menu line. 597 files, 3 header versions (19/21/22 cols). "
        "Auto Loader with schema merge."
    ),
)
def tickets_bronze():
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("mergeSchema", True)
        .option("cloudFiles.schemaHints", SCHEMA_HINTS)
        .load(VOLUME_PATH)
    )

    # Rename CSV header names to snake_case
    select_exprs = [
        F.col(f"`{csv_name}`").alias(target_name)
        for csv_name, target_name in COLUMN_MAP.items()
    ]

    # Capture rescued data as _corrupt (matching original bronze schema)
    if "_rescued_data" in raw.columns:
        select_exprs.append(F.col("_rescued_data").alias("_corrupt"))
    else:
        select_exprs.append(F.lit(None).cast("string").alias("_corrupt"))

    # Metadata: source file name
    select_exprs.append(F.col("_metadata.file_name").alias("archivo_origen"))

    df = raw.select(*select_exprs)

    # Extract date from filename and add ingestion timestamp
    df = (
        df
        .withColumn(
            "fecha_archivo",
            F.to_date(
                F.regexp_extract("archivo_origen", REGEX_FECHA_ARCHIVO, 1),
                FMT_FECHA_ARCHIVO,
            ),
        )
        .withColumn("ingesta_ts", F.current_timestamp())
    )

    # Consistent column order matching original bronze table
    ordered_cols = list(COLUMN_MAP.values()) + [
        "_corrupt",
        "archivo_origen",
        "fecha_archivo",
        "ingesta_ts",
    ]
    return df.select(ordered_cols)
