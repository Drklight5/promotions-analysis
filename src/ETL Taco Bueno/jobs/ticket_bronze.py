"""
Bronze ETL — Taco Bueno
Ingests raw CSV files from UC Volume into tacobueno.tickets.tickets_bronze.
All columns stored as strings; 3 header versions (19, 21, 22 cols) unified.
"""

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# ════════════════════════════════════════════════════════════════
# CONFIG (from 00_config)
# ════════════════════════════════════════════════════════════════
CATALOG = "tacobueno"
SCHEMA  = "tickets"
VOLUME  = "ventas_csv"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

def t(nombre: str) -> str:
    """Fully qualified table name: t('tickets_bronze') -> tacobueno.tickets.tickets_bronze"""
    return f"{CATALOG}.{SCHEMA}.{nombre}"

T_BRONZE     = t("tickets_bronze")
T_INVENTARIO = t("ingesta_archivos")

# Column mapping (original CSV header -> snake_case)
COLUMN_MAP = {
    "Store #": "store_id", "Date": "fecha", "Total": "total",
    "Check Number": "check_number", "Time Card": "time_card",
    "Begin Time": "begin_time", "End Time": "end_time",
    "Total Discount": "total_discount", "Order Voided": "order_voided",
    "Tax": "tax", "Quantity": "quantity", "Transaction Type": "transaction_type",
    "Menu Item": "menu_item", "Menu Item Ext Ref": "menu_item_ext_ref",
    "Pop": "pop", "POS Code": "pos_code", "Menu Price": "menu_price",
    "Item Discount": "item_discount", "Item Voided": "item_voided",
    "Report Group": "report_group", "Tender Type": "tender_type",
    "Total Rounding": "total_rounding",
}

N_ARCHIVOS_ESPERADO = 597
REGEX_FECHA_ARCHIVO = r"(\d{2}-\d{2}-\d{4})"
FMT_FECHA_ARCHIVO   = "MM-dd-yyyy"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# ════════════════════════════════════════════════════════════════
# 1. FILE INVENTORY & HEADER DETECTION
# ════════════════════════════════════════════════════════════════
print("─" * 60)
print("  BRONZE ETL — Ingesta de CSV crudos")
print("─" * 60)
print(f"  Volume: {VOLUME_PATH}")
print(f"  Target: {T_BRONZE}")
print("─" * 60)

archivos = [f for f in dbutils.fs.ls(VOLUME_PATH) if f.name.lower().endswith(".csv")]
print(f"CSV encontrados: {len(archivos)} (esperado {N_ARCHIVOS_ESPERADO})")
print(f"Tamaño total: {sum(f.size for f in archivos)/1e9:.2f} GB")
assert len(archivos) == N_ARCHIVOS_ESPERADO, "El número de archivos no coincide"

def leer_header(path):
    txt = dbutils.fs.head(path, 2000)
    return txt.lstrip("\ufeff").splitlines()[0].strip() if txt else ""

info = [(f.path, f.name, f.size, leer_header(f.path)) for f in archivos]

# Verify all columns are mapped
cols_en_archivos = {c.strip() for _, _, _, h in info for c in h.split(",")}
desconocidas = cols_en_archivos - set(COLUMN_MAP.keys())
assert not desconocidas, f"Columnas no mapeadas: {desconocidas}"

# Build inventory DataFrame
inv = (spark.createDataFrame(info, "path string, name string, size long, header string")
       .withColumn("n_cols", F.size(F.split("header", ",")))
       .withColumn("fecha_archivo", F.to_date(
           F.regexp_extract("name", REGEX_FECHA_ARCHIVO, 1), FMT_FECHA_ARCHIVO)))

print(f"\nArchivos con fecha no interpretable: {inv.filter('fecha_archivo IS NULL').count()}")
inv.groupBy("n_cols").agg(
    F.count("*").alias("archivos"),
    F.min("fecha_archivo").alias("desde"),
    F.max("fecha_archivo").alias("hasta"),
).orderBy("n_cols").show()

# ════════════════════════════════════════════════════════════════
# 2. READ BY HEADER GROUP & UNION
# ════════════════════════════════════════════════════════════════
grupos = {}
for path, name, size, header in info:
    grupos.setdefault(header, []).append(path)
print(f"Versiones de encabezado: {len(grupos)}")

dfs = []
for header, paths in grupos.items():
    cols = [c.strip() for c in header.split(",")]
    schema = StructType(
        [StructField(c, StringType(), True) for c in cols]
        + [StructField("_corrupt", StringType(), True)]
    )
    g = (spark.read
         .option("header", True)
         .option("enforceSchema", False)
         .option("mode", "PERMISSIVE")
         .option("columnNameOfCorruptRecord", "_corrupt")
         .schema(schema)
         .csv(paths)
         .withColumn("archivo_origen", F.col("_metadata.file_name")))
    g = g.select(
        [F.col(f"`{c}`").alias(COLUMN_MAP[c]) for c in cols]
        + ["_corrupt", "archivo_origen"]
    )
    dfs.append(g)
    print(f"  {len(cols)} columnas -> {len(paths)} archivos")

df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

# Reorder columns + add metadata
orden = list(COLUMN_MAP.values()) + ["_corrupt", "archivo_origen"]
df = (df.select(orden)
        .withColumn("fecha_archivo", F.to_date(
            F.regexp_extract("archivo_origen", REGEX_FECHA_ARCHIVO, 1), FMT_FECHA_ARCHIVO))
        .withColumn("ingesta_ts", F.current_timestamp()))

# ════════════════════════════════════════════════════════════════
# 3. WRITE TO DELTA TABLE
# ════════════════════════════════════════════════════════════════
(df.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(T_BRONZE))

spark.sql(f"""COMMENT ON TABLE {T_BRONZE} IS
 'Bronze: crudo, todo en texto. 1 fila = 1 línea de menú.
  CSV diarios Taco Bueno 2025-2026 (597 archivos, 3 versiones de header).
  NO modificar. Generado por etl/01_bronze.py.'""")

spark.sql(f"OPTIMIZE {T_BRONZE}")

# ════════════════════════════════════════════════════════════════
# 4. VALIDATION
# ════════════════════════════════════════════════════════════════
bronze = spark.table(T_BRONZE)

# Row reconciliation per file
filas = bronze.groupBy("archivo_origen").agg(F.count("*").alias("filas_bronze"))
lineas = (spark.read.text(VOLUME_PATH)
          .withColumn("archivo_origen", F.col("_metadata.file_name"))
          .groupBy("archivo_origen")
          .agg((F.count("*") - 1).alias("filas_texto")))

rec = (inv.select(F.col("name").alias("archivo_origen"), "fecha_archivo", "n_cols", "size")
       .join(lineas, "archivo_origen", "left")
       .join(filas, "archivo_origen", "left")
       .fillna(0, ["filas_texto", "filas_bronze"])
       .withColumn("diferencia", F.col("filas_bronze") - F.col("filas_texto")))

n_bronze = bronze.count()
n_archivos_sin_filas = rec.filter("filas_bronze = 0").count()
n_archivos_con_diff  = rec.filter("diferencia != 0").count()
n_corrupt = bronze.filter("_corrupt IS NOT NULL").count()

print("═" * 60)
print("  RESUMEN BRONZE")
print("═" * 60)
print(f"  Filas en bronze:              {n_bronze:>15,}")
print(f"  Archivos sin filas (header):  {n_archivos_sin_filas:>15,}")
print(f"  Archivos con diferencia:      {n_archivos_con_diff:>15,}")
print(f"  Filas corruptas:              {n_corrupt:>15,}")
print(f"  Tabla: {T_BRONZE}")
print("═" * 60)

# Save file inventory
(rec.withColumn("ingesta_ts", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(T_INVENTARIO))
print(f"Inventario guardado en {T_INVENTARIO}")
