"""
Gold ETL — Taco Bueno (SUPERSEDIDO por el pipeline SDP "ETL Taco Bueno",
que ahora crea/refresca las vistas gold en cada update; este script queda
como referencia para ejecución manual fuera del pipeline).

Ejecuta en orden los archivos .sql de esta carpeta para crear o
reemplazar las vistas materializadas de la capa gold (tacobueno.gold).
Cada archivo .sql contiene exactamente UNA sentencia SQL.

Orden de dependencia:
  01 schema gold
  02 mv_tickets_base        (lee tickets_silver)
  03 mv_tienda_dia          (lee mv_tickets_base)
  04 mv_tienda_dia_canal    (lee mv_tickets_base)
  05 mv_tienda_variables    (lee mv_tienda_dia)
  06 mv_promo_resumen_mes   (lee mv_tickets_base)

Las MVs también se refrescan solas (TRIGGER ON UPDATE, máx. 1 vez al día)
cuando cambia la silver; este script sirve para desplegar cambios de
lógica o forzar el recálculo desde el job.

Después de este script, ejecutar 08_validaciones.py como tarea
siguiente del mismo job.
"""

import pathlib

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════
CATALOG = "tacobueno"
SCHEMA = "gold"

GOLD_DIR = pathlib.Path(
    "/Workspace/Users/a00833973@tec.mx/Tacobueno/"
    "src/ETL Taco Bueno/transformations/gold"
)

SQL_FILES = [
    "01_gold_setup.sql",
    "02_mv_tickets_base.sql",
    "03_mv_tienda_dia.sql",
    "04_mv_tienda_dia_canal.sql",
    "05_mv_tienda_variables.sql",
    "06_mv_promo_resumen_mes.sql",
]

VIEWS = [
    "mv_tickets_base",
    "mv_tienda_dia",
    "mv_tienda_dia_canal",
    "mv_tienda_variables",
    "mv_promo_resumen_mes",
]

# ════════════════════════════════════════════════════════════════
# EJECUCIÓN
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("─" * 60)
    print("  GOLD ETL — Vistas materializadas")
    print(f"  Fuente: {CATALOG}.tickets.tickets_silver")
    print(f"  Destino: {CATALOG}.{SCHEMA}")
    print("─" * 60)

    for fname in SQL_FILES:
        sql = (GOLD_DIR / fname).read_text(encoding="utf-8")
        print(f"\n=== Ejecutando {fname} ===")
        spark.sql(sql)
        print(f"OK: {fname}")

    print("\n=== Resumen de filas por vista ===")
    for view in VIEWS:
        n = spark.sql(
            f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{view}"
        ).collect()[0][0]
        print(f"  {CATALOG}.{SCHEMA}.{view}: {n:,} filas")

    print("\nGOLD ETL terminado. Ejecutar 08_validaciones.py a continuación.")