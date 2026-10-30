"""
Validaciones de la capa gold — Taco Bueno

Ejecutar como tarea de job después de un update exitoso del pipeline
SDP "ETL Taco Bueno" (el pipeline crea/refresca las vistas gold).
Adaptación de las validaciones V1-V7 de los notebooks 000_promos y
02_promo_react a las vistas materializadas de tacobueno.gold.

V1  Llave y conteo: mv_tickets_base sin duplicados y con el mismo
    número de tickets que tickets_silver.
V2  Montos: sub_prod y adj por anio_mes vs tickets_silver (dif ~0,
    tolerancia por redondeos de DECIMAL).
V3  Cascada: por anio_mes, mv_tickets_base = mv_tienda_dia
    (tickets + excluidos) = mv_tienda_dia_canal.
V4  mv_tienda_variables: proporciones (dow, franja, canal) suman 1;
    universo por periodo.
V5  Referencia cruzada contra las tablas exploratorias originales
    (tacobueno.tickets.*) — informativo, no detiene el job.

Falla (AssertionError) si V1, V3 o V4 no cumplen. V2 falla si alguna
mes se desvía más de TOL_MONETARIO. V5 solo reporta.
"""

CATALOG, SCHEMA = "tacobueno", "gold"
G = f"{CATALOG}.{SCHEMA}"
SILVER = "tacobueno.tickets.tickets_silver"
TOL_MONETARIO = 10.0  # dólares por mes (redondeos DECIMAL)

fallos = []


def check(nombre, cond, detalle=""):
    estado = "OK    " if cond else "FALLO "
    print(f"[{estado}] {nombre}{' — ' + detalle if detalle else ''}")
    if not cond:
        fallos.append(nombre)
    return cond


print("─" * 60)
print("  VALIDACIONES CAPA GOLD")
print("─" * 60)

# ─────────────────────────────────────────────────────────────
# V1. Llave y conteo
# ─────────────────────────────────────────────────────────────
print("\n=== V1. Llave y conteo de mv_tickets_base ===")
r = spark.sql(f"""
    SELECT COUNT(*) AS n,
           COUNT(DISTINCT ticket_id) AS d,
           SUM(n_lineas) AS lineas
    FROM {G}.mv_tickets_base
""").collect()[0]
check("V1a. ticket_id único", r.n == r.d,
      f"{r.n:,} filas / {r.d:,} distintos / {r.lineas:,} líneas")

r2 = spark.sql(f"""
    SELECT COUNT(DISTINCT CONCAT_WS('|', store_id, CAST(fecha AS STRING), check_number)) AS d
    FROM {SILVER}
""").collect()[0]
check("V1b. tickets = tickets_silver", r.n == r2.d,
      f"gold {r.n:,} vs silver {r2.d:,}")

# ─────────────────────────────────────────────────────────────
# V2. Montos por mes vs silver
# ─────────────────────────────────────────────────────────────
print("\n=== V2. Montos por anio_mes vs silver ===")
df_v2 = spark.sql(f"""
    WITH ref AS (
        SELECT anio_mes,
               SUM(CASE WHEN row_type = 'product' THEN quantity * menu_price ELSE 0 END) AS sub_prod,
               SUM(CASE WHEN row_type = 'price_adjustment' THEN quantity * menu_price ELSE 0 END) AS adj
        FROM {SILVER}
        GROUP BY anio_mes
    ),
    gol AS (
        SELECT anio_mes, SUM(sub_prod) AS sub_prod, SUM(adj) AS adj
        FROM {G}.mv_tickets_base
        GROUP BY anio_mes
    )
    SELECT g.anio_mes,
           ROUND(g.sub_prod - r.sub_prod, 2) AS dif_sub_prod,
           ROUND(g.adj - r.adj, 2) AS dif_adj
    FROM gol g
    JOIN ref r USING (anio_mes)
    ORDER BY g.anio_mes
""")
rows_v2 = df_v2.collect()
df_v2.show()
meses_malos = [x.anio_mes for x in rows_v2
               if x.dif_sub_prod is not None and abs(x.dif_sub_prod) > TOL_MONETARIO
               or x.dif_adj is not None and abs(x.dif_adj) > TOL_MONETARIO]
check("V2. dif montos dentro de tolerancia", not meses_malos,
      f"{len(rows_v2)} meses, tolerancia ${TOL_MONETARIO}" + (f", fuera: {meses_malos}" if meses_malos else ""))

# ─────────────────────────────────────────────────────────────
# V3. Cascada base -> tienda_dia -> tienda_dia_canal
# ─────────────────────────────────────────────────────────────
print("\n=== V3. Cascada por anio_mes ===")
df_v3 = spark.sql(f"""
    WITH tb AS (
        SELECT anio_mes, COUNT(*) AS tickets
        FROM {G}.mv_tickets_base GROUP BY anio_mes
    ),
    td AS (
        SELECT anio_mes, SUM(tickets) AS t, SUM(tickets_excluidos) AS e
        FROM {G}.mv_tienda_dia GROUP BY anio_mes
    ),
    tdc AS (
        SELECT anio_mes, SUM(tickets) AS t
        FROM {G}.mv_tienda_dia_canal GROUP BY anio_mes
    )
    SELECT tb.anio_mes,
           tb.tickets,
           td.t + td.e AS td_total,
           tdc.t AS tdc_total
    FROM tb
    JOIN td USING (anio_mes)
    JOIN tdc USING (anio_mes)
    ORDER BY tb.anio_mes
""")
rows_v3 = df_v3.collect()
df_v3.show()
malos_v3 = [x.anio_mes for x in rows_v3
            if not (x.tickets == x.td_total == x.tdc_total)]
check("V3. tickets_base = tienda_dia = tienda_dia_canal", not malos_v3,
      f"{len(rows_v3)} meses" + (f", fuera: {malos_v3}" if malos_v3 else ""))

# ─────────────────────────────────────────────────────────────
# V4. mv_tienda_variables: proporciones y universo
# ─────────────────────────────────────────────────────────────
print("\n=== V4. mv_tienda_variables ===")
df_v4 = spark.sql(f"""
    SELECT periodo_prov,
           COUNT(*) AS tiendas,
           SUM(CASE WHEN en_universo THEN 1 ELSE 0 END) AS en_universo,
           MIN(p_dow_1 + p_dow_2 + p_dow_3 + p_dow_4 + p_dow_5 + p_dow_6 + p_dow_7) AS min_dow,
           MAX(p_dow_1 + p_dow_2 + p_dow_3 + p_dow_4 + p_dow_5 + p_dow_6 + p_dow_7) AS max_dow,
           MIN(p_desayuno + p_almuerzo + p_tarde + p_cena + p_noche) AS min_franja,
           MAX(p_desayuno + p_almuerzo + p_tarde + p_cena + p_noche) AS max_franja,
           MIN(p_drive_thru + p_para_llevar + p_salon + p_digital_propio
               + p_delivery_terceros + p_desconocido + p_otro) AS min_canal,
           MAX(p_drive_thru + p_para_llevar + p_salon + p_digital_propio
               + p_delivery_terceros + p_desconocido + p_otro) AS max_canal,
           SUM(CASE WHEN pct_promo IS NULL OR razon_desc IS NULL THEN 1 ELSE 0 END) AS nulos_clave
    FROM {G}.mv_tienda_variables
    GROUP BY periodo_prov
    ORDER BY periodo_prov
""")
rows_v4 = df_v4.collect()
df_v4.show()
TOL_PROP = 1e-6
ok_v4 = all(
    abs(x.min_dow - 1) < TOL_PROP and abs(x.max_dow - 1) < TOL_PROP
    and abs(x.min_franja - 1) < TOL_PROP and abs(x.max_franja - 1) < TOL_PROP
    and abs(x.min_canal - 1) < TOL_PROP and abs(x.max_canal - 1) < TOL_PROP
    and x.nulos_clave == 0
    for x in rows_v4
) and len(rows_v4) > 0
check("V4a. proporciones suman 1 y sin nulos clave", ok_v4,
      ", ".join(f"{x.periodo_prov}: {x.tiendas} tiendas ({x.en_universo} en universo)" for x in rows_v4))

# ─────────────────────────────────────────────────────────────
# V5. Referencia cruzada vs tablas exploratorias (informativo)
# ─────────────────────────────────────────────────────────────
print("\n=== V5. Comparación vs tablas exploratorias (informativo) ===")
try:
    df_v5 = spark.sql(f"""
        SELECT
          (SELECT COUNT(*) FROM {G}.mv_tickets_base)          AS gold_tickets,
          (SELECT COUNT(*) FROM tacobueno.tickets.tickets_base) AS exp_tickets,
          (SELECT COUNT(*) FROM {G}.mv_tienda_dia)              AS gold_td,
          (SELECT COUNT(*) FROM tacobueno.tickets.tienda_dia)  AS exp_td,
          (SELECT COUNT(*) FROM {G}.mv_tienda_variables)        AS gold_tv,
          (SELECT COUNT(*) FROM tacobueno.tickets.tienda_variables) AS exp_tv
    """)
    x = df_v5.collect()[0]
    df_v5.show()
    print(f"  tickets: gold {x.gold_tickets:,} vs exploratoria {x.exp_tickets:,} "
          f"(dif {x.gold_tickets - x.exp_tickets:,})")
    print(f"  tienda_dia: gold {x.gold_td:,} vs exploratoria {x.exp_td:,} "
          f"(dif {x.gold_td - x.exp_td:,})")
    print(f"  tienda_variables: gold {x.gold_tv:,} vs exploratoria {x.exp_tv:,} "
          f"(dif {x.gold_tv - x.exp_tv:,})")
    print("  Nota: diferencias pequeñas pueden venir de que la silver actual")
    print("  (tickets_silver) no es idéntica a la silver exploratoria (ventas_clean).")
except Exception as e:
    print(f"  (informativo, no bloqueante) No se pudo comparar: {e}")

# ─────────────────────────────────────────────────────────────
# RESUMEN
# ─────────────────────────────────────────────────────────────
print("\n" + "═" * 60)
if not fallos:
    print("  RESULTADO: TODAS LAS VALIDACIONES OK")
else:
    print(f"  RESULTADO: {len(fallos)} VALIDACIONES CON FALLO: {fallos}")
print("═" * 60)

assert not fallos, f"Validaciones de la capa gold con fallos: {fallos}"