# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 3c: Genie Space — Preguntas en lenguaje natural sobre datos SAP
# MAGIC
# MAGIC ## Objetivo
# MAGIC Construir las tablas Gold necesarias para un Genie Space que permita al
# MAGIC **Gerente Comercial del Grupo Argos** hacer preguntas en español sobre
# MAGIC clientes, revenue, materiales y devoluciones — sin abrir SAP ni escribir SQL.
# MAGIC
# MAGIC ## Tablas que vamos a construir
# MAGIC
# MAGIC | Tabla | Fuente | Descripción |
# MAGIC |---|---|---|
# MAGIC | `kna1_silver` | kna1_bronze | Clientes limpios con campos validados |
# MAGIC | `kna1_gold` | kna1_silver | Maestro de clientes con KPIs históricos y clasificación |
# MAGIC | `vbap_silver` | vbap_bronze + mara_bronze | Posiciones de venta con descripción de material |
# MAGIC | `vbap_gold` | vbap_silver + vbak_silver | Revenue por material con contexto de negocio |
# MAGIC
# MAGIC > **Prerequisito**: tener las 8 tablas Bronze y vbak_silver cargadas del Módulo 2.

# COMMAND ----------

# ── Celda autosuficiente — importar siempre ──
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, trim, upper,
    when, coalesce, count, sum as _sum, avg as _avg,
    round as _round, min as _min, max as _max,
    countDistinct, regexp_replace
)

CATALOG     = "laboratory_sap_dev"
SCHEMA      = "sap_course"
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/curso_databricks"

spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"Catalog : {CATALOG}.{SCHEMA}")
print()

# Verificar tablas de entrada
tablas_req = ["kna1_bronze", "vbap_bronze", "mara_bronze", "vbak_silver"]
for t in tablas_req:
    try:
        n = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
        print(f"  OK  {t:<25} {n:>8,} registros")
    except:
        print(f"  ERR {t} — ejecutar M2 primero")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PASO 1: kna1_silver — Maestro de clientes limpio
# MAGIC
# MAGIC Transformaciones aplicadas:
# MAGIC - Limpiar espacios en NAME1, ORT01
# MAGIC - Estandarizar LAND1 a mayúsculas
# MAGIC - Marcar registros con nombre nulo como inválidos
# MAGIC - Agregar metadatos de carga

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.kna1_silver
    USING DELTA
    COMMENT 'Maestro de clientes SAP SD — tabla Silver. Datos limpios y validados.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'layer' = 'silver',
        'source' = 'kna1_bronze'
    )
    AS
    SELECT
        KUNNR                                       AS codigo_cliente,
        TRIM(NAME1)                                 AS nombre_cliente,
        UPPER(TRIM(LAND1))                          AS pais,
        TRIM(ORT01)                                 AS ciudad,
        TRIM(BRSCH)                                 AS sector_industria,
        TRIM(KTOKD)                                 AS grupo_cuentas,
        -- Validaciones
        CASE
            WHEN NAME1 IS NULL OR TRIM(NAME1) = '' THEN FALSE
            WHEN LAND1 IS NULL OR TRIM(LAND1) = '' THEN FALSE
            ELSE TRUE
        END                                         AS es_valido,
        -- Metadatos
        current_timestamp()                         AS _silver_loaded_at,
        'kna1_bronze'                               AS _source_table
    FROM {CATALOG}.{SCHEMA}.kna1_bronze
    WHERE KUNNR IS NOT NULL
""")

n = spark.table(f"{CATALOG}.{SCHEMA}.kna1_silver").count()
validos = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.kna1_silver WHERE es_valido = TRUE").collect()[0][0]
print(f"kna1_silver: {n:,} registros — {validos:,} válidos ({validos*100//n}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PASO 2: kna1_gold — Clientes con KPIs y clasificación ABC
# MAGIC
# MAGIC Esta tabla es el **corazón del Genie Space** para preguntas comerciales.
# MAGIC Combina el maestro de clientes con métricas históricas de ventas.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.kna1_gold
    USING DELTA
    COMMENT 'Maestro de clientes SAP con KPIs de ventas históricos y clasificación ABC. Usar en dashboards y Genie Space.'
    TBLPROPERTIES (
        'layer' = 'gold',
        'domain' = 'comercial',
        'genie_space' = 'true'
    )
    AS
    WITH metricas AS (
        SELECT
            v.KUNNR                             AS codigo_cliente,
            COUNT(DISTINCT v.VBELN)             AS total_ordenes,
            ROUND(SUM(v.NETWR), 2)              AS revenue_total,
            ROUND(AVG(v.NETWR), 2)              AS ticket_promedio,
            COUNT(DISTINCT YEAR(v.ERDAT))       AS anios_activo,
            MIN(YEAR(v.ERDAT))                  AS primer_anio_compra,
            MAX(YEAR(v.ERDAT))                  AS ultimo_anio_compra,
            SUM(CASE WHEN v.AUART IN ('RE','RK') THEN 1 ELSE 0 END) AS total_devoluciones,
            COUNT(DISTINCT v.VKORG)             AS orgs_ventas_activas
        FROM {CATALOG}.{SCHEMA}.vbak_silver v
        GROUP BY v.KUNNR
    )
    SELECT
        -- Datos maestros del cliente
        k.codigo_cliente,
        k.nombre_cliente,
        k.pais,
        k.ciudad,
        k.sector_industria,
        k.es_valido,
        -- KPIs de ventas
        COALESCE(m.total_ordenes, 0)            AS total_ordenes,
        COALESCE(m.revenue_total, 0)            AS revenue_total,
        COALESCE(m.ticket_promedio, 0)          AS ticket_promedio,
        COALESCE(m.total_devoluciones, 0)       AS total_devoluciones,
        COALESCE(m.anios_activo, 0)             AS anios_activo,
        m.primer_anio_compra,
        m.ultimo_anio_compra,
        COALESCE(m.orgs_ventas_activas, 0)      AS orgs_ventas_activas,
        -- Clasificación ABC
        CASE
            WHEN COALESCE(m.revenue_total, 0) > 1000000 THEN 'A - Strategic'
            WHEN COALESCE(m.revenue_total, 0) > 500000  THEN 'B - Key Account'
            WHEN COALESCE(m.revenue_total, 0) > 100000  THEN 'C - Standard'
            ELSE 'D - Small'
        END                                     AS clasificacion_abc,
        -- Indicador de cliente activo (compró en los últimos 2 años)
        CASE
            WHEN m.ultimo_anio_compra >= YEAR(CURRENT_DATE()) - 2 THEN TRUE
            ELSE FALSE
        END                                     AS es_cliente_activo,
        -- Metadatos
        current_timestamp()                     AS _gold_loaded_at
    FROM {CATALOG}.{SCHEMA}.kna1_silver k
    LEFT JOIN metricas m ON k.codigo_cliente = m.codigo_cliente
    WHERE k.es_valido = TRUE
""")

n = spark.table(f"{CATALOG}.{SCHEMA}.kna1_gold").count()
print(f"kna1_gold: {n:,} clientes")
print()

# Distribución por clasificación
print("=== Distribución ABC ===")
spark.sql(f"""
    SELECT clasificacion_abc, COUNT(*) AS clientes,
           ROUND(AVG(revenue_total), 0) AS revenue_promedio
    FROM {CATALOG}.{SCHEMA}.kna1_gold
    GROUP BY clasificacion_abc
    ORDER BY clientes DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PASO 3: vbap_silver — Posiciones de venta limpias
# MAGIC
# MAGIC Cada posición es un material dentro de una orden de venta.
# MAGIC Join con mara_bronze para traer la descripción del material.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.vbap_silver
    USING DELTA
    COMMENT 'Posiciones de órdenes de venta SAP SD — tabla Silver. Incluye descripción de material.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'layer' = 'silver',
        'source' = 'vbap_bronze + mara_bronze'
    )
    AS
    SELECT
        p.VBELN                                 AS numero_orden,
        p.POSNR                                 AS posicion,
        p.MATNR                                 AS codigo_material,
        TRIM(m.MAKTX)                           AS descripcion_material,
        m.MTART                                 AS tipo_material,
        m.MATKL                                 AS categoria_material,
        ROUND(p.KWMENG, 3)                      AS cantidad,
        ROUND(p.NETWR, 2)                       AS valor_neto,
        p.WAERK                                 AS moneda,
        -- Validaciones
        CASE
            WHEN p.NETWR IS NULL OR p.NETWR <= 0 THEN FALSE
            WHEN p.KWMENG IS NULL OR p.KWMENG <= 0 THEN FALSE
            ELSE TRUE
        END                                     AS es_valido,
        -- Metadatos
        current_timestamp()                     AS _silver_loaded_at
    FROM {CATALOG}.{SCHEMA}.vbap_bronze p
    LEFT JOIN {CATALOG}.{SCHEMA}.mara_bronze m ON p.MATNR = m.MATNR
    WHERE p.VBELN IS NOT NULL
      AND p.MATNR IS NOT NULL
""")

n = spark.table(f"{CATALOG}.{SCHEMA}.vbap_silver").count()
print(f"vbap_silver: {n:,} posiciones")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PASO 4: vbap_gold — Revenue por material con contexto de negocio
# MAGIC
# MAGIC Agrega las posiciones de venta por material y período.
# MAGIC Esta tabla permite a Genie responder preguntas sobre materiales específicos.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.vbap_gold
    USING DELTA
    COMMENT 'Revenue y volumen por material SAP — tabla Gold. Aggregado por material y período para análisis comercial.'
    TBLPROPERTIES (
        'layer' = 'gold',
        'domain' = 'comercial',
        'genie_space' = 'true'
    )
    AS
    SELECT
        p.codigo_material,
        p.descripcion_material,
        p.tipo_material,
        p.categoria_material,
        -- Dimensión temporal desde la orden de venta
        YEAR(v.ERDAT)                           AS anio,
        MONTH(v.ERDAT)                          AS mes,
        CONCAT(YEAR(v.ERDAT), '-',
            LPAD(MONTH(v.ERDAT), 2, '0'))       AS periodo,
        v.VKORG                                 AS org_ventas,
        v.KUNNR                                 AS codigo_cliente,
        -- KPIs
        COUNT(DISTINCT p.numero_orden)          AS total_ordenes,
        ROUND(SUM(p.cantidad), 2)               AS unidades_vendidas,
        ROUND(SUM(p.valor_neto), 2)             AS revenue_total,
        -- Metadatos
        current_timestamp()                     AS _gold_loaded_at
    FROM {CATALOG}.{SCHEMA}.vbap_silver p
    JOIN {CATALOG}.{SCHEMA}.vbak_silver v ON p.numero_orden = v.VBELN
    WHERE p.es_valido = TRUE
      AND v.ERDAT IS NOT NULL
    GROUP BY
        p.codigo_material, p.descripcion_material, p.tipo_material,
        p.categoria_material,
        YEAR(v.ERDAT), MONTH(v.ERDAT),
        CONCAT(YEAR(v.ERDAT), '-', LPAD(MONTH(v.ERDAT), 2, '0')),
        v.VKORG, v.KUNNR
""")

n = spark.table(f"{CATALOG}.{SCHEMA}.vbap_gold").count()
print(f"vbap_gold: {n:,} registros")
print()

# Top 10 materiales por revenue total
print("=== Top 10 materiales por revenue ===")
spark.sql(f"""
    SELECT
        descripcion_material,
        tipo_material,
        ROUND(SUM(revenue_total), 0)    AS revenue_total,
        SUM(unidades_vendidas)          AS unidades_totales
    FROM {CATALOG}.{SCHEMA}.vbap_gold
    GROUP BY descripcion_material, tipo_material
    ORDER BY revenue_total DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PASO 5: Resumen de tablas Gold disponibles para Genie Space

# COMMAND ----------

print("=== TABLAS GOLD LISTAS PARA GENIE SPACE ===\n")
tablas_gold = {
    "vbak_gold":  "Órdenes de venta — numero_orden, fecha_creacion, org_ventas, valor_neto, es_devolucion",
    "kna1_gold":  "Clientes — nombre_cliente, pais, ciudad, revenue_total, clasificacion_abc",
    "vbap_gold":  "Materiales vendidos — descripcion_material, revenue_total, unidades_vendidas, periodo",
}
for tabla, desc in tablas_gold.items():
    n = spark.table(f"{CATALOG}.{SCHEMA}.{tabla}").count()
    print(f"  {tabla:<20} {n:>8,} registros")
    print(f"  {'':20} {desc}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PASO 6: Configurar Genie Space — instrucciones paso a paso
# MAGIC
# MAGIC ### Desde la UI de Databricks:
# MAGIC
# MAGIC 1. Menú izquierdo → **SQL Editor** → ícono robot **"Genie"**
# MAGIC 2. Clic en **"+ New Genie Space"**
# MAGIC 3. Nombre: `SAP Analytics — Grupo Argos`
# MAGIC 4. En **"Tables"** → agregar las 3 tablas:
# MAGIC    - `laboratory_sap_dev.sap_course.vbak_gold`
# MAGIC    - `laboratory_sap_dev.sap_course.kna1_gold`
# MAGIC    - `laboratory_sap_dev.sap_course.vbap_gold`
# MAGIC 5. En **"Instructions"** → pegar el texto de la siguiente celda
# MAGIC 6. En **"Sample questions"** → agregar las preguntas de ejemplo
# MAGIC 7. Clic en **"Save"**

# COMMAND ----------

contexto_genie = """
Eres un asistente de análisis comercial para el Grupo Argos — conglomerado colombiano
con negocios en cemento (Cementos Argos), energía (Celsia) e infraestructura (Odinsa).
Respondes preguntas sobre ventas, clientes y materiales usando datos SAP SD.

TABLAS DISPONIBLES:
- vbak_gold: órdenes de venta. Una fila = una orden completa.
- kna1_gold: maestro de clientes con KPIs históricos y clasificación ABC.
- vbap_gold: revenue por material y período. Para preguntas sobre qué se vende.

CONTEXTO DEL NEGOCIO:
- org_ventas: organización de ventas interna. 1000=Colombia, 2000=USA, 3000=Caribe
- clasificacion_abc: A-Strategic (>$1M), B-Key Account (>$500K), C-Standard (>$100K), D-Small
- es_devolucion: TRUE cuando el tipo de orden es RE (devolución) o RK (crédito)
- canal_distribucion: 10=Directo, 20=Distribuidor, 30=Exportación
- periodo: formato YYYY-MM para filtros temporales (ej: '2023-01' a '2023-12')
- Los montos están en USD

CÓMO RESPONDER:
- Siempre en español
- Para tendencias: usar vbak_gold agrupando por periodo o anio_creacion
- Para análisis de clientes: unir vbak_gold con kna1_gold por codigo_cliente
- Para análisis de materiales: usar vbap_gold
- Revenue total = SUM(valor_neto) en vbak_gold o SUM(revenue_total) en vbap_gold
- Devoluciones = filtrar WHERE es_devolucion = TRUE en vbak_gold
- Clientes nuevos = primer_anio_compra = año consultado en kna1_gold
"""

print("=== CONTEXTO PARA PEGAR EN GENIE SPACE INSTRUCTIONS ===\n")
print(contexto_genie)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PASO 7: Preguntas de ejemplo para el Genie Space
# MAGIC
# MAGIC Agregar estas preguntas en la sección **"Sample questions"** del Genie Space.
# MAGIC Genie aprende el patrón SQL de cada una y las usa como referencia.

# COMMAND ----------

preguntas_ejemplo = [
    {
        "pregunta": "¿Cuál fue el cliente que más compró en 2023?",
        "sql": """
SELECT nombre_cliente, pais, revenue_total, total_ordenes, clasificacion_abc
FROM laboratory_sap_dev.sap_course.kna1_gold
WHERE ultimo_anio_compra = 2023
ORDER BY revenue_total DESC
LIMIT 1"""
    },
    {
        "pregunta": "¿Cuánto revenue generamos por mes en 2023?",
        "sql": """
SELECT periodo, org_ventas, ROUND(SUM(valor_neto), 0) AS revenue_mensual,
       COUNT(DISTINCT numero_orden) AS ordenes
FROM laboratory_sap_dev.sap_course.vbak_gold
WHERE anio_creacion = 2023 AND es_devolucion = FALSE
GROUP BY periodo, org_ventas
ORDER BY periodo"""
    },
    {
        "pregunta": "¿Cuántas devoluciones hubo y cuánto revenue representaron?",
        "sql": """
SELECT
    COUNT(DISTINCT numero_orden)    AS total_devoluciones,
    ROUND(SUM(valor_neto), 0)       AS revenue_devuelto,
    ROUND(SUM(valor_neto) * 100.0 /
        SUM(SUM(valor_neto)) OVER(), 2) AS pct_del_total
FROM laboratory_sap_dev.sap_course.vbak_gold
GROUP BY es_devolucion
ORDER BY es_devolucion"""
    },
    {
        "pregunta": "¿Qué 5 materiales generan más revenue?",
        "sql": """
SELECT descripcion_material, tipo_material,
       ROUND(SUM(revenue_total), 0) AS revenue_total,
       SUM(unidades_vendidas) AS unidades
FROM laboratory_sap_dev.sap_course.vbap_gold
GROUP BY descripcion_material, tipo_material
ORDER BY revenue_total DESC
LIMIT 5"""
    },
    {
        "pregunta": "¿Cuántos clientes tenemos por país?",
        "sql": """
SELECT pais, COUNT(*) AS total_clientes,
       SUM(CASE WHEN clasificacion_abc = 'A - Strategic' THEN 1 ELSE 0 END) AS clientes_A,
       ROUND(SUM(revenue_total), 0) AS revenue_total
FROM laboratory_sap_dev.sap_course.kna1_gold
GROUP BY pais
ORDER BY total_clientes DESC"""
    },
]

print("=== PREGUNTAS DE EJEMPLO PARA GENIE SPACE ===\n")
for i, item in enumerate(preguntas_ejemplo, 1):
    print(f"Pregunta {i}: {item['pregunta']}")
    print(f"SQL:{item['sql']}\n")
    print("-" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PASO 8: Verificar el Genie Space — preguntas de prueba en código
# MAGIC
# MAGIC Estas son las mismas preguntas que Genie debería poder responder.
# MAGIC Ejecuta esta celda para verificar que los datos son correctos antes de la demo.

# COMMAND ----------

print("=" * 65)
print("VERIFICACIÓN PRE-DEMO GENIE SPACE")
print("=" * 65)

# Pregunta 1 — cliente top 2023
print("\n1. ¿Cuál fue el cliente que más compró en 2023?")
spark.sql(f"""
    SELECT nombre_cliente, pais, revenue_total, total_ordenes, clasificacion_abc
    FROM {CATALOG}.{SCHEMA}.kna1_gold
    WHERE ultimo_anio_compra = 2023
    ORDER BY revenue_total DESC
    LIMIT 3
""").show(truncate=False)

# Pregunta 2 — devoluciones
print("\n2. ¿Cuántas devoluciones hubo y cuánto revenue representaron?")
spark.sql(f"""
    SELECT
        es_devolucion,
        COUNT(DISTINCT numero_orden)    AS ordenes,
        ROUND(SUM(valor_neto), 0)       AS revenue
    FROM {CATALOG}.{SCHEMA}.vbak_gold
    GROUP BY es_devolucion
    ORDER BY es_devolucion
""").show()

# Pregunta 3 — top materiales
print("\n3. ¿Qué 5 materiales generan más revenue?")
spark.sql(f"""
    SELECT descripcion_material,
           ROUND(SUM(revenue_total), 0) AS revenue_total,
           SUM(unidades_vendidas)       AS unidades
    FROM {CATALOG}.{SCHEMA}.vbap_gold
    GROUP BY descripcion_material
    ORDER BY revenue_total DESC
    LIMIT 5
""").show(truncate=False)

# Pregunta 4 — clientes por país
print("\n4. ¿Cuántos clientes activos tenemos por país?")
spark.sql(f"""
    SELECT pais, COUNT(*) AS clientes,
           ROUND(SUM(revenue_total), 0) AS revenue_total
    FROM {CATALOG}.{SCHEMA}.kna1_gold
    WHERE es_cliente_activo = TRUE
    GROUP BY pais
    ORDER BY clientes DESC
    LIMIT 8
""").show()

print("\n✅ Verificación completa — Genie Space listo para la demo")
