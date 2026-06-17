# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 5: Ingesta y Arquitectura Medallion — Caso Práctico SAP
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC - Implementar la arquitectura Bronze → Silver → Gold con datos SAP reales
# MAGIC - Usar Auto Loader para ingesta incremental de archivos
# MAGIC - Orquestar el pipeline completo con Databricks Jobs
# MAGIC - Construir la capa Gold lista para dashboards ejecutivos
# MAGIC
# MAGIC ---
# MAGIC ## 5.1 La Arquitectura Medallion — Filosofía
# MAGIC
# MAGIC ```
# MAGIC  FUENTE SAP           BRONZE                SILVER                 GOLD
# MAGIC  ─────────          ──────────            ──────────           ──────────
# MAGIC  BKPF.csv    ──►   bkpf_bronze   ──►   bkpf_silver   ──►   fin_summary
# MAGIC  BSEG.csv    ──►   bseg_bronze   ──►   bseg_silver   ──►
# MAGIC  VBAK.csv    ──►   vbak_bronze   ──►   vbak_silver   ──►   sales_kpis
# MAGIC  VBAP.csv    ──►   vbap_bronze   ──►   vbap_silver   ──►
# MAGIC  KNA1.csv    ──►   kna1_bronze   ──►   kna1_silver   ──►   customer_360
# MAGIC  MARA.csv    ──►   mara_bronze   ──►   mara_silver   ──►
# MAGIC ```
# MAGIC
# MAGIC | Capa | Propósito | Transformación | Quién la usa |
# MAGIC |---|---|---|---|
# MAGIC | **Bronze** | Datos crudos de SAP tal como llegan | Ninguna — solo ingestar | Ingenieros de datos |
# MAGIC | **Silver** | Datos limpios, validados, tipados | Limpieza, deduplicación, tipos correctos | Analistas, Data Scientists |
# MAGIC | **Gold** | Agregados de negocio listos para consumo | KPIs, métricas, joins complejos | Directivos, dashboards, IA |

# COMMAND ----------

CATALOG = "laboratory_sap_dev"
SCHEMA  = "sap_course"
SAP_DATA_PATH = f"/Volumes/{CATALOG}/bronze/curso_databricks"
spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"Contexto: {CATALOG}.{SCHEMA}")
print(f"Datos   : {SAP_DATA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Capa BRONZE — Ingesta raw de SAP
# MAGIC
# MAGIC Principio: **sin transformaciones**, datos exactamente como vienen de SAP.
# MAGIC Solo agregar metadatos de auditoría.

# COMMAND ----------

from pyspark.sql.functions import (
    col, current_timestamp, lit, to_date, to_timestamp,
    upper, trim, when, isnan, isnull, regexp_replace,
    sum as _sum, count, avg, round as _round,
    year, month, dayofmonth, datediff
)
from pyspark.sql.types import DoubleType, IntegerType, StringType
from datetime import datetime

# Bronze IDEMPOTENTE: las tablas ya existen del Módulo 2 — NO las sobreescribimos
# (perderíamos comments, propiedades CDF y el historial de Time Travel)
# Esta celda solo crea las que falten — patrón de producción real

def ingest_bronze_si_falta(tabla_nombre):
    tabla_full = f"{CATALOG}.{SCHEMA}.{tabla_nombre.lower()}_bronze"
    if spark.catalog.tableExists(tabla_full):
        return spark.table(tabla_full).count(), "ya existía"
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(f"{SAP_DATA_PATH}/{tabla_nombre}.csv")
          .withColumn("_ingestion_ts",  current_timestamp())
          .withColumn("_source_system", lit("SAP_ECC"))
          .withColumn("_source_file",   lit(f"{tabla_nombre}.csv"))
          .withColumn("_batch_id",      lit(datetime.now().strftime("%Y%m%d_%H%M%S"))))
    df.write.mode("overwrite").saveAsTable(tabla_full)
    return df.count(), "creada"

print("Verificando capa Bronze (idempotente)...")
for tabla in ["BKPF","BSEG","KNA1","MARA","VBAK","VBAP","LFA1","EKKO"]:
    n, estado = ingest_bronze_si_falta(tabla)
    print(f"  {tabla:<6} -> bronze  ({n:>6,} filas)  [{estado}]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Capa SILVER — Limpieza y validación de datos SAP
# MAGIC
# MAGIC Aquí aplicamos las reglas de calidad de datos:
# MAGIC - Eliminar duplicados
# MAGIC - Corregir tipos de datos (fechas SAP en formato YYYYMMDD)
# MAGIC - Normalizar strings
# MAGIC - Marcar registros inválidos

# COMMAND ----------

# Silver: BKPF — Documentos contables limpios
df_bkpf_bronze = spark.table(f"{CATALOG}.{SCHEMA}.bkpf_bronze")

df_bkpf_silver = (df_bkpf_bronze
    # Convertir fechas SAP (YYYYMMDD string) a DateType
    .withColumn("BLDAT_DT",  to_date(col("BLDAT"), "yyyyMMdd"))
    .withColumn("BUDAT_DT",  to_date(col("BUDAT"), "yyyyMMdd"))
    # Normalizar strings
    .withColumn("USNAM",     upper(trim(col("USNAM"))))
    .withColumn("BKTXT",     trim(col("BKTXT")))
    # Validar campos obligatorios de SAP
    .withColumn("_is_valid",
        when(col("BELNR").isNull(), False)
        .when(col("BUKRS").isNull(), False)
        .when(col("GJAHR").isNull(), False)
        .otherwise(True))
    # Eliminar metadatos de bronze
    .drop("_source_file", "_batch_id")
    .withColumn("_silver_ts", current_timestamp())
    # Deduplicar por clave primaria SAP: BELNR + BUKRS + GJAHR
    .dropDuplicates(["BELNR", "BUKRS", "GJAHR"]))

(df_bkpf_silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bkpf_silver"))

print(f"BKPF Silver: {df_bkpf_silver.count():,} registros")
print(f"  Válidos  : {df_bkpf_silver.filter(col('_is_valid') == True).count():,}")
print(f"  Inválidos: {df_bkpf_silver.filter(col('_is_valid') == False).count():,}")

# COMMAND ----------

# ⚠ IMPORTANTE: vbak_silver y kna1_silver YA EXISTEN del Módulo 3 (con CDF y
# nombres usados por vbak_gold, kna1_gold y el Genie Space). NO las sobreescribimos.
# Esta celda es SOLO REFERENCIA del patrón — está protegida con un guard.

if spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.vbak_silver"):
    print("vbak_silver ya existe (Módulo 3) — se conserva. Celda en modo referencia.")
else:
    df_vbak_bronze = spark.table(f"{CATALOG}.{SCHEMA}.vbak_bronze")

    df_vbak_silver = (df_vbak_bronze
        .withColumn("ERDAT_DT",  to_date(col("ERDAT"), "yyyyMMdd"))
        .withColumn("YEAR",      year(to_date(col("ERDAT"), "yyyyMMdd")))
        .withColumn("MONTH",     month(to_date(col("ERDAT"), "yyyyMMdd")))
        .withColumn("NETWR",     col("NETWR").cast(DoubleType()))
        .withColumn("AUART",     upper(trim(col("AUART"))))
        .withColumn("_is_valid",
            when(col("NETWR") <= 0, False)
            .when(col("KUNNR").isNull(), False)
            .otherwise(True))
        .withColumn("_silver_ts", current_timestamp())
        .dropDuplicates(["VBELN"]))
    df_vbak_silver.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.vbak_silver")
    print(f"VBAK Silver: {df_vbak_silver.count():,} registros")

# COMMAND ----------

# Silver: KNA1 — ya existe del Módulo 3c con nombres en español (codigo_cliente...)
# kna1_gold y el Genie Space dependen de ella. PROTEGIDA con guard.
if spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.kna1_silver"):
    print("kna1_silver ya existe (Módulo 3c) — se conserva.")
else:
    df_kna1_silver = (spark.table(f"{CATALOG}.{SCHEMA}.kna1_bronze")
        .withColumn("NAME1",  trim(col("NAME1")))
        .withColumn("ORT01",  upper(trim(col("ORT01"))))
        .withColumn("LAND1",  upper(trim(col("LAND1"))))
        .withColumn("_silver_ts", current_timestamp())
        .dropDuplicates(["KUNNR"]))
    df_kna1_silver.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.kna1_silver")
    print(f"KNA1 Silver: {df_kna1_silver.count():,} clientes únicos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Capa GOLD — KPIs de negocio listos para dashboards
# MAGIC
# MAGIC La capa Gold responde preguntas de negocio específicas.
# MAGIC Está optimizada para consumo rápido por SQL Warehouses y dashboards.

# COMMAND ----------

# GOLD 1: Resumen financiero por compañía y año fiscal
spark.sql("""
    CREATE OR REPLACE TABLE laboratory_sap_dev.sap_course.gold_fin_summary AS
    SELECT
        b.BUKRS                                    AS company_code,
        b.GJAHR                                    AS fiscal_year,
        b.WAERK                                    AS currency,
        COUNT(DISTINCT b.BELNR)                    AS num_documents,
        COUNT(DISTINCT b.USNAM)                    AS num_users,
        ROUND(SUM(s.DMBTR), 2)                     AS total_amount,
        ROUND(AVG(s.DMBTR), 2)                     AS avg_line_amount,
        COUNT(s.BUZEI)                             AS num_line_items
    FROM laboratory_sap_dev.sap_course.bkpf_silver b
    JOIN laboratory_sap_dev.sap_course.bseg_bronze s
        ON  b.BELNR = s.BELNR
        AND b.BUKRS = s.BUKRS
        AND b.GJAHR = s.GJAHR
    WHERE b._is_valid = TRUE
    GROUP BY b.BUKRS, b.GJAHR, b.WAERK
    ORDER BY b.GJAHR DESC, total_amount DESC
""")
print("GOLD: fin_summary creado")
spark.table("laboratory_sap_dev.sap_course.gold_fin_summary").show()

# COMMAND ----------

# GOLD 2: KPIs de ventas por año, mes y organización
spark.sql("""
    CREATE OR REPLACE TABLE laboratory_sap_dev.sap_course.gold_sales_kpis AS
    SELECT
        YEAR(v.ERDAT)                              AS year,
        MONTH(v.ERDAT)                             AS month,
        v.VKORG                                    AS sales_org,
        v.WAERK                                    AS currency,
        COUNT(DISTINCT v.VBELN)                    AS num_orders,
        COUNT(DISTINCT v.KUNNR)                    AS num_customers,
        ROUND(SUM(v.NETWR), 2)                     AS total_revenue,
        ROUND(AVG(v.NETWR), 2)                     AS avg_order_value,
        ROUND(MAX(v.NETWR), 2)                     AS max_order_value,
        ROUND(MIN(v.NETWR), 2)                     AS min_order_value
    FROM laboratory_sap_dev.sap_course.vbak_silver v
    WHERE v.VBELN  IS NOT NULL
      AND v.NETWR  IS NOT NULL
      AND v.NETWR  > 0
      AND v.ERDAT  IS NOT NULL
    GROUP BY YEAR(v.ERDAT), MONTH(v.ERDAT), v.VKORG, v.WAERK
    ORDER BY year DESC, month DESC, total_revenue DESC
""")
print("✅ GOLD: gold_sales_kpis creado")
spark.table("laboratory_sap_dev.sap_course.gold_sales_kpis").show(10)

# COMMAND ----------

# GOLD 3: Customer 360 — vista unificada del cliente
spark.sql("""
    CREATE OR REPLACE TABLE laboratory_sap_dev.sap_course.gold_customer_360 AS
    SELECT
        k.codigo_cliente                             AS customer_id,
        k.nombre_cliente                             AS customer_name,
        k.pais                                       AS country,
        k.ciudad                                     AS city,
        k.sector_industria                           AS industry_sector,
        COUNT(DISTINCT v.VBELN)                      AS total_orders,
        ROUND(SUM(v.NETWR),  2)                      AS total_revenue,
        ROUND(AVG(v.NETWR),  2)                      AS avg_order_value,
        MIN(v.ERDAT)                                 AS first_order_date,
        MAX(v.ERDAT)                                 AS last_order_date,
        DATEDIFF(MAX(v.ERDAT), MIN(v.ERDAT))         AS customer_lifetime_days,
        CASE
            WHEN SUM(v.NETWR) > 500000 THEN 'PLATINUM'
            WHEN SUM(v.NETWR) > 100000 THEN 'GOLD'
            WHEN SUM(v.NETWR) > 50000  THEN 'SILVER'
            ELSE                            'STANDARD'
        END                                          AS customer_tier
    FROM laboratory_sap_dev.sap_course.kna1_silver  k
    LEFT JOIN laboratory_sap_dev.sap_course.vbak_silver v
           ON LPAD(CAST(k.codigo_cliente AS STRING), 10, '0') = v.KUNNR
          AND v.VBELN  IS NOT NULL
          AND v.NETWR  > 0
          AND v.ERDAT  IS NOT NULL
    WHERE k.es_valido = TRUE
    GROUP BY k.codigo_cliente, k.nombre_cliente, k.pais,
             k.ciudad, k.sector_industria
    ORDER BY total_revenue DESC NULLS LAST
""")
print("✅ GOLD: gold_customer_360 creado")

spark.sql("""
    SELECT customer_tier,
           COUNT(*)                    AS num_customers,
           ROUND(SUM(total_revenue),2) AS revenue
    FROM laboratory_sap_dev.sap_course.gold_customer_360
    GROUP BY customer_tier
    ORDER BY revenue DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 Auto Loader — Ingesta incremental de archivos
# MAGIC
# MAGIC **El escenario SAP real**: el extractor de S/4HANA deja un archivo nuevo cada
# MAGIC noche en el landing zone. Sin Auto Loader, `spark.read` relee TODO el directorio
# MAGIC cada vez — reprocesa, duplica y el costo crece cada día.
# MAGIC
# MAGIC **Auto Loader (cloudFiles)** detecta SOLO los archivos nuevos desde la última
# MAGIC ejecución usando un checkpoint. Garantiza exactly-once: si el job falla y
# MAGIC reintenta, no duplica.
# MAGIC
# MAGIC | Capacidad | Qué hace |
# MAGIC |---|---|
# MAGIC | Incremental | Solo archivos nuevos — checkpoint registra lo procesado |
# MAGIC | Exactly-once | Reintentos sin duplicados |
# MAGIC | Schema inference | Detecta el schema y lo guarda en `schemaLocation` |
# MAGIC | Schema evolution | Columna nueva en el CSV de SAP → la agrega sola |
# MAGIC | `_rescued_data` | Columna automática donde caen datos que no matchean el schema |

# COMMAND ----------

# Rutas separadas claramente
LANDING_PATH    = f"{SAP_DATA_PATH}/landing/vbak"          # solo CSVs — zona de aterrizaje
STAGING_PATH    = f"{SAP_DATA_PATH}/staging/vbak"          # escritura temporal de Spark
CHECKPOINT_PATH = f"{SAP_DATA_PATH}/_checkpoints/vbak_al"  # checkpoint Auto Loader

# Depositar el lote 1 en staging primero, luego mover al landing
(spark.table(f"{CATALOG}.{SCHEMA}.vbak_bronze")
    .drop("_loaded_at", "_source_file")
    .limit(100)
    .write.mode("overwrite").option("header", "true")
    .csv(STAGING_PATH))                                     # Spark escribe aquí sus metadatos

# Mover solo el part-* al landing (simula lo que haría el extractor SAP)
for f in dbutils.fs.ls(STAGING_PATH):
    if f.name.startswith("part-"):
        dbutils.fs.cp(f.path, f"{LANDING_PATH}/batch_01/{f.name}")

print("Landing zone limpio — solo el CSV de datos")
dbutils.fs.ls(f"{LANDING_PATH}/batch_01")

# COMMAND ----------

# ── EJECUCIÓN 1 de Auto Loader: procesa el lote inicial ──
from pyspark.sql.functions import current_timestamp,col

def correr_autoloader():
    """Auto Loader en modo batch incremental: procesa lo nuevo y termina."""
    stream = (spark.readStream
        .format("cloudFiles")                                       # ← Auto Loader
        .option("cloudFiles.format", "csv")                         # formato archivos SAP
        .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)       # schema aprendido
        .option("cloudFiles.inferColumnTypes", "true")              # tipos automáticos
        .option("header", "true")
        # ── Manejo de campos desconocidos ──────────────────────
        .option("cloudFiles.schemaEvolutionMode", "rescue")   # ← clave
        .option("rescuedDataColumn", "_rescued_data")         # ← nombre explícito
        # ───────────────────────────────────────────────────────
        .option("pathGlobFilter", "vbak_*.csv")
        .load(LANDING_PATH)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source_file_name", col("_metadata.file_name")) 
        .withColumn("_autoloader_ts", current_timestamp()))

    query = (stream.writeStream
        .option("checkpointLocation", CHECKPOINT_PATH)              # registro de lo procesado
        .option("mergeSchema", "true")
        .trigger(availableNow=True)                                 # batch incremental, no 24/7
        .toTable(TARGET_TABLE))
    query.awaitTermination()
    return spark.table(TARGET_TABLE).count()

n = correr_autoloader()
print(f"EJECUCIÓN 1 → tabla {TARGET_TABLE}: {n:,} registros")

# COMMAND ----------

# ── EJECUCIÓN 2: correr de nuevo SIN archivos nuevos ──
# El momento clave del lab: el checkpoint sabe qué ya procesó

n = correr_autoloader()
print(f"EJECUCIÓN 2 (sin archivos nuevos) → total: {n:,} registros")
print()
print("Resultado: el conteo NO cambió — Auto Loader no reprocesó nada.")
print("Eso es lo que spark.read NO puede hacer.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from laboratory_sap_dev.sap_course.vbak_autoloader

# COMMAND ----------

# ── EJECUCIÓN 3: SAP deposita un archivo nuevo → solo procesa ese ──
# Lote 2: llegan 50 órdenes nuevas en la extracción de la mañana

# 1. Spark escribe en staging (sus metadatos quedan aquí)
(spark.table(f"{CATALOG}.{SCHEMA}.vbak_bronze")
    .drop("_loaded_at", "_source_file")
    .orderBy("VBELN", ascending=False)
    .limit(50)
    .write.mode("overwrite").option("header", "true")
    .csv(STAGING_PATH))

# 2. Limpiar batch_02 si existe (re-ejecuciones del notebook)
try:
    dbutils.fs.rm(f"{LANDING_PATH}/batch_02", recurse=True)
except:
    pass

# 3. Solo el part-* va al landing (simula extractor SAP)
for f in dbutils.fs.ls(STAGING_PATH):
    if f.name.startswith("part-"):
        dbutils.fs.cp(
            f.path,
            f"{LANDING_PATH}/batch_02/vbak_batch_02.csv"
        )
        break

print("Lote 2 depositado: 50 órdenes nuevas")

n = correr_autoloader()
print(f"EJECUCIÓN 3 → total: {n:,} registros (100 iniciales + 50 nuevas)")
print()
print("Auto Loader procesó SOLO el archivo nuevo — incremental real.")

# COMMAND ----------

# ── Verificar _rescued_data: auditoría de calidad gratis ──
from pyspark.sql.functions import col as _col
df_target = spark.table(TARGET_TABLE)
if "_rescued_data" in df_target.columns:
    rescatados = df_target.filter(_col("_rescued_data").isNotNull()).count()
    print(f"Registros con datos rescatados (no matchearon el schema): {rescatados}")
    print("En producción: monitorear esta columna detecta cambios en los extractores SAP")
else:
    print("Sin columna _rescued_data — todos los datos matchearon el schema")

# COMMAND ----------

# ── Simular extractor SAP que agrega un campo nuevo sin avisar ──
# Esto pasa en S/4HANA cuando activan un campo custom (ZZFIELD)

csv_malo = """VBELN,ERDAT,AUART,KUNNR,NETWR,WAERK,ZZREGION_NUEVA
0000000999,20240101,TA,1000,9999.00,COP,ANTIOQUIA
0000000998,20240102,TA,2000,8888.00,COP,CUNDINAMARCA"""

# Escribir directamente al landing (ya es un CSV limpio, sin metadatos)
dbutils.fs.put(
    f"{LANDING_PATH}/batch_rescue/vbak_con_campo_nuevo.csv",
    csv_malo,
    overwrite=True
)
print("✅ CSV con columna ZZREGION_NUEVA,NEWFIELD depositado en landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Las 3 reglas de Auto Loader para producción SAP
# MAGIC
# MAGIC 1. **`trigger(availableNow=True)`** = modo batch incremental: procesa lo pendiente
# MAGIC    y apaga el cluster. NO es streaming 24/7 — perfecto para extracciones nocturnas.
# MAGIC    Programarlo como Job con Jobs cluster (5x más barato — Módulo 4).
# MAGIC 2. **`checkpointLocation` es sagrado**: si lo borras, reprocesa TODO.
# MAGIC    Nunca compartirlo entre dos streams diferentes.
# MAGIC 3. **Monitorear `_rescued_data`**: si empieza a llenarse, el extractor SAP
# MAGIC    cambió el formato — alerta temprana de calidad.

# COMMAND ----------

# ══════════════════════════════════════════════════════════════
# 🔄 RESET COMPLETO — ejecutar solo cuando quieras empezar de cero
# ══════════════════════════════════════════════════════════════
# ⚠️  ADVERTENCIA: borra tabla, checkpoint y landing zone completo

def reset_autoloader(confirmar=False):
    if not confirmar:
        print("⚠️  Para ejecutar el reset, llama: reset_autoloader(confirmar=True)")
        return

    pasos = [
        ("Tabla Bronze",    lambda: spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")),
        ("Checkpoint",      lambda: dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)),
        # ("Landing/batch_01",lambda: dbutils.fs.rm(f"{LANDING_PATH}/batch_01", recurse=True)),
        # ("Landing/batch_02",lambda: dbutils.fs.rm(f"{LANDING_PATH}/batch_02", recurse=True)),
        # ("Landing/batch_rescue", lambda: dbutils.fs.rm(f"{LANDING_PATH}/batch_rescue", recurse=True)),
        ("Staging",         lambda: dbutils.fs.rm(STAGING_PATH, recurse=True)),
    ]

    for nombre, accion in pasos:
        try:
            accion()
            print(f"  ✅ {nombre} eliminado")
        except Exception as e:
            print(f"  ℹ️  {nombre} no existía — omitido")

    print("\n🚀 Reset completo. Orden de ejecución:")
    print("   1️⃣  Celda: preparar batch_01  → correr_autoloader()")
    print("   2️⃣  Celda: preparar batch_02  → correr_autoloader()")
    print("   3️⃣  Celda: preparar batch_rescue → correr_autoloader()")
    print("   4️⃣  Celda: verificar _rescued_data")

# Ejecutar así:
reset_autoloader(confirmar=True)

# COMMAND ----------

## 5.6 Resumen del Pipeline Medallion completo

# COMMAND ----------

# Vista final de todas las tablas del pipeline
tablas = spark.sql("""
    SHOW TABLES IN laboratory_sap_dev.sap_course
""")

print("=== TABLAS DEL PIPELINE MEDALLION ===")
for row in tablas.collect():
    tabla = row["tableName"]
    try:
        n = spark.table(f"laboratory_sap_dev.sap_course.{tabla}").count()
        capa = "BRONZE" if "_bronze" in tabla else "SILVER" if "_silver" in tabla else "GOLD" if "gold_" in tabla else "OTHER"
        print(f"  [{capa:<6}] {tabla:<35} {n:>8,} filas")
    except:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del Módulo 5
# MAGIC
# MAGIC ✅ **Bronze**: datos SAP crudos con metadatos de auditoría  
# MAGIC ✅ **Silver**: datos limpios, validados, fechas convertidas, deduplicados  
# MAGIC ✅ **Gold**: KPIs de negocio — fin_summary, sales_kpis, customer_360  
# MAGIC ✅ **Auto Loader**: ingesta incremental sin listar directorios completos  
# MAGIC ✅ Pipeline completo de 8 tablas SAP desde CSV hasta dashboards  
# MAGIC
# MAGIC **Próximo módulo**: Delta Sharing — compartir estas tablas Gold con recipientes externos
