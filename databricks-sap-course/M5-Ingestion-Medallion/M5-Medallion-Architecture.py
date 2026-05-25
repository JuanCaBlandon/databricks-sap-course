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

spark.sql("USE sap_course")
SAP_DATA_PATH = "/FileStore/sap_course/datasets"
print("Contexto: sap_course")

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

# Bronze: ingestar con metadatos de auditoría
def ingest_bronze(tabla_nombre):
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(f"{SAP_DATA_PATH}/{tabla_nombre}.csv")
          .withColumn("_ingestion_ts",  current_timestamp())
          .withColumn("_source_system", lit("SAP_ECC"))
          .withColumn("_source_file",   lit(f"{tabla_nombre}.csv"))
          .withColumn("_batch_id",      lit(datetime.now().strftime("%Y%m%d_%H%M%S"))))
    
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"sap_course.{tabla_nombre.lower()}_bronze"))
    
    return df.count()

print("Cargando capa Bronze...")
for tabla in ["BKPF","BSEG","KNA1","MARA","VBAK","VBAP","LFA1","EKKO"]:
    n = ingest_bronze(tabla)
    print(f"  {tabla:<6} -> bronze  ({n:>6,} filas)")

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
df_bkpf_bronze = spark.table("sap_course.bkpf_bronze")

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
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("sap_course.bkpf_silver"))

print(f"BKPF Silver: {df_bkpf_silver.count():,} registros")
print(f"  Válidos  : {df_bkpf_silver.filter(col('_is_valid') == True).count():,}")
print(f"  Inválidos: {df_bkpf_silver.filter(col('_is_valid') == False).count():,}")

# COMMAND ----------

# Silver: VBAK — Órdenes de venta limpias
df_vbak_bronze = spark.table("sap_course.vbak_bronze")

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

(df_vbak_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("sap_course.vbak_silver"))

print(f"VBAK Silver: {df_vbak_silver.count():,} registros")

# COMMAND ----------

# Silver: KNA1 — Maestro de clientes limpio
df_kna1_silver = (spark.table("sap_course.kna1_bronze")
    .withColumn("NAME1",  trim(col("NAME1")))
    .withColumn("ORT01",  upper(trim(col("ORT01"))))
    .withColumn("LAND1",  upper(trim(col("LAND1"))))
    .withColumn("_silver_ts", current_timestamp())
    .dropDuplicates(["KUNNR"]))

(df_kna1_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("sap_course.kna1_silver"))

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
    CREATE OR REPLACE TABLE sap_course.gold_fin_summary AS
    SELECT
        b.BUKRS                                    AS company_code,
        b.GJAHR                                    AS fiscal_year,
        b.WAERK                                    AS currency,
        COUNT(DISTINCT b.BELNR)                    AS num_documents,
        COUNT(DISTINCT b.USNAM)                    AS num_users,
        ROUND(SUM(s.DMBTR), 2)                     AS total_amount,
        ROUND(AVG(s.DMBTR), 2)                     AS avg_line_amount,
        COUNT(s.BUZEI)                             AS num_line_items
    FROM sap_course.bkpf_silver b
    JOIN sap_course.bseg_bronze s
        ON  b.BELNR = s.BELNR
        AND b.BUKRS = s.BUKRS
        AND b.GJAHR = s.GJAHR
    WHERE b._is_valid = TRUE
    GROUP BY b.BUKRS, b.GJAHR, b.WAERK
    ORDER BY b.GJAHR DESC, total_amount DESC
""")
print("GOLD: fin_summary creado")
spark.table("sap_course.gold_fin_summary").show()

# COMMAND ----------

# GOLD 2: KPIs de ventas por año, mes y organización
spark.sql("""
    CREATE OR REPLACE TABLE sap_course.gold_sales_kpis AS
    SELECT
        v.YEAR                                     AS year,
        v.MONTH                                    AS month,
        v.VKORG                                    AS sales_org,
        v.WAERK                                    AS currency,
        COUNT(DISTINCT v.VBELN)                    AS num_orders,
        COUNT(DISTINCT v.KUNNR)                    AS num_customers,
        ROUND(SUM(v.NETWR), 2)                     AS total_revenue,
        ROUND(AVG(v.NETWR), 2)                     AS avg_order_value,
        ROUND(MAX(v.NETWR), 2)                     AS max_order_value,
        ROUND(MIN(v.NETWR), 2)                     AS min_order_value
    FROM sap_course.vbak_silver v
    WHERE v._is_valid = TRUE
    GROUP BY v.YEAR, v.MONTH, v.VKORG, v.WAERK
    ORDER BY v.YEAR DESC, v.MONTH DESC, total_revenue DESC
""")
print("GOLD: sales_kpis creado")
spark.table("sap_course.gold_sales_kpis").show(10)

# COMMAND ----------

# GOLD 3: Customer 360 — vista unificada del cliente
spark.sql("""
    CREATE OR REPLACE TABLE sap_course.gold_customer_360 AS
    SELECT
        k.KUNNR                                    AS customer_id,
        k.NAME1                                    AS customer_name,
        k.LAND1                                    AS country,
        k.ORT01                                    AS city,
        COUNT(DISTINCT v.VBELN)                    AS total_orders,
        ROUND(SUM(v.NETWR), 2)                     AS total_revenue,
        ROUND(AVG(v.NETWR), 2)                     AS avg_order_value,
        MIN(v.ERDAT_DT)                            AS first_order_date,
        MAX(v.ERDAT_DT)                            AS last_order_date,
        DATEDIFF(MAX(v.ERDAT_DT), MIN(v.ERDAT_DT)) AS customer_lifetime_days,
        CASE
            WHEN SUM(v.NETWR) > 500000  THEN 'PLATINUM'
            WHEN SUM(v.NETWR) > 100000  THEN 'GOLD'
            WHEN SUM(v.NETWR) > 50000   THEN 'SILVER'
            ELSE 'STANDARD'
        END                                         AS customer_tier
    FROM sap_course.kna1_silver k
    LEFT JOIN sap_course.vbak_silver v
        ON k.KUNNR = v.KUNNR AND v._is_valid = TRUE
    GROUP BY k.KUNNR, k.NAME1, k.LAND1, k.ORT01
    ORDER BY total_revenue DESC NULLS LAST
""")
print("GOLD: customer_360 creado")

# Distribución de clientes por tier
spark.sql("""
    SELECT customer_tier, COUNT(*) as num_customers, 
           ROUND(SUM(total_revenue),2) as revenue
    FROM sap_course.gold_customer_360
    GROUP BY customer_tier
    ORDER BY revenue DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 Auto Loader — Ingesta incremental de archivos
# MAGIC
# MAGIC Auto Loader detecta automáticamente archivos nuevos en cloud storage
# MAGIC sin necesidad de listar el directorio completo. Ideal para feeds SAP que
# MAGIC llegan como archivos periódicos (diarios, por cierre contable, etc.)

# COMMAND ----------

# Simular ingesta incremental con Auto Loader
# En producción: apuntar a ADLS/S3/GCS donde SAP deposita los archivos

AUTOLOADER_PATH        = "/FileStore/sap_course/autoloader/input"
AUTOLOADER_CHECKPOINT  = "/FileStore/sap_course/autoloader/checkpoint"
AUTOLOADER_TARGET      = "sap_course.vbak_autoloader"

# Crear un archivo de ejemplo en la ruta de Auto Loader
df_sample = spark.table("sap_course.vbak_bronze").limit(100)
df_sample.write.mode("overwrite").csv(AUTOLOADER_PATH, header=True)

# Configurar Auto Loader
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{AUTOLOADER_CHECKPOINT}/schema")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(AUTOLOADER_PATH)
    .withColumn("_autoloader_ts", current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", AUTOLOADER_CHECKPOINT)
    .trigger(availableNow=True)   # Procesa lo disponible y se detiene
    .toTable(AUTOLOADER_TARGET))

print(f"Auto Loader completado")
print(f"Registros en {AUTOLOADER_TARGET}: {spark.table(AUTOLOADER_TARGET).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6 Resumen del Pipeline Medallion completo

# COMMAND ----------

# Vista final de todas las tablas del pipeline
tablas = spark.sql("""
    SHOW TABLES IN sap_course
""")

print("=== TABLAS DEL PIPELINE MEDALLION ===")
for row in tablas.collect():
    tabla = row["tableName"]
    try:
        n = spark.table(f"sap_course.{tabla}").count()
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
