# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 2: Delta Lake — El Corazón del Lakehouse
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC - Entender por qué Delta Lake reemplaza Parquet plano
# MAGIC - Implementar transacciones ACID sobre datos SAP
# MAGIC - Usar Time Travel para auditoría y rollback
# MAGIC - Configurar Schema Evolution y Change Data Feed (CDF)
# MAGIC
# MAGIC ---
# MAGIC ## 2.1 ¿Por qué Delta Lake y no Parquet plano?
# MAGIC
# MAGIC | Característica | Parquet plano | Delta Lake |
# MAGIC |---|---|---|
# MAGIC | Transacciones ACID | ❌ | ✅ |
# MAGIC | Time Travel | ❌ | ✅ |
# MAGIC | Schema enforcement | ❌ | ✅ |
# MAGIC | Schema evolution | ❌ | ✅ (controlada) |
# MAGIC | Change Data Feed | ❌ | ✅ |
# MAGIC | OPTIMIZE / Z-Order | ❌ | ✅ |
# MAGIC | Costo de storage | Bajo | Bajo (mismo Parquet bajo el capó) |
# MAGIC
# MAGIC > Delta Lake **no es un formato diferente** — es una **capa de metadatos** (transaction log)
# MAGIC > sobre Parquet que le agrega confiabilidad.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 El Transaction Log — cómo funciona ACID en un Data Lake

# COMMAND ----------

# Setup: crear base de datos del curso
spark.sql("CREATE DATABASE IF NOT EXISTS sap_course COMMENT 'Base de datos del curso SAP + Databricks'")
spark.sql("USE sap_course")

SAP_DATA_PATH = "/FileStore/sap_course/datasets"
DELTA_PATH    = "/FileStore/sap_course/delta"

print("Database 'sap_course' activa")

# COMMAND ----------

# Cargar BKPF como tabla Delta
df_bkpf = (spark.read
           .option("header", "true")
           .option("inferSchema", "true")
           .csv(f"{SAP_DATA_PATH}/BKPF.csv"))

# Escribir como Delta con particionamiento por año fiscal
(df_bkpf.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("GJAHR")
    .option("overwriteSchema", "true")
    .saveAsTable("sap_course.bkpf_bronze"))

print("Tabla bkpf_bronze creada como Delta Lake")
print(f"Registros: {spark.table('sap_course.bkpf_bronze').count():,}")

# COMMAND ----------

# Ver el Transaction Log — el "libro contable" de Delta Lake
# Cada operación queda registrada en _delta_log/
display(spark.sql("DESCRIBE HISTORY sap_course.bkpf_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Time Travel — Versionamiento de tablas SAP
# MAGIC
# MAGIC En un contexto SAP, Time Travel es crítico para:
# MAGIC - **Auditoría**: ¿cómo estaba el documento contable X el día de cierre?
# MAGIC - **Rollback**: revertir una carga incorrecta de datos SAP
# MAGIC - **Debugging**: comparar estado de datos antes y después de una migración

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# Simular una actualización de datos (corrección de documento contable)
df_update = spark.sql("""
    SELECT *, 'CORREGIDO' as BKTXT
    FROM sap_course.bkpf_bronze
    WHERE BELNR = '0000000001'
""")

(df_update.write
    .format("delta")
    .mode("append")
    .saveAsTable("sap_course.bkpf_bronze"))

print("Actualización simulada — nueva versión creada en el transaction log")

# COMMAND ----------

# Ver versiones disponibles
display(spark.sql("DESCRIBE HISTORY sap_course.bkpf_bronze LIMIT 5"))

# COMMAND ----------

# Time Travel: consultar versión anterior
print("=== VERSIÓN ACTUAL ===")
spark.sql("SELECT COUNT(*) as total FROM sap_course.bkpf_bronze").show()

print("=== VERSIÓN 0 (original) ===")
spark.sql("SELECT COUNT(*) as total FROM sap_course.bkpf_bronze VERSION AS OF 0").show()

# COMMAND ----------

# Time Travel por timestamp
from datetime import datetime, timedelta

# Consultar cómo estaba la tabla hace 1 hora (simulado)
# En producción real: usar la fecha de un cierre contable SAP
spark.sql(f"""
    SELECT BELNR, BUKRS, GJAHR, BKTXT
    FROM sap_course.bkpf_bronze
    TIMESTAMP AS OF '{(datetime.now() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")}'
    LIMIT 5
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Restaurar una tabla a una versión anterior
# MAGIC
# MAGIC Caso de uso real: carga incorrecta de datos SAP → rollback a versión previa

# COMMAND ----------

# Restaurar a versión 0 (antes de la actualización incorrecta)
spark.sql("RESTORE TABLE sap_course.bkpf_bronze TO VERSION AS OF 0")
print("Tabla restaurada a versión 0")
print(f"Registros después del restore: {spark.table('sap_course.bkpf_bronze').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 VACUUM — Gestión de retención
# MAGIC
# MAGIC Por defecto Delta Lake retiene versiones históricas por **30 días**.
# MAGIC VACUUM elimina los archivos físicos más antiguos que el retention period.

# COMMAND ----------

# Ver cuánto espacio ocupa la tabla
spark.sql("DESCRIBE DETAIL sap_course.bkpf_bronze").select("name","numFiles","sizeInBytes").show()

# VACUUM — dry run primero (no elimina nada, solo muestra qué eliminaría)
# En producción: nunca bajar de 168 horas (7 días) para no romper Time Travel
spark.sql("VACUUM sap_course.bkpf_bronze RETAIN 168 HOURS DRY RUN")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Schema Evolution — Agregar columnas a tablas SAP sin romper el pipeline
# MAGIC
# MAGIC Caso real: SAP agrega un campo nuevo en un enhancement. El pipeline debe seguir funcionando.

# COMMAND ----------

from pyspark.sql.functions import lit

# Simular que SAP agrega el campo KOSTL (centro de costo) en un nuevo extracto
df_bkpf_v2 = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(f"{SAP_DATA_PATH}/BKPF.csv")
              .withColumn("KOSTL", lit("CC001"))  # Campo nuevo
              .withColumn("SEGMENT", lit("SEG01")))  # Otro campo nuevo

# Sin mergeSchema=True esto fallaría porque la tabla no tiene KOSTL
(df_bkpf_v2.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")  # Schema evolution habilitado
    .saveAsTable("sap_course.bkpf_bronze"))

print("Schema evolution exitoso — columnas nuevas agregadas sin romper la tabla")
spark.sql("DESCRIBE sap_course.bkpf_bronze").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7 Change Data Feed (CDF) — Captura de cambios incrementales
# MAGIC
# MAGIC CDF es la base para pipelines CDC (Change Data Capture) desde SAP.
# MAGIC En vez de procesar toda la tabla en cada ejecución, solo procesas los cambios.

# COMMAND ----------

# Habilitar CDF en una tabla
spark.sql("""
    CREATE TABLE IF NOT EXISTS sap_course.vbak_silver
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    AS SELECT * FROM (
        SELECT * FROM delta.`/FileStore/sap_course/datasets/VBAK.csv`
    ) LIMIT 0
""")

# Cargar datos iniciales
df_vbak = (spark.read
           .option("header", "true")
           .option("inferSchema", "true")
           .csv(f"{SAP_DATA_PATH}/VBAK.csv"))

(df_vbak.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("sap_course.vbak_silver"))

print("Tabla vbak_silver con CDF habilitado")

# COMMAND ----------

# Simular actualización (orden de venta modificada en SAP)
spark.sql("""
    UPDATE sap_course.vbak_silver
    SET NETWR = NETWR * 1.1
    WHERE AUART = 'ZOR'
""")
print("Actualización aplicada — precios aumentados 10% en ordenes ZOR")

# COMMAND ----------

# Leer SOLO los cambios con CDF — esto es lo que usarías en un pipeline incremental
cambios = (spark.read
           .format("delta")
           .option("readChangeFeed", "true")
           .option("startingVersion", 1)
           .table("sap_course.vbak_silver"))

print(f"Registros cambiados: {cambios.count():,}")
cambios.groupBy("_change_type").count().show()
# _change_type: update_preimage, update_postimage, insert, delete

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8 Lab Integrador: Analizar el Transaction Log
# MAGIC
# MAGIC El transaction log de Delta Lake vive en `_delta_log/` como archivos JSON.
# MAGIC Databricks lo expone via `DESCRIBE HISTORY`.

# COMMAND ----------

# Historial completo de operaciones sobre bkpf_bronze
print("=== HISTORIAL DE OPERACIONES — BKPF ===")
history = spark.sql("DESCRIBE HISTORY sap_course.bkpf_bronze")
history.select("version","timestamp","operation","operationParameters","operationMetrics").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.9 Lab Final: Pipeline básico con tablas SAP
# MAGIC
# MAGIC Crear las tablas Bronze de los 8 datasets SAP como Delta Lake

# COMMAND ----------

sap_tables = ["BKPF","BSEG","KNA1","MARA","VBAK","VBAP","LFA1","EKKO"]

for tabla in sap_tables:
    df = (spark.read
          .option("header","true")
          .option("inferSchema","true")
          .csv(f"{SAP_DATA_PATH}/{tabla}.csv"))
    
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema","true")
        .saveAsTable(f"sap_course.{tabla.lower()}_bronze"))
    
    count = spark.table(f"sap_course.{tabla.lower()}_bronze").count()
    print(f"  {tabla:<6} -> sap_course.{tabla.lower()}_bronze  ({count:>6,} rows)")

print("\nTodas las tablas Bronze creadas exitosamente!")

# COMMAND ----------

# Verificar con una consulta cross-table: documentos contables con cliente
resultado = spark.sql("""
    SELECT 
        b.BUKRS,
        b.GJAHR,
        COUNT(DISTINCT b.BELNR)  AS num_documentos,
        ROUND(SUM(s.DMBTR), 2)   AS total_monto
    FROM sap_course.bkpf_bronze b
    JOIN sap_course.bseg_bronze s
        ON  b.BELNR = s.BELNR
        AND b.BUKRS = s.BUKRS
        AND b.GJAHR = s.GJAHR
    GROUP BY b.BUKRS, b.GJAHR
    ORDER BY b.GJAHR DESC, total_monto DESC
""")

print("=== Documentos contables por compañía y año fiscal ===")
resultado.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del Módulo 2
# MAGIC
# MAGIC ✅ Delta Lake agrega ACID, Time Travel y Schema Evolution sobre Parquet  
# MAGIC ✅ El Transaction Log registra cada operación — base de la auditoría  
# MAGIC ✅ Time Travel permite consultar datos SAP en cualquier punto histórico  
# MAGIC ✅ CDF habilita pipelines incrementales (CDC) desde fuentes SAP  
# MAGIC ✅ Las 8 tablas SAP están cargadas como Delta Bronze  
# MAGIC
# MAGIC **Próximo módulo**: Databricks SQL — construir dashboards ejecutivos sobre estos datos
