# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 2: Delta Lake — El Corazón del Lakehouse
# MAGIC
# MAGIC ## Objetivos de aprendizaje (Clases 2 y 3)
# MAGIC - Entender cómo funcionan los **Volumes** en Unity Catalog para cargar archivos SAP
# MAGIC - Conocer **Lakehouse Federation** y los conectores nativos de Databricks
# MAGIC - Implementar transacciones **ACID** sobre datos SAP con Delta Lake
# MAGIC - Usar **Time Travel** para auditoría y rollback sobre tablas SAP
# MAGIC - Configurar **Schema Evolution** para campos nuevos de SAP
# MAGIC - Implementar **Change Data Feed (CDF)** para pipelines incrementales
# MAGIC
# MAGIC ---
# MAGIC ## Datasets SAP que usaremos en este módulo
# MAGIC
# MAGIC | Tabla | Módulo SAP | Descripción | Registros |
# MAGIC |---|---|---|---|
# MAGIC | `BKPF` | FI — Finanzas | Cabeceras de documentos contables | 5,000 |
# MAGIC | `BSEG` | FI — Finanzas | Posiciones de documentos contables | ~7,400 |
# MAGIC | `KNA1` | SD — Ventas | Maestro de clientes | 500 |
# MAGIC | `MARA` | MM — Materiales | Maestro de materiales | 300 |
# MAGIC | `VBAK` | SD — Ventas | Cabeceras de órdenes de venta | 2,000 |
# MAGIC | `VBAP` | SD — Ventas | Posiciones de órdenes de venta | ~4,500 |
# MAGIC | `LFA1` | MM — Compras | Maestro de proveedores | 200 |
# MAGIC | `EKKO` | MM — Compras | Cabeceras de órdenes de compra | 1,000 |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # CLASE 2 — JUEVES
# MAGIC ## Sección 2.1: Volumes — Donde viven los archivos en Databricks
# MAGIC
# MAGIC Un **Volume** es un objeto de Unity Catalog para almacenar archivos (no tablas).
# MAGIC Reemplaza al antiguo DBFS como la forma recomendada en Databricks moderno.
# MAGIC
# MAGIC ### Jerarquía:
# MAGIC ```
# MAGIC Catalog (laboratory_dev)
# MAGIC  └── Schema (bronze)
# MAGIC       └── Volume (curso_databricks)
# MAGIC            ├── BKPF.csv
# MAGIC            ├── BSEG.csv
# MAGIC            ├── VBAK.csv
# MAGIC            └── ...
# MAGIC ```
# MAGIC
# MAGIC ### Cómo subir los archivos SAP:
# MAGIC 1. En Databricks: **Catalog Explorer → laboratory_dev → bronze → Volumes → curso_databricks**
# MAGIC 2. Botón **"Upload"** → arrastrar los CSV del repositorio
# MAGIC 3. Verificar que aparecen en la lista del Volume

# COMMAND ----------

# Configuración del módulo — ajustar si tu catalog/schema son diferentes
CATALOG       = "laboratory_dev"
SCHEMA        = "sap_course"
VOLUME_PATH   = f"/Volumes/{CATALOG}/bronze/curso_databricks"

# Crear el schema si no existe
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"Catalog  : {CATALOG}")
print(f"Schema   : {CATALOG}.{SCHEMA}")
print(f"Volume   : {VOLUME_PATH}")

# COMMAND ----------

# Verificar que los archivos SAP están en el Volume
import os

tablas_sap = ["BKPF", "BSEG", "KNA1", "MARA", "VBAK", "VBAP", "LFA1", "EKKO"]

print("=== Archivos SAP en el Volume ===")
for tabla in tablas_sap:
    path = f"{VOLUME_PATH}/{tabla}.csv"
    try:
        df_check = spark.read.option("header","true").csv(path)
        n = df_check.count()
        print(f"  OK  {tabla}.csv  —  {n:,} registros")
    except Exception as e:
        print(f"  ERR {tabla}.csv  —  No encontrado. Subir desde: datasets/{tabla}.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 2.2: Lakehouse Federation y Conectores
# MAGIC
# MAGIC **Lakehouse Federation** permite consultar datos de sistemas externos directamente
# MAGIC desde Databricks SQL sin moverlos ni replicarlos.
# MAGIC
# MAGIC ### Conectores nativos disponibles:
# MAGIC
# MAGIC | Conector | Sistema | Caso de uso SAP |
# MAGIC |---|---|---|
# MAGIC | **Delta Sharing** | Capa de salida | Compartir datos SAP con SAC, Tableau, pandas |
# MAGIC | **PostgreSQL** | BD relacional | Datos operativos combinados con SAP |
# MAGIC | **MySQL** | BD relacional | Catálogo de productos junto con MARA |
# MAGIC | **Snowflake** | Data warehouse | Migración gradual de Snowflake a Databricks |
# MAGIC | **BigQuery** | DW Google | Datos marketing + datos SAP FI |
# MAGIC | **Azure SQL / Synapse** | Microsoft Azure | Datos Azure AD + datos SAP |
# MAGIC | **Amazon Redshift** | DW AWS | Migración o consulta federada |
# MAGIC
# MAGIC ### Warehouse Migration
# MAGIC Herramienta que transpila queries de Redshift, Synapse, BigQuery → Databricks SQL
# MAGIC automáticamente. Útil para migrar reports de SAP BW/4HANA a Databricks.

# COMMAND ----------

# Ejemplo de Foreign Catalog (ejecutar con permisos de admin en workspace con UC)
# Este código crea una conexión a PostgreSQL y la expone como catalog en Unity Catalog

# -- SQL equivalente en el SQL Editor:
# CREATE CONNECTION postgres_conn TYPE POSTGRESQL
#   OPTIONS (
#     host '10.0.0.1',
#     port '5432',
#     user secret('scope','user'),
#     password secret('scope','pass')
#   );
#
# CREATE FOREIGN CATALOG postgres_catalog
#   USING CONNECTION postgres_conn
#   OPTIONS (database 'mi_base_datos');
#
# -- Luego consultar como si fueran tablas locales:
# SELECT * FROM postgres_catalog.public.clientes
# JOIN sap_course.kna1_bronze ON ...

print("Lakehouse Federation: CREATE CONNECTION + CREATE FOREIGN CATALOG")
print("Ver documentación: docs.databricks.com/lakehouse-federation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 2.3: ¿Por qué Delta Lake y no Parquet plano?
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
# MAGIC >
# MAGIC > **Analogía SAP**: SAP FI nunca guarda un documento contable a medias.
# MAGIC > Un asiento o está completo o no existe. Delta Lake aplica esa misma filosofía al data lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 2.4: El Transaction Log — el corazón de Delta Lake
# MAGIC
# MAGIC Cada operación sobre una tabla Delta queda registrada como un archivo JSON
# MAGIC numerado en el directorio `_delta_log/`:
# MAGIC ```
# MAGIC _delta_log/
# MAGIC   00000000000000000000.json  → v0: CREATE TABLE (schema inicial BKPF)
# MAGIC   00000000000000000001.json  → v1: INSERT 5,000 rows (carga inicial)
# MAGIC   00000000000000000002.json  → v2: UPDATE BKTXT (corrección de campo)
# MAGIC   00000000000000000003.json  → v3: OPTIMIZE + ZORDER BY BUKRS, GJAHR
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date,
    year, month, sum as _sum, count,
    round as _round
)
from pyspark.sql.types import IntegerType, DoubleType

# PASO 1: Cargar BKPF desde Volume como tabla Delta Bronze
# Particionamos por GJAHR (año fiscal) — columna de baja cardinalidad, filtro frecuente
df_bkpf = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{VOLUME_PATH}/BKPF.csv"))

print(f"BKPF cargado: {df_bkpf.count():,} registros")
print(f"Columnas: {df_bkpf.columns}")
df_bkpf.show(3, truncate=False)

# COMMAND ----------

# Escribir como Delta con particionamiento por año fiscal
(df_bkpf.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("GJAHR")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bkpf_bronze"))

n = spark.table(f"{CATALOG}.{SCHEMA}.bkpf_bronze").count()
print(f"Tabla {CATALOG}.{SCHEMA}.bkpf_bronze creada: {n:,} registros")
print(f"Particionada por: GJAHR (año fiscal SAP)")

# COMMAND ----------

# Ver el Transaction Log — el "libro contable" de Delta Lake
print("=== TRANSACTION LOG — BKPF ===")
spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.bkpf_bronze") \
    .select("version","timestamp","operation","operationMetrics") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 2.5: Time Travel — Auditoría y Rollback sobre datos SAP
# MAGIC
# MAGIC ### Casos de uso en contexto SAP:
# MAGIC - **Auditoría de cierre contable**: ¿cómo estaba BKPF el 31/12 a las 23:59?
# MAGIC - **Rollback de carga incorrecta**: el CSV de SAP llegó con errores → RESTORE en segundos
# MAGIC - **Debugging de migración**: comparar VBAK antes y después de una migración

# COMMAND ----------

# Simular una actualización incorrecta (como si llegara un CSV de SAP con errores)
from pyspark.sql.functions import when

df_bkpf_incorrecto = spark.table(f"{CATALOG}.{SCHEMA}.bkpf_bronze") \
    .withColumn("BKTXT", lit("DATO_INCORRECTO")) \
    .withColumn("WAERK", lit("XXX"))  # Moneda inválida

(df_bkpf_incorrecto.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bkpf_bronze"))

print("Carga incorrecta simulada — datos corruptos en la tabla")
spark.sql(f"SELECT BELTXT, WAERK FROM {CATALOG}.{SCHEMA}.bkpf_bronze LIMIT 3").show()

# COMMAND ----------

# Ver versiones disponibles
print("=== VERSIONES DISPONIBLES (Time Travel) ===")
spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.bkpf_bronze") \
    .select("version","timestamp","operation") \
    .show(truncate=False)

# COMMAND ----------

# TIME TRAVEL: consultar versión correcta (v0 — antes del error)
print("=== VERSIÓN ACTUAL (con datos incorrectos) ===")
spark.sql(f"SELECT COUNT(*) AS total, COUNT(DISTINCT WAERK) AS monedas FROM {CATALOG}.{SCHEMA}.bkpf_bronze").show()

print("=== VERSIÓN 0 (datos correctos originales) ===")
spark.sql(f"SELECT COUNT(*) AS total, COUNT(DISTINCT WAERK) AS monedas FROM {CATALOG}.{SCHEMA}.bkpf_bronze VERSION AS OF 0").show()

# COMMAND ----------

# RESTORE: volver a la versión correcta (rollback)
spark.sql(f"RESTORE TABLE {CATALOG}.{SCHEMA}.bkpf_bronze TO VERSION AS OF 0")
print("Tabla restaurada a versión 0 — datos correctos recuperados")
print(f"Registros después del restore: {spark.table(f'{CATALOG}.{SCHEMA}.bkpf_bronze').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sintaxis completa de Time Travel:
# MAGIC ```sql
# MAGIC -- Por versión
# MAGIC SELECT * FROM bkpf_bronze VERSION AS OF 0
# MAGIC
# MAGIC -- Por timestamp (caso típico: fecha de cierre contable SAP)
# MAGIC SELECT * FROM bkpf_bronze TIMESTAMP AS OF '2024-12-31 23:59:00'
# MAGIC
# MAGIC -- Rollback completo
# MAGIC RESTORE TABLE bkpf_bronze TO VERSION AS OF 0
# MAGIC RESTORE TABLE bkpf_bronze TO TIMESTAMP AS OF '2024-12-31 23:59:00'
# MAGIC
# MAGIC -- Ver espacio ocupado y número de archivos
# MAGIC DESCRIBE DETAIL bkpf_bronze
# MAGIC
# MAGIC -- Eliminar versiones antiguas (nunca bajar de 168 horas en producción)
# MAGIC VACUUM bkpf_bronze RETAIN 168 HOURS DRY RUN   -- ver qué eliminaría
# MAGIC VACUUM bkpf_bronze RETAIN 168 HOURS            -- ejecutar
# MAGIC ```

# COMMAND ----------

# Ver espacio y archivos de la tabla
spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.bkpf_bronze") \
    .select("name","numFiles","sizeInBytes","partitionColumns") \
    .show(truncate=False)

# VACUUM dry run — ver qué eliminaría sin borrar nada
spark.sql(f"VACUUM {CATALOG}.{SCHEMA}.bkpf_bronze RETAIN 168 HOURS DRY RUN")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # CLASE 3 — VIERNES
# MAGIC ## Sección 2.6: Change Data Feed (CDF) — CDC incremental desde SAP
# MAGIC
# MAGIC ### El problema sin CDF:
# MAGIC ```
# MAGIC Pipeline sin CDF (full scan):
# MAGIC   BKPF tiene 5,000 documentos
# MAGIC   Llegan 50 nuevos documentos de SAP
# MAGIC   → Proceso los 5,000 para encontrar los 50 → 100x ineficiente
# MAGIC
# MAGIC Pipeline con CDF (delta scan):
# MAGIC   Leo solo los 50 cambios desde la última ejecución
# MAGIC   → De horas a segundos
# MAGIC ```
# MAGIC
# MAGIC ### Tipos de cambio que captura CDF:
# MAGIC | `_change_type` | Cuándo aparece | Uso en SAP |
# MAGIC |---|---|---|
# MAGIC | `insert` | Nueva fila insertada | Nuevo documento contable, nueva orden de venta |
# MAGIC | `update_preimage` | Estado ANTES de actualización | Valor anterior para auditoría |
# MAGIC | `update_postimage` | Estado DESPUÉS de actualización | Nuevo valor — el que va a Silver |
# MAGIC | `delete` | Fila eliminada | Documento anulado, orden cancelada |

# COMMAND ----------

# Cargar VBAK (órdenes de venta) con CDF habilitado desde el inicio
df_vbak = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{VOLUME_PATH}/VBAK.csv"))

# Crear tabla con CDF habilitado
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.vbak_silver
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    AS SELECT * FROM delta.`{VOLUME_PATH}/VBAK.csv`
    WHERE 1=0
""")

# Carga inicial
(df_vbak.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.vbak_silver"))

print(f"vbak_silver con CDF: {spark.table(f'{CATALOG}.{SCHEMA}.vbak_silver').count():,} registros")

# COMMAND ----------

# Simular nuevas órdenes de venta llegando de SAP
# En producción: aquí llegaría el delta del extracto SAP del día

# UPDATE: ajuste de precios en órdenes tipo ZOR
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.vbak_silver
    SET NETWR = NETWR * 1.1
    WHERE AUART = 'ZOR'
""")
print("UPDATE aplicado: precios ZOR +10%")

# INSERT: nuevas órdenes del día
df_nuevas = spark.table(f"{CATALOG}.{SCHEMA}.vbak_silver").limit(10) \
    .withColumn("VBELN", lit("9999999999"))
df_nuevas.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.vbak_silver")
print("INSERT aplicado: 10 nuevas órdenes simuladas")

# COMMAND ----------

# Leer SOLO los cambios con CDF desde la versión 1
cambios = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table(f"{CATALOG}.{SCHEMA}.vbak_silver"))

print("=== CAMBIOS CAPTURADOS POR CDF ===")
print(f"Total registros de cambio: {cambios.count():,}")
cambios.groupBy("_change_type").count().orderBy("_change_type").show()

print("\nDetalle de los cambios (primeras 5 filas):")
cambios.select("VBELN","AUART","NETWR","_change_type","_commit_version","_commit_timestamp").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 2.7: Schema Evolution — Cuando SAP agrega campos
# MAGIC
# MAGIC ### Escenario real:
# MAGIC El equipo SAP activa el campo **KOSTL** (centro de costo) en un enhancement de BKPF.
# MAGIC Desde la próxima extracción, el CSV llega con esa columna nueva.
# MAGIC
# MAGIC ### Sin `mergeSchema`:
# MAGIC ```
# MAGIC AnalysisException: Field KOSTL does not exist in table bkpf_bronze.
# MAGIC Enable schema evolution by adding .option("mergeSchema", "true")
# MAGIC ```
# MAGIC
# MAGIC ### Con `mergeSchema=true`:
# MAGIC - KOSTL se agrega automáticamente como columna nullable
# MAGIC - Registros anteriores: KOSTL = null
# MAGIC - Registros nuevos: KOSTL = valor del campo SAP
# MAGIC - **El pipeline sigue corriendo sin intervención**

# COMMAND ----------

# Simular que SAP agrega campo KOSTL (centro de costo) y SEGMENT en un enhancement
df_bkpf_v2 = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{VOLUME_PATH}/BKPF.csv")
    .withColumn("KOSTL",   lit("CC001"))    # Campo nuevo: centro de costo
    .withColumn("SEGMENT", lit("SEG01"))    # Campo nuevo: segmento de negocio
    .withColumn("MANDT",   lit("100"))      # Campo nuevo: mandante SAP
)

print("Columnas del nuevo extracto SAP (con campos nuevos):")
print(df_bkpf_v2.columns)

# COMMAND ----------

# SIN mergeSchema → intentar escribir y ver el error
# (Comentado para no interrumpir el flujo — descomentarlo para ver el error)
# (df_bkpf_v2.write
#     .format("delta")
#     .mode("append")
#     .saveAsTable(f"{CATALOG}.{SCHEMA}.bkpf_bronze"))
# → AnalysisException: Field KOSTL does not exist...

# CON mergeSchema=True → los campos nuevos se agregan automáticamente
(df_bkpf_v2.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bkpf_bronze"))

print("Schema Evolution exitoso — columnas nuevas agregadas:")
spark.sql(f"DESCRIBE {CATALOG}.{SCHEMA}.bkpf_bronze") \
    .filter(col("col_name").isin(["KOSTL","SEGMENT","MANDT"])) \
    .show()

# COMMAND ----------

# Verificar que los registros anteriores tienen null en los campos nuevos
# y los registros nuevos tienen los valores correctos
print("=== Distribución de KOSTL (null = registros anteriores, CC001 = nuevos) ===")
spark.sql(f"""
    SELECT KOSTL, COUNT(*) AS registros
    FROM {CATALOG}.{SCHEMA}.bkpf_bronze
    GROUP BY KOSTL
    ORDER BY registros DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 2.8: Lab Final — Cargar todas las tablas SAP como Delta Bronze
# MAGIC
# MAGIC Crear las 8 tablas Bronze con las mejores prácticas aplicadas:
# MAGIC - Leer desde Volume (no desde DBFS)
# MAGIC - Escribir como Delta con particionamiento correcto
# MAGIC - Agregar metadatos de auditoría (_loaded_at, _source_file)

# COMMAND ----------

tablas_config = {
    "BKPF": {"partition": "GJAHR",  "desc": "Cabeceras documentos contables FI"},
    "BSEG": {"partition": "GJAHR",  "desc": "Posiciones documentos contables FI"},
    "KNA1": {"partition": "LAND1",  "desc": "Maestro de clientes SD"},
    "MARA": {"partition": "MTART",  "desc": "Maestro de materiales MM"},
    "VBAK": {"partition": None,     "desc": "Cabeceras órdenes de venta SD"},
    "VBAP": {"partition": None,     "desc": "Posiciones órdenes de venta SD"},
    "LFA1": {"partition": "LAND1",  "desc": "Maestro de proveedores MM"},
    "EKKO": {"partition": "BUKRS",  "desc": "Cabeceras órdenes de compra MM"},
}

print(f"{'Tabla':<8} {'Registros':>10} {'Partición':<12} {'Descripción'}")
print("=" * 70)

for tabla, config in tablas_config.items():
    try:
        df = (spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{VOLUME_PATH}/{tabla}.csv")
            .withColumn("_loaded_at",   current_timestamp())
            .withColumn("_source_file", lit(f"{tabla}.csv")))

        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema","true")
        if config["partition"]:
            writer = writer.partitionBy(config["partition"])

        writer.saveAsTable(f"{CATALOG}.{SCHEMA}.{tabla.lower()}_bronze")
        n = spark.table(f"{CATALOG}.{SCHEMA}.{tabla.lower()}_bronze").count()
        part = config["partition"] or "—"
        print(f"{tabla:<8} {n:>10,} {part:<12} {config['desc']}")
    except Exception as e:
        print(f"{tabla:<8} {'ERROR':>10} {str(e)[:40]}")

print("\nTodas las tablas Bronze creadas en Unity Catalog")

# COMMAND ----------

# Query integradora: documentos contables con posiciones
# Demuestra el join entre BKPF (cabeceras) y BSEG (posiciones)
print("=== JOIN BKPF + BSEG: Documentos por sociedad y año fiscal ===")
spark.sql(f"""
    SELECT
        b.BUKRS                             AS sociedad,
        b.GJAHR                             AS ejercicio,
        b.WAERK                             AS moneda,
        COUNT(DISTINCT b.BELNR)             AS num_documentos,
        COUNT(s.BUZEI)                      AS num_posiciones,
        ROUND(SUM(s.DMBTR), 2)              AS monto_total_local,
        ROUND(AVG(s.DMBTR), 2)             AS monto_promedio
    FROM {CATALOG}.{SCHEMA}.bkpf_bronze b
    JOIN {CATALOG}.{SCHEMA}.bseg_bronze s
        ON  b.BELNR = s.BELNR
        AND b.BUKRS = s.BUKRS
        AND b.GJAHR = s.GJAHR
    GROUP BY b.BUKRS, b.GJAHR, b.WAERK
    ORDER BY b.GJAHR DESC, monto_total_local DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del Módulo 2
# MAGIC
# MAGIC ### Clase 2 — Jueves:
# MAGIC ✅ **Volumes**: archivos SAP en `/Volumes/{catalog}/{schema}/{volume}/` — reemplazo de DBFS  
# MAGIC ✅ **Lakehouse Federation**: consultar PostgreSQL, MySQL, Snowflake desde Databricks sin ETL  
# MAGIC ✅ **Transaction Log**: `_delta_log/` registra cada operación — base de ACID y auditoría  
# MAGIC ✅ **Time Travel**: `VERSION AS OF` y `TIMESTAMP AS OF` para auditoría de cierre SAP  
# MAGIC ✅ **RESTORE TABLE**: rollback de carga incorrecta en segundos  
# MAGIC ✅ **VACUUM**: gestión de retención — mínimo 168 horas en producción  
# MAGIC
# MAGIC ### Clase 3 — Viernes:
# MAGIC ✅ **CDF**: Change Data Feed incremental — del full scan al delta scan  
# MAGIC ✅ **Schema Evolution**: `mergeSchema=true` para campos nuevos de SAP sin romper pipelines  
# MAGIC ✅ **8 tablas Bronze SAP** cargadas desde Volume con particionamiento correcto  
# MAGIC
# MAGIC **Próximo módulo**: Databricks SQL — CTEs, Window Functions, dashboards ejecutivos SAP
