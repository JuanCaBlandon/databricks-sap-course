# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 4: Optimización, Performance y Control de Costos
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC - Entender cómo Databricks cobra (DBUs, tipos de cómputo)
# MAGIC - Implementar tagging de recursos para visibilidad de costos
# MAGIC - Aplicar OPTIMIZE, Z-Ordering y Liquid Clustering
# MAGIC - Entender Photon Engine y qué acelera
# MAGIC - Diferenciar Disk Cache vs DataFrame Cache
# MAGIC - Analizar costos reales con tablas del sistema
# MAGIC
# MAGIC ---
# MAGIC ## 4.1 Cómo cobra Databricks — DBUs y tipos de cómputo
# MAGIC
# MAGIC **DBU (Databricks Unit)** = unidad de medida de cómputo. El costo real
# MAGIC depende del tipo de cómputo multiplicado por el precio del DBU en tu nube/región.
# MAGIC
# MAGIC | Tipo de cómputo | DBUs/hora | Cuándo usar |
# MAGIC |---|---|---|
# MAGIC | All-purpose cluster | 0.75 DBU/core | Notebooks desarrollo, ETL ad-hoc |
# MAGIC | Jobs cluster | 0.15 DBU/core | Pipelines productivos (5x más barato) |
# MAGIC | SQL Warehouse serverless | 2-16 DBU/hr | Queries SQL, dashboards, Genie |
# MAGIC | SQL Warehouse Pro | 1.5-12 DBU/hr | Dashboards producción |
# MAGIC | DLT (Lakeflow) | 0.2-0.87 DBU/core | Pipelines declarativos |
# MAGIC
# MAGIC > **Regla de oro**: NUNCA usar all-purpose clusters para jobs productivos.
# MAGIC > Un job cluster cuesta 5 veces menos que un all-purpose cluster.

# COMMAND ----------

spark.sql("USE sap_course")
print("Módulo 4: Optimización y Costos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Tagging de recursos — Visibilidad de costos por proyecto

# COMMAND ----------

# Ver los tags del cluster actual
# En producción: configurar tags en el cluster al crearlo
# Spark UI → Cluster → Tags
print("Tags recomendados para un proyecto SAP:")
tags_recomendados = {
    "proyecto"    : "sap-databricks-curso",
    "equipo"      : "data-engineering",
    "ambiente"    : "desarrollo",        # desarrollo | qa | produccion
    "modulo_sap"  : "FI-SD-MM",
    "cliente"     : "selecta",
    "centro_costo": "CC-DATA-001"
}
for k, v in tags_recomendados.items():
    print(f"  {k:<20} = {v}")

print("\nEstos tags aparecen en system.billing.usage para analizar costos por proyecto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 OPTIMIZE y Z-Ordering — Compactar y ordenar datos SAP

# COMMAND ----------

# Verificar estado de la tabla antes de optimizar
print("=== ANTES de OPTIMIZE ===")
spark.sql("DESCRIBE DETAIL sap_course.bkpf_bronze").select(
    "name","numFiles","sizeInBytes"
).show()

# COMMAND ----------

# OPTIMIZE: compacta archivos pequeños en archivos más grandes
# Ideal para tablas SAP que reciben cargas incrementales frecuentes
spark.sql("OPTIMIZE sap_course.bkpf_bronze")
print("OPTIMIZE completado")

spark.sql("DESCRIBE DETAIL sap_course.bkpf_bronze").select(
    "name","numFiles","sizeInBytes"
).show()

# COMMAND ----------

# Z-ORDERING: ordena los datos por columnas de filtro frecuente
# Para datos SAP, las columnas más usadas en WHERE son:
# BKPF: BUKRS (sociedad), GJAHR (ejercicio), BLART (tipo doc)
# VBAK: KUNNR (cliente), VKORG (org ventas), ERDAT (fecha)

print("Aplicando Z-Ordering en BKPF por columnas de filtro más comunes...")
spark.sql("""
    OPTIMIZE sap_course.bkpf_bronze
    ZORDER BY (BUKRS, GJAHR, BLART)
""")
print("Z-Ordering aplicado")

print("\nAplicando Z-Ordering en VBAK por cliente y fecha...")
spark.sql("""
    OPTIMIZE sap_course.vbak_silver
    ZORDER BY (KUNNR, ERDAT_DT, VKORG)
""")
print("Z-Ordering en VBAK aplicado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Liquid Clustering — La evolución del Z-Ordering
# MAGIC
# MAGIC Z-Ordering requiere ejecutar OPTIMIZE manualmente y reconstruye todo el archivo.
# MAGIC **Liquid Clustering** es automático, incremental y más eficiente.
# MAGIC
# MAGIC | Característica | Z-Ordering | Liquid Clustering |
# MAGIC |---|---|---|
# MAGIC | Tipo | Manual, batch | Automático, incremental |
# MAGIC | Cuando aplica | Al ejecutar OPTIMIZE | En cada escritura |
# MAGIC | Costo | Alto (reescribe todo) | Bajo (solo nuevos datos) |
# MAGIC | Cambiar columnas | Requiere reescritura total | Sin costo |
# MAGIC | Disponible desde | Delta Lake 1.0 | Delta Lake 3.1 (Databricks 13.3+) |

# COMMAND ----------

# Crear tabla nueva con Liquid Clustering habilitado
# Para tablas existentes: ALTER TABLE ... CLUSTER BY (...)
spark.sql("""
    CREATE TABLE IF NOT EXISTS sap_course.vbak_liquid
    CLUSTER BY (KUNNR, VKORG, AUART)
    AS SELECT * FROM sap_course.vbak_silver
""")

print("Tabla vbak_liquid creada con Liquid Clustering por KUNNR, VKORG, AUART")
print("Databricks ejecutará OPTIMIZE automáticamente en background")

# Verificar el clustering aplicado
spark.sql("DESCRIBE DETAIL sap_course.vbak_liquid").select(
    "name","clusteringColumns","numFiles"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 Auto Optimize — Optimize Write y Auto Compact

# COMMAND ----------

# Habilitar Auto Optimize en una tabla
# Optimize Write: Databricks ajusta automáticamente el tamaño de los archivos al escribir
# Auto Compact: después de cada escritura, compacta archivos pequeños automáticamente

spark.sql("""
    ALTER TABLE sap_course.bseg_bronze
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")
print("Auto Optimize habilitado en bseg_bronze")
print("  optimizeWrite: ajusta tamaño de archivos al escribir")
print("  autoCompact  : compacta archivos pequeños post-escritura")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 Photon Engine — Qué acelera y qué no

# COMMAND ----------

# Verificar si Photon está habilitado en el cluster actual
try:
    photon_enabled = spark.conf.get("spark.databricks.photon.enabled", "false")
    print(f"Photon habilitado: {photon_enabled}")
except:
    print("Photon: verificar en la configuración del cluster")

print("""
Photon acelera (usa vectorización en C++):
  OK  Lecturas y escrituras Delta Lake
  OK  Aggregaciones (GROUP BY, SUM, AVG, COUNT)
  OK  Joins (INNER, LEFT, HASH JOIN)
  OK  Filtros simples (WHERE col = 'valor')
  OK  Window Functions
  OK  Databricks SQL Warehouses

Photon NO acelera:
  NO  Python UDFs (User Defined Functions)
  NO  Pandas UDFs complejos
  NO  Operaciones Scala/Java custom
  NO  Algunas funciones de ML nativas

Recomendación para datos SAP: reemplazar UDFs Python
por funciones SQL nativas siempre que sea posible.
""")

# COMMAND ----------

# Ejemplo: reemplazar UDF Python por función SQL nativa (Photon-compatible)
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType

# MAL: UDF Python — Photon no puede optimizar esto
@udf(returnType=StringType())
def clasificar_documento_udf(blart):
    if blart in ['RE','KR']:    return 'Proveedor'
    elif blart in ['DR','KD']:  return 'Cliente'
    elif blart == 'SA':         return 'Asiento'
    else:                       return 'Otro'

# BIEN: función SQL nativa — Photon sí optimiza esto
df_bkpf = spark.table("sap_course.bkpf_bronze")

df_clasificado = df_bkpf.withColumn(
    "tipo_documento",
    when(col("BLART").isin("RE","KR"),  "Proveedor")
    .when(col("BLART").isin("DR","KD"), "Cliente")
    .when(col("BLART") == "SA",         "Asiento")
    .otherwise("Otro")
)

print("Clasificación de documentos SAP (SQL nativo, Photon-compatible):")
df_clasificado.groupBy("tipo_documento").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7 Caché: Disk Cache vs DataFrame Cache

# COMMAND ----------

print("""
DISK CACHE (Delta Cache):
  - Almacena datos descomprimidos en el SSD del worker
  - Transparente: Databricks lo gestiona automáticamente
  - Persiste entre consultas del mismo cluster
  - Ideal para: tablas que se consultan frecuentemente (ej. tablas Gold SAP)
  - Cómo activar: configuración del cluster o CACHE SELECT

DATAFRAME CACHE (.cache()):
  - Almacena el DataFrame en memoria JVM
  - Manual: tú decides qué cachear y cuándo
  - Se pierde al terminar la sesión Spark
  - Ideal para: DataFrames intermedios usados múltiples veces en el mismo notebook
  - Costo: memoria del cluster
""")

# DataFrame cache — para DataFrames usados múltiples veces en el notebook
df_ventas = (spark.table("sap_course.vbak_silver")
             .filter("_is_valid = TRUE")
             .cache())  # Se cachea en la primera acción

# Forzar materialización del cache
df_ventas.count()
print(f"DataFrame cacheado: {df_ventas.count():,} registros")

# Usar múltiples veces sin releer Delta Lake
print("Por org de ventas:", df_ventas.groupBy("VKORG").count().collect())
print("Por tipo orden:   ", df_ventas.groupBy("AUART").count().collect())

# Liberar el cache cuando ya no se necesita
df_ventas.unpersist()
print("Cache liberado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.8 Particionamiento — Cuándo ayuda y cuándo destruye el performance

# COMMAND ----------

print("""
REGLA GENERAL DE PARTICIONAMIENTO SAP:

  BUENAS columnas para particionar (cardinalidad baja, filtro frecuente):
    - GJAHR  (ejercicio fiscal: 5-10 valores)
    - BUKRS  (sociedad: 3-10 valores)
    - BLART  (tipo documento: 5-15 valores)

  MALAS columnas para particionar (cardinalidad alta):
    - BELNR  (número documento: millones de valores = millones de carpetas)
    - KUNNR  (código cliente: miles de valores)
    - ERDAT  (fecha exacta: miles de fechas únicas)

ANTI-PATRÓN MÁS COMÚN en datos SAP:
  Particionar por BUDAT (fecha de contabilización) crea una carpeta por día.
  Con años de datos SAP esto son miles de carpetas = 'small file problem'.
  Mejor: particionar por GJAHR + usar Z-Order por BUDAT.
""")

# Ejemplo: tabla con buen particionamiento para datos SAP
df_bkpf = (spark.read
           .option("header","true")
           .option("inferSchema","true")
           .csv("/FileStore/sap_course/datasets/BKPF.csv"))

(df_bkpf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .partitionBy("GJAHR","BUKRS")       # Buen particionamiento
    .saveAsTable("sap_course.bkpf_partitioned"))

spark.sql("DESCRIBE DETAIL sap_course.bkpf_partitioned").select(
    "name","partitionColumns","numFiles"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.9 Estrategias de compactación — El problema de los archivos pequeños

# COMMAND ----------

# El problema de los archivos pequeños en SAP
print("""
Escenario típico en datos SAP:
  - Pipeline que carga documentos contables cada hora
  - Cada carga escribe ~5 archivos Parquet pequeños (50-100 KB)
  - Después de 1 mes: 3,600 archivos pequeños
  - Resultado: queries 10x más lentas por overhead de metadata

Soluciones:
  1. OPTIMIZE periódico (cron job diario o semanal)
  2. Auto Compact (delta.autoOptimize.autoCompact = true)
  3. Liquid Clustering (manejo automático)
  4. Repartition antes de escribir (repartition(8) para tablas medianas)
""")

# Verificar número de archivos en tablas del curso
for tabla in ["bkpf_bronze","bseg_bronze","vbak_silver"]:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL sap_course.{tabla}")
        row = detail.collect()[0]
        size_mb = round(row["sizeInBytes"] / 1024 / 1024, 2)
        print(f"  {tabla:<25}: {row['numFiles']:>4} archivos | {size_mb:>8.2f} MB")
    except:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.10 Tablas del sistema — Auditoría de costos reales
# MAGIC
# MAGIC Las tablas `system.*` permiten analizar el consumo real de DBUs por cluster,
# MAGIC usuario, job o notebook. Disponibles en workspaces con Unity Catalog.

# COMMAND ----------

# NOTA: system.billing.usage requiere workspace con Unity Catalog habilitado
# y permisos de administrador. En el trial de 14 días está disponible.

# En Free Edition, usar esta alternativa para estimar costos:
print("""
En el Trial de 14 días, ejecuta estas queries en el SQL Editor:

-- Uso por cluster (últimos 7 días)
SELECT
    cluster_id,
    cluster_name,
    SUM(dbus_per_hour * usage_quantity)  AS total_dbus,
    SUM(list_price * usage_quantity)     AS costo_estimado_usd
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - 7
GROUP BY cluster_id, cluster_name
ORDER BY total_dbus DESC;

-- Uso por tipo de trabajo
SELECT
    billing_origin_product,
    SUM(usage_quantity)                  AS total_dbus,
    ROUND(SUM(list_price * usage_quantity), 4) AS costo_usd
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - 30
GROUP BY billing_origin_product
ORDER BY total_dbus DESC;

-- Alerta: clusters con alto consumo
SELECT cluster_name, SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_date = CURRENT_DATE - 1
GROUP BY cluster_name
HAVING dbus > 10
ORDER BY dbus DESC;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.11 Lab — Analizar y optimizar un pipeline SAP

# COMMAND ----------

import time

# Pipeline sin optimizar
print("=== Pipeline SIN optimizar ===")
t0 = time.time()

resultado_sin_opt = spark.sql("""
    SELECT
        b.BUKRS, b.GJAHR,
        COUNT(*) AS docs,
        SUM(s.DMBTR) AS total
    FROM sap_course.bkpf_bronze b
    JOIN sap_course.bseg_bronze s
        ON b.BELNR = s.BELNR AND b.BUKRS = s.BUKRS AND b.GJAHR = s.GJAHR
    WHERE b.BLART = 'RE'
    GROUP BY b.BUKRS, b.GJAHR
""").collect()

t1 = time.time()
print(f"  Tiempo: {t1-t0:.2f}s | Resultados: {len(resultado_sin_opt)} filas")

# COMMAND ----------

# Aplicar optimizaciones
print("Aplicando optimizaciones...")

# 1. Cachear la tabla de hechos (más grande)
spark.sql("CACHE TABLE sap_course.bseg_bronze")

# 2. Broadcast hint para tabla pequeña de cabeceras
print("=== Pipeline CON optimizaciones ===")
t0 = time.time()

resultado_con_opt = spark.sql("""
    SELECT /*+ BROADCAST(b) */
        b.BUKRS, b.GJAHR,
        COUNT(*) AS docs,
        ROUND(SUM(s.DMBTR),2) AS total
    FROM sap_course.bkpf_bronze b
    JOIN sap_course.bseg_bronze s
        ON b.BELNR = s.BELNR AND b.BUKRS = s.BUKRS AND b.GJAHR = s.GJAHR
    WHERE b.BUKRS  = '1000'
      AND b.GJAHR  = '2023'
      AND b.BLART  = 'RE'
    GROUP BY b.BUKRS, b.GJAHR
""").collect()

t1 = time.time()
print(f"  Tiempo: {t1-t0:.2f}s | Resultados: {len(resultado_con_opt)} filas")

spark.sql("UNCACHE TABLE sap_course.bseg_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del Módulo 4
# MAGIC
# MAGIC OK DBUs: jobs cluster 5x mas barato que all-purpose para produccion  
# MAGIC OK Tags: visibilidad de costos por proyecto, equipo y modulo SAP  
# MAGIC OK OPTIMIZE + Z-Order: queries hasta 10x mas rapidas en tablas SAP  
# MAGIC OK Liquid Clustering: evolucion automatica e incremental del Z-Order  
# MAGIC OK Photon: reemplazar UDFs Python por SQL nativo para maxima aceleracion  
# MAGIC OK Cache: Disk Cache automatico vs DataFrame Cache manual y sus casos de uso  
# MAGIC OK Particionamiento: GJAHR y BUKRS son buenas columnas, BELNR y fechas NO  
# MAGIC OK system.billing.usage: auditoria de costos reales por cluster y job  
# MAGIC
# MAGIC Proximo modulo: Ingesta y Arquitectura Medallion con datasets SAP completos
