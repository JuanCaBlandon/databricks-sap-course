# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 1: El Mundo Databricks — Contexto y Evolución
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC Al finalizar este módulo podrás:
# MAGIC - Entender por qué Databricks existe y qué problema resuelve
# MAGIC - Diferenciar Data Warehouse, Data Lake y Lakehouse
# MAGIC - Identificar cómo Databricks se integra con SAP, Salesforce y Microsoft
# MAGIC - Navegar el workspace: Unity Catalog, Catalog Explorer, SQL Warehouse
# MAGIC
# MAGIC ---
# MAGIC ## 1.1 ¿Por qué existe Databricks?
# MAGIC
# MAGIC El problema que resuelve Databricks puede resumirse en una frase:
# MAGIC > *"Los datos empresariales están atrapados en silos. SAP tiene los datos de negocio más críticos del mundo. Databricks tiene la mejor plataforma de IA y analytics. Juntos, eliminan los silos."*
# MAGIC
# MAGIC ### Evolución: Data Warehouse → Data Lake → Lakehouse
# MAGIC
# MAGIC | Arquitectura | Ventajas | Limitaciones |
# MAGIC |---|---|---|
# MAGIC | **Data Warehouse** | SQL performante, estructura clara | Caro, no soporta ML, datos no estructurados difíciles |
# MAGIC | **Data Lake** | Barato, flexible, soporta cualquier formato | Sin transacciones ACID, sin governance, "data swamp" |
# MAGIC | **Lakehouse** | Lo mejor de ambos: ACID + flexibilidad + IA | Requiere diseño cuidadoso (lo aprenderemos en este curso) |
# MAGIC
# MAGIC ---
# MAGIC ## 1.2 El ecosistema Databricks hoy

# COMMAND ----------

# MAGIC %md
# MAGIC ### Componentes clave que veremos en este curso
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                  DATABRICKS PLATFORM                        │
# MAGIC │                                                             │
# MAGIC │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐ │
# MAGIC │  │  Delta Lake  │  │  Databricks  │  │  Unity Catalog   │ │
# MAGIC │  │  (M2)        │  │  SQL (M3)    │  │  (M6)            │ │
# MAGIC │  └──────────────┘  └──────────────┘  └──────────────────┘ │
# MAGIC │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐ │
# MAGIC │  │  Lakeflow /  │  │  Photon /    │  │  Delta Sharing   │ │
# MAGIC │  │  Auto Loader │  │  Optimizac.  │  │  (M6)            │ │
# MAGIC │  │  (M5)        │  │  (M4)        │  │                  │ │
# MAGIC │  └──────────────┘  └──────────────┘  └──────────────────┘ │
# MAGIC │                                                             │
# MAGIC │         SAP BDC CONNECTOR via Delta Sharing (M7)           │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Lab: Exploración del workspace
# MAGIC
# MAGIC ### Instrucciones
# MAGIC Antes de ejecutar código, explora manualmente:
# MAGIC 1. **Catalog** (ícono izquierdo) → Unity Catalog → `main` catalog
# MAGIC 2. **SQL Editor** → verifica que tienes un Warehouse disponible
# MAGIC 3. **Compute** → confirma que el cluster serverless está activo
# MAGIC
# MAGIC Cuando estés listo, ejecuta las celdas siguientes:

# COMMAND ----------

# Verificar la versión de Spark y el entorno
print(f"Spark version : {spark.version}")
print(f"Databricks Runtime: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion', 'N/A')}")
print(f"Default catalog  : {spark.catalog.currentCatalog()}")
print(f"Default database : {spark.catalog.currentDatabase()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Cargar los datasets SAP de muestra
# MAGIC
# MAGIC A lo largo del curso usaremos tablas que simulan datos reales de SAP.
# MAGIC Estas son las tablas que usaremos y a qué módulo SAP pertenecen:
# MAGIC
# MAGIC | Tabla | Módulo SAP | Descripción |
# MAGIC |---|---|---|
# MAGIC | `BKPF` | FI (Finanzas) | Cabeceras de documentos contables |
# MAGIC | `BSEG` | FI (Finanzas) | Posiciones de documentos contables |
# MAGIC | `KNA1` | SD (Ventas) | Datos maestros de clientes |
# MAGIC | `MARA` | MM (Materiales) | Datos maestros de materiales |
# MAGIC | `VBAK` | SD (Ventas) | Cabeceras de órdenes de venta |
# MAGIC | `VBAP` | SD (Ventas) | Posiciones de órdenes de venta |
# MAGIC | `LFA1` | MM (Compras) | Datos maestros de proveedores |
# MAGIC | `EKKO` | MM (Compras) | Cabeceras de órdenes de compra |

# COMMAND ----------

# Subir los archivos CSV a DBFS antes de ejecutar esto
# Desde la UI: File > Upload Data > sube los CSV desde datasets/
# Luego ajusta la ruta si es necesario

SAP_DATA_PATH = "/FileStore/sap_course/datasets"

# Leer BKPF como primer vistazo
df_bkpf = (spark.read
           .option("header", "true")
           .option("inferSchema", "true")
           .csv(f"{SAP_DATA_PATH}/BKPF.csv"))

print(f"BKPF - registros: {df_bkpf.count():,}")
print(f"BKPF - columnas : {df_bkpf.columns}")
df_bkpf.show(5, truncate=False)

# COMMAND ----------

# Verificar todas las tablas SAP cargadas
sap_tables = ["BKPF", "BSEG", "KNA1", "MARA", "VBAK", "VBAP", "LFA1", "EKKO"]

print("=" * 60)
print(f"{'Tabla':<8} | {'Registros':>12} | {'Columnas':>8}")
print("=" * 60)
for tabla in sap_tables:
    try:
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(f"{SAP_DATA_PATH}/{tabla}.csv"))
        print(f"{tabla:<8} | {df.count():>12,} | {len(df.columns):>8}")
    except Exception as e:
        print(f"{tabla:<8} | {'ERROR':>12} | {str(e)[:30]}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Primera consulta: ¿cuánto vendió cada compañía?
# MAGIC
# MAGIC Usamos VBAK (órdenes de venta) para una primera exploración de negocio.

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, count, round as _round

df_vbak = (spark.read
           .option("header", "true")
           .option("inferSchema", "true")
           .csv(f"{SAP_DATA_PATH}/VBAK.csv"))

# Ventas totales por organización de ventas
ventas_por_vkorg = (df_vbak
    .groupBy("VKORG", "WAERK")
    .agg(
        _round(_sum("NETWR"), 2).alias("TOTAL_VENTAS"),
        count("VBELN").alias("NUM_ORDENES")
    )
    .orderBy("TOTAL_VENTAS", ascending=False))

print("Ventas por organización de ventas (VKORG):")
ventas_por_vkorg.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Diferencias Azure Databricks vs otras nubes
# MAGIC
# MAGIC | Aspecto | Azure Databricks | AWS Databricks | GCP Databricks |
# MAGIC |---|---|---|---|
# MAGIC | Integración nativa | Azure AD, ADLS, ADF, Synapse | S3, IAM, Glue, Redshift | GCS, BigQuery, Vertex AI |
# MAGIC | SAP en la nube | RISE with SAP en Azure es el más común | También soportado | Soportado desde julio 2025 |
# MAGIC | Delta Sharing cross-cloud | ✅ Soportado | ✅ Soportado | ✅ Soportado (julio 2025) |
# MAGIC | Unity Catalog | ✅ Igual en todas las nubes | ✅ | ✅ |
# MAGIC
# MAGIC > **Clave**: Unity Catalog y Delta Sharing funcionan **exactamente igual** en AWS, Azure y GCP.
# MAGIC > La nube es un detalle de infraestructura, no cambia el desarrollo ni la arquitectura.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del Módulo 1
# MAGIC
# MAGIC ✅ Databricks nació para resolver el dilema Data Warehouse vs Data Lake con el Lakehouse  
# MAGIC ✅ El ecosistema tiene 4 pilares: Delta Lake, Databricks SQL, Unity Catalog, Delta Sharing  
# MAGIC ✅ SAP tiene los datos más críticos del mundo — Databricks es la plataforma para explotarlos  
# MAGIC ✅ Los 8 datasets SAP de muestra están cargados y listos para los próximos módulos  
# MAGIC
# MAGIC **Próximo módulo**: Delta Lake — el motor de confiabilidad que hace posible el Lakehouse
