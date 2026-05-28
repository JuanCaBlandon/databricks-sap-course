# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 7: SAP + Databricks — Integración Avanzada
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC - Entender el ecosistema SAP: S/4HANA, SAC, Datasphere, BDC, Joule
# MAGIC - Configurar el conector SAP BDC ↔ Databricks via Delta Sharing
# MAGIC - Implementar el flujo SAP → Databricks → SAC end-to-end
# MAGIC - Construir casos de uso reales: forecasting, customer analytics, inventory
# MAGIC
# MAGIC ---
# MAGIC ## 7.1 El ecosistema SAP hoy
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                  SAP BUSINESS DATA CLOUD (BDC)                  │
# MAGIC │                                                                  │
# MAGIC │  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐   │
# MAGIC │  │ S/4HANA     │  │ SAP         │  │ SAP Analytics Cloud  │   │
# MAGIC │  │ (fuente)    │  │ Datasphere  │  │ (SAC) - consumo/BI   │   │
# MAGIC │  │             │  │ (modelado)  │  │                      │   │
# MAGIC │  └──────┬──────┘  └──────┬──────┘  └──────────────────────┘   │
# MAGIC │         │                │                    ▲                  │
# MAGIC │         └────────────────┘                    │                  │
# MAGIC │                  │                    Delta Sharing              │
# MAGIC │                  ▼                            │                  │
# MAGIC │         SAP Data Products                     │                  │
# MAGIC │         (curated, governed)   ◄──────────────►│                  │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC                        │ BDC Connect
# MAGIC                        ▼ Delta Sharing (zero-copy)
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │           DATABRICKS DATA INTELLIGENCE PLATFORM                  │
# MAGIC │                                                                  │
# MAGIC │  Unity Catalog → Delta Sharing → ML/AI → SQL Analytics          │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC | Componente SAP | Rol | Analogía Databricks |
# MAGIC |---|---|---|
# MAGIC | S/4HANA | Sistema transaccional fuente | Base de datos operacional |
# MAGIC | SAP Datasphere | Modelado y data products | Delta Lake + Unity Catalog |
# MAGIC | SAP Analytics Cloud (SAC) | Visualización y BI | Databricks SQL Dashboards |
# MAGIC | SAP BDC | Plataforma unificada | Databricks Platform |
# MAGIC | SAP Joule | IA generativa nativa | Databricks Genie / AI |
# MAGIC | BDC Connect | Conector con Databricks externo | Delta Sharing connector |

# COMMAND ----------

spark.sql("USE sap_course")
print("Módulo 7: SAP + Databricks Integration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Por qué las empresas combinan SAP + Databricks
# MAGIC
# MAGIC ### El problema que resuelven juntos
# MAGIC
# MAGIC **Sin la integración:**
# MAGIC - Datos SAP atrapados en el ERP — solo accesibles via reportes SAP
# MAGIC - Análisis avanzado requiere exportar datos manualmente
# MAGIC - Machine Learning imposible a escala sobre datos SAP
# MAGIC - Sin posibilidad de combinar datos SAP con otras fuentes (IoT, CRM, social)
# MAGIC
# MAGIC **Con SAP + Databricks via Delta Sharing:**
# MAGIC - Datos SAP accesibles en tiempo real sin moverlos (zero-copy)
# MAGIC - ML/AI sobre millones de transacciones SAP
# MAGIC - Datos SAP + cualquier otra fuente en Unity Catalog
# MAGIC - Resultados de Databricks de vuelta a SAC/Joule

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Patrones de integración: Batch vs Near-Real-Time

# COMMAND ----------

# Patrón 1: BATCH — Carga completa periódica (diaria/semanal)
# Caso de uso: reportes de cierre contable, conciliaciones mensuales
print("=== PATRÓN BATCH: Carga completa de documentos contables ===")

df_batch = spark.sql("""
    SELECT 
        BUKRS, GJAHR,
        COUNT(*) as documentos,
        ROUND(SUM(DMBTR),2) as monto_total
    FROM sap_course.bseg_bronze
    GROUP BY BUKRS, GJAHR
    ORDER BY GJAHR DESC, monto_total DESC
""")
df_batch.show()

# COMMAND ----------

# Patrón 2: NEAR-REAL-TIME con CDF — Solo los cambios desde la última ejecución
# Caso de uso: actualización de inventario, nuevas órdenes de venta
print("=== PATRÓN CDC: Cambios en órdenes de venta ===")

# Simular nuevas órdenes que llegaron desde SAP
from pyspark.sql.functions import current_timestamp, lit
import random

nuevas_ordenes = spark.sql("""
    SELECT * FROM sap_course.vbak_silver 
    WHERE AUART = 'ZOR' 
    LIMIT 50
""").withColumn("ERDAT", lit("20241215"))

(nuevas_ordenes.write
    .format("delta")
    .mode("append")
    .saveAsTable("sap_course.vbak_silver"))

print(f"50 nuevas ordenes ZOR insertadas")

# Leer solo los cambios con CDF
cambios = (spark.read
           .format("delta")
           .option("readChangeFeed", "true")
           .option("startingVersion", 2)
           .table("sap_course.vbak_silver")
           .filter("_change_type = 'insert'"))

print(f"Nuevas inserciones detectadas via CDF: {cambios.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 Delta Sharing → SAP Analytics Cloud (SAC)
# MAGIC
# MAGIC ### Configuración del conector nativo en SAC
# MAGIC
# MAGIC **Pasos (requieren trial activo de SAP BDC + Databricks):**
# MAGIC
# MAGIC 1. En Databricks → Data → Delta Sharing → crear Share con tablas Gold
# MAGIC 2. Agregar SAC como recipient con credenciales
# MAGIC 3. En SAC → Connections → New Connection → Delta Sharing
# MAGIC 4. Ingresar la URL del share y el token de autenticación
# MAGIC 5. Las tablas aparecen como modelos de datos en SAC sin replicar datos
# MAGIC
# MAGIC **El siguiente código prepara las tablas para ser compartidas:**

# COMMAND ----------

# Preparar la tabla Gold de ventas para compartir con SAC
# En producción: este share se configura en Unity Catalog
print("=== Preparando tablas Gold para Delta Sharing a SAC ===")

# Verificar que las tablas Gold existen y tienen datos
gold_tables = ["gold_fin_summary", "gold_sales_kpis", "gold_customer_360"]

for tabla in gold_tables:
    try:
        n = spark.table(f"sap_course.{tabla}").count()
        print(f"  ✅ {tabla}: {n:,} registros — lista para sharing")
    except Exception as e:
        print(f"  ❌ {tabla}: no encontrada — ejecutar Módulo 5 primero")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comandos SQL para configurar Delta Sharing desde Unity Catalog
# MAGIC
# MAGIC ```sql
# MAGIC -- Crear el share para SAP Analytics Cloud
# MAGIC CREATE SHARE sap_analytics_share
# MAGIC   COMMENT 'Share de tablas Gold SAP para SAC y Datasphere';
# MAGIC
# MAGIC -- Agregar tablas al share
# MAGIC ALTER SHARE sap_analytics_share
# MAGIC   ADD TABLE sap_course.gold_fin_summary;
# MAGIC
# MAGIC ALTER SHARE sap_analytics_share
# MAGIC   ADD TABLE sap_course.gold_sales_kpis;
# MAGIC
# MAGIC ALTER SHARE sap_analytics_share
# MAGIC   ADD TABLE sap_course.gold_customer_360;
# MAGIC
# MAGIC -- Crear recipient para SAC (Open Sharing — sin cuenta Databricks)
# MAGIC CREATE RECIPIENT sap_analytics_cloud_recipient
# MAGIC   COMMENT 'SAP Analytics Cloud tenant';
# MAGIC
# MAGIC -- Obtener el token de activación para SAC
# MAGIC DESCRIBE RECIPIENT sap_analytics_cloud_recipient;
# MAGIC
# MAGIC -- Otorgar acceso
# MAGIC GRANT SELECT ON SHARE sap_analytics_share
# MAGIC   TO RECIPIENT sap_analytics_cloud_recipient;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.5 Caso de uso: Forecasting de ventas SAP con ML en Databricks

# COMMAND ----------

from pyspark.sql.functions import (
    col, year, month, sum as _sum, count, round as _round,
    lag, avg
)
from pyspark.sql.window import Window

# Preparar serie de tiempo de ventas para forecasting
print("=== Preparando datos para forecasting de ventas ===")

ventas_mensuales = spark.sql("""
    SELECT
        YEAR                            AS anio,
        MONTH                           AS mes,
        VKORG                           AS org_ventas,
        ROUND(SUM(NETWR), 2)            AS ventas_total,
        COUNT(DISTINCT VBELN)           AS num_ordenes,
        ROUND(AVG(NETWR), 2)            AS ticket_promedio
    FROM sap_course.vbak_silver
    WHERE _is_valid = TRUE
    GROUP BY YEAR, MONTH, VKORG
    ORDER BY YEAR, MONTH, VKORG
""")

print("Serie de tiempo mensual de ventas SAP:")
ventas_mensuales.show(20)

# COMMAND ----------

# Feature engineering para ML: agregar lag features
window_spec = Window.partitionBy("org_ventas").orderBy("anio", "mes")

df_features = (ventas_mensuales
    .withColumn("ventas_mes_anterior",  lag("ventas_total", 1).over(window_spec))
    .withColumn("ventas_2m_anterior",   lag("ventas_total", 2).over(window_spec))
    .withColumn("ventas_3m_anterior",   lag("ventas_total", 3).over(window_spec))
    .withColumn("variacion_pct",
        _round(
            (col("ventas_total") - col("ventas_mes_anterior")) /
            col("ventas_mes_anterior") * 100, 2
        ))
    .dropna())

print("Features de forecasting (con lags):")
df_features.show(10)

(df_features.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("sap_course.gold_forecast_features"))

print("Tabla gold_forecast_features lista para modelo ML")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.6 Caso de uso: Customer Analytics — Segmentación RFM
# MAGIC
# MAGIC RFM (Recency, Frequency, Monetary) es un modelo de segmentación de clientes
# MAGIC muy usado en empresas SAP para marketing y gestión de cuentas.

# COMMAND ----------

from pyspark.sql.functions import datediff, to_date, lit, ntile
from pyspark.sql.window import Window

# Calcular métricas RFM sobre datos SAP
rfm = spark.sql("""
    SELECT
        k.KUNNR                                        AS customer_id,
        k.NAME1                                        AS customer_name,
        k.LAND1                                        AS country,
        DATEDIFF(DATE('2024-12-31'), MAX(v.ERDAT_DT))  AS recency_days,
        COUNT(DISTINCT v.VBELN)                        AS frequency,
        ROUND(SUM(v.NETWR), 2)                         AS monetary
    FROM sap_course.kna1_silver k
    JOIN sap_course.vbak_silver v ON k.KUNNR = v.KUNNR
    WHERE v._is_valid = TRUE
      AND v.ERDAT_DT IS NOT NULL
    GROUP BY k.KUNNR, k.NAME1, k.LAND1
    HAVING COUNT(DISTINCT v.VBELN) > 0
""")

# Scoring RFM (quintiles 1-5, mayor = mejor)
w = Window.orderBy(col("recency_days").asc())
w2 = Window.orderBy(col("frequency").desc())
w3 = Window.orderBy(col("monetary").desc())

rfm_scored = (rfm
    .withColumn("R_score", ntile(5).over(w))
    .withColumn("F_score", ntile(5).over(w2))
    .withColumn("M_score", ntile(5).over(w3))
    .withColumn("RFM_score",
        col("R_score") + col("F_score") + col("M_score"))
    .withColumn("segment",
        when(col("RFM_score") >= 13, "Champions")
        .when(col("RFM_score") >= 10, "Loyal Customers")
        .when(col("RFM_score") >= 7,  "Potential Loyalists")
        .when(col("RFM_score") >= 4,  "At Risk")
        .otherwise("Lost")))

(rfm_scored.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("sap_course.gold_rfm_segmentation"))

print("=== Segmentación RFM de clientes SAP ===")
spark.sql("""
    SELECT segment, COUNT(*) as clientes, 
           ROUND(AVG(monetary),2) as avg_revenue,
           ROUND(SUM(monetary),2) as total_revenue
    FROM sap_course.gold_rfm_segmentation
    GROUP BY segment ORDER BY total_revenue DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.7 Flujo inverso: Databricks → SAP
# MAGIC
# MAGIC Los resultados de ML en Databricks pueden volver a SAP vía Delta Sharing:
# MAGIC - Segmentación de clientes (RFM) → SAC para reportes de marketing
# MAGIC - Predicciones de demanda → S/4HANA para planificación MRP
# MAGIC - Scores de riesgo de crédito → SAP para gestión de límites
# MAGIC
# MAGIC ```sql
# MAGIC -- Compartir resultados de ML de vuelta a SAP BDC
# MAGIC CREATE SHARE databricks_to_sap_share
# MAGIC   COMMENT 'Resultados ML Databricks para SAP';
# MAGIC
# MAGIC ALTER SHARE databricks_to_sap_share
# MAGIC   ADD TABLE sap_course.gold_rfm_segmentation;
# MAGIC
# MAGIC ALTER SHARE databricks_to_sap_share
# MAGIC   ADD TABLE sap_course.gold_forecast_features;
# MAGIC
# MAGIC -- El recipient SAP BDC fue creado automáticamente en el paso de conexión BDC Connect
# MAGIC GRANT SELECT ON SHARE databricks_to_sap_share
# MAGIC   TO RECIPIENT sap_bdc_recipient;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.8 Lab Final: Pipeline end-to-end SAP → Databricks → insights

# COMMAND ----------

# Resumen ejecutivo: todas las tablas Gold del curso listas para SAC
print("=" * 65)
print("  TABLAS GOLD LISTAS PARA SAP ANALYTICS CLOUD via DELTA SHARING")
print("=" * 65)

gold_summary = {
    "gold_fin_summary":       "Resumen financiero por compañía y año fiscal",
    "gold_sales_kpis":        "KPIs de ventas mensuales por organización",
    "gold_customer_360":      "Vista 360° del cliente con tier y LTV",
    "gold_rfm_segmentation":  "Segmentación RFM para marketing",
    "gold_forecast_features": "Features de forecasting para modelos ML",
}

for tabla, desc in gold_summary.items():
    try:
        n = spark.table(f"sap_course.{tabla}").count()
        print(f"\n  📊 {tabla}")
        print(f"     {desc}")
        print(f"     {n:,} registros | Compartible via Delta Sharing")
    except:
        print(f"\n  ❌ {tabla} — pendiente de crear")

print("\n" + "=" * 65)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del Módulo 7
# MAGIC
# MAGIC ✅ SAP BDC conecta S/4HANA, Datasphere, SAC y Databricks en una sola plataforma  
# MAGIC ✅ BDC Connect usa Delta Sharing para acceso zero-copy y bidireccional  
# MAGIC ✅ Forecasting de ventas SAP con features de serie de tiempo  
# MAGIC ✅ Segmentación RFM de clientes SAP lista para SAC  
# MAGIC ✅ Todas las tablas Gold compartibles con SAC sin replicar datos  
# MAGIC ✅ Flujo inverso: resultados ML de Databricks de vuelta a SAP  
# MAGIC
# MAGIC **Próximo módulo**: Arquitectura de referencia y hoja de ruta para tu organización
