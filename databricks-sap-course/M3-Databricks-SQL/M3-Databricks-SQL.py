# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 3: Databricks SQL — La Capa Analítica Enterprise
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC - Entender cuándo usar Databricks SQL vs Spark SQL vs SQL tradicional
# MAGIC - Configurar y optimizar SQL Warehouses
# MAGIC - Dominar CTEs, Window Functions y parámetros avanzados
# MAGIC - Usar Query Profile para diagnosticar consultas lentas
# MAGIC - Construir dashboards ejecutivos con datos SAP
# MAGIC - Explorar Genie Space (AI/BI): consultas en lenguaje natural
# MAGIC
# MAGIC ---
# MAGIC ## 3.1 Databricks SQL vs Spark SQL vs SQL tradicional
# MAGIC
# MAGIC | Aspecto | SQL Tradicional | Spark SQL | Databricks SQL |
# MAGIC |---|---|---|---|
# MAGIC | Motor | RDBMS (Postgres, Oracle) | Apache Spark | Photon Engine |
# MAGIC | Escala | GB | TB–PB | TB–PB |
# MAGIC | ACID | Nativo | Via Delta | Via Delta |
# MAGIC | BI/Dashboards | Conectores JDBC | Limitado | Nativo |
# MAGIC | AI/ML | No | Via PySpark | + Genie |
# MAGIC | Cuándo usarlo | OLTP | ETL, ML | Analytics, BI |

# COMMAND ----------

spark.sql("USE laboratory_sap_dev.sap_course")
print("Módulo 3: Databricks SQL")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 SQL Warehouses — Tipos y configuración
# MAGIC
# MAGIC | Tipo | Cuándo usar | Arranque |
# MAGIC |---|---|---|
# MAGIC | **Serverless** | Queries ad-hoc, Genie | Instantáneo |
# MAGIC | **Pro** | Dashboards producción | ~2 min |
# MAGIC | **Classic** | Workloads muy grandes | ~5 min |
# MAGIC
# MAGIC **Regla de oro**: configura auto-stop en 10 minutos para el curso.
# MAGIC Un warehouse olvidado encendido consume todo el presupuesto.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 CTEs — Common Table Expressions sobre datos SAP

# COMMAND ----------

# CTE: documentos contables con detalle de líneas
resultado = spark.sql("""
    WITH documentos AS (
        SELECT BUKRS, BELNR, GJAHR, WAERK, USNAM
        FROM sap_course.bkpf_bronze
        WHERE GJAHR IN ('2022','2023','2024')
    ),
    lineas AS (
        SELECT
            BUKRS, BELNR, GJAHR,
            COUNT(BUZEI)        AS num_lineas,
            SUM(DMBTR)          AS monto_total,
            AVG(DMBTR)          AS monto_promedio
        FROM sap_course.bseg_bronze
        GROUP BY BUKRS, BELNR, GJAHR
    ),
    resultado_final AS (
        SELECT
            d.BUKRS                         AS sociedad,
            d.GJAHR                         AS ejercicio,
            d.BELNR                         AS documento,
            d.WAERK                         AS moneda,
            d.USNAM                         AS usuario,
            l.num_lineas,
            ROUND(l.monto_total, 2)         AS monto_total,
            ROUND(l.monto_promedio, 2)      AS monto_promedio
        FROM documentos d
        JOIN lineas l
            ON  d.BELNR = l.BELNR
            AND d.BUKRS = l.BUKRS
            AND d.GJAHR = l.GJAHR
    )
    SELECT * FROM resultado_final
    ORDER BY monto_total DESC
    LIMIT 20
""")
print("Top 20 documentos contables por monto:")
resultado.show(truncate=False)

# COMMAND ----------

# CTE avanzada: clasificación ABC de clientes SAP
pipeline_ventas = spark.sql("""
    WITH base_ventas AS (
        SELECT
            v.VBELN, v.KUNNR, v.VKORG, v.AUART, v.NETWR,
            YEAR(v.ERDAT)    AS anio,
            MONTH(v.ERDAT)   AS mes
        FROM sap_course.vbak_silver v
        WHERE  v.ERDAT IS NOT NULL
    ),
    metricas_cliente AS (
        SELECT
            KUNNR,
            COUNT(DISTINCT VBELN)       AS total_ordenes,
            ROUND(SUM(NETWR), 2)        AS revenue_total,
            ROUND(AVG(NETWR), 2)        AS ticket_promedio,
            MIN(anio)                   AS primer_anio,
            MAX(anio)                   AS ultimo_anio
        FROM base_ventas
        GROUP BY KUNNR
    ),
    clasificacion AS (
        SELECT
            mc.*,
            k.NAME1                     AS nombre_cliente,
            k.LAND1                     AS pais,
            k.ORT01                     AS ciudad,
            CASE
                WHEN mc.revenue_total > 1000000 THEN 'A - Strategic'
                WHEN mc.revenue_total > 500000  THEN 'B - Key Account'
                WHEN mc.revenue_total > 100000  THEN 'C - Standard'
                ELSE 'D - Small'
            END                         AS clasificacion_abc
        FROM metricas_cliente mc
        LEFT JOIN sap_course.kna1_bronze k ON mc.KUNNR = k.KUNNR
    )
    SELECT * FROM clasificacion
    ORDER BY revenue_total DESC
    LIMIT 20
""")
print("Clasificación ABC de clientes SAP:")
pipeline_ventas.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Window Functions — Análisis temporal SAP

# COMMAND ----------

# Tendencias de ventas con LAG, YTD y media móvil
analisis_temporal = spark.sql("""
    WITH ventas_mensuales AS (
        SELECT
            YEAR(ERDAT)              AS anio,
            MONTH(ERDAT)             AS mes,
            VKORG                       AS org_ventas,
            ROUND(SUM(NETWR), 2)        AS ventas_mes,
            COUNT(DISTINCT VBELN)       AS num_ordenes
        FROM sap_course.vbak_silver
        WHERE ERDAT IS NOT NULL
        GROUP BY YEAR(ERDAT), MONTH(ERDAT), VKORG
    )
    SELECT
        anio, mes, org_ventas, ventas_mes, num_ordenes,
        -- Mes anterior
        LAG(ventas_mes, 1) OVER (
            PARTITION BY org_ventas ORDER BY anio, mes
        )                                               AS ventas_mes_anterior,
        -- Variación % mes a mes
        ROUND(
            (ventas_mes - LAG(ventas_mes,1) OVER (
                PARTITION BY org_ventas ORDER BY anio, mes)
            ) / NULLIF(LAG(ventas_mes,1) OVER (
                PARTITION BY org_ventas ORDER BY anio, mes), 0) * 100, 2
        )                                               AS variacion_pct,
        -- Acumulado del año (YTD)
        ROUND(SUM(ventas_mes) OVER (
            PARTITION BY org_ventas, anio ORDER BY mes
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2)                                           AS ventas_ytd
    FROM ventas_mensuales
    ORDER BY org_ventas, anio, mes
""")
print("Análisis temporal con Window Functions:")
analisis_temporal.show(20)

# COMMAND ----------

# Análisis Pareto de materiales — top N por categoría
top_materiales = spark.sql("""
    WITH ventas_material AS (
        SELECT
            p.MATNR,
            m.MATKL                     AS categoria,
            m.MTART                     AS tipo_material,
            ROUND(SUM(p.NETWR), 2)      AS revenue_total,
            ROUND(SUM(p.KWMENG), 2)     AS unidades_vendidas
        FROM sap_course.vbap_bronze p
        JOIN sap_course.mara_bronze m ON p.MATNR = m.MATNR
        GROUP BY p.MATNR, m.MATKL, m.MTART
    )
    SELECT
        *,
        RANK() OVER (
            PARTITION BY categoria ORDER BY revenue_total DESC
        )                                           AS rank_categoria,
        ROUND(
            revenue_total / SUM(revenue_total) OVER (
                PARTITION BY categoria) * 100, 2
        )                                           AS pct_categoria,
        -- Curva de Pareto acumulada
        ROUND(
            SUM(revenue_total) OVER (
                PARTITION BY categoria
                ORDER BY revenue_total DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / SUM(revenue_total) OVER (PARTITION BY categoria) * 100, 2
        )                                           AS pct_acumulado_pareto
    FROM ventas_material
    QUALIFY RANK() OVER (
        PARTITION BY categoria ORDER BY revenue_total DESC) <= 5
    ORDER BY categoria, rank_categoria
""")
print("Top 5 materiales por categoría con análisis Pareto:")
top_materiales.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Parámetros — Dashboards interactivos por sociedad SAP

# COMMAND ----------

dbutils.widgets.dropdown("sociedad",  "1000", ["1000","2000","3000"], "Sociedad SAP")
dbutils.widgets.dropdown("ejercicio", "2023", ["2021","2022","2023","2024"], "Ejercicio")
dbutils.widgets.text("moneda", "COP", "Moneda")

# COMMAND ----------

sociedad  = dbutils.widgets.get("sociedad")
ejercicio = dbutils.widgets.get("ejercicio")
moneda    = dbutils.widgets.get("moneda")

print(f"Filtros activos: Sociedad={sociedad} | Ejercicio={ejercicio} | Moneda={moneda}")

spark.sql(f"""
    SELECT
        b.BLART                         AS tipo_doc,
        COUNT(DISTINCT b.BELNR)         AS num_documentos,
        ROUND(SUM(s.DMBTR), 2)         AS monto_total,
        ROUND(AVG(s.DMBTR), 2)         AS monto_promedio
    FROM sap_course.bkpf_bronze b
    JOIN sap_course.bseg_bronze s
        ON  b.BELNR = s.BELNR
        AND b.BUKRS = s.BUKRS
        AND b.GJAHR = s.GJAHR
    WHERE b.BUKRS = '{sociedad}'
      AND b.GJAHR = '{ejercicio}'
      AND b.WAERK = '{moneda}'
    GROUP BY b.BLART
    ORDER BY monto_total DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 Query Profile — Diagnóstico de consultas lentas
# MAGIC
# MAGIC ### Cómo acceder:
# MAGIC 1. Ejecutar la query en el **SQL Editor** (no en el notebook)
# MAGIC 2. Click en el ícono de rendimiento en los resultados
# MAGIC 3. Ver el grafo de ejecución
# MAGIC
# MAGIC ### Qué buscar en datos SAP:
# MAGIC | Síntoma | Causa probable | Solución |
# MAGIC |---|---|---|
# MAGIC | Nodo muy ancho | Full scan en tabla grande | Z-Order por columna de filtro |
# MAGIC | Shuffle grande | JOIN sin partición común | Broadcast join o partition pruning |
# MAGIC | Spill to disk | Agregación muy grande | Reducir groupBy o aumentar cluster |
# MAGIC | PhotonNotSupported | UDF o función legacy | Reescribir en SQL nativo |

# COMMAND ----------

# Consulta sin optimizar (full scan) — ver en Query Profile el costo
print("=== Consulta sin optimizar — notar el full scan en Query Profile ===")
spark.sql("""
    SELECT COUNT(*), ROUND(SUM(DMBTR),2)
    FROM sap_course.bseg_bronze
    WHERE KONTO LIKE '4%'
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 Lab — Dashboard ejecutivo SAP (4 visualizaciones)
# MAGIC
# MAGIC Crea estas 4 queries en el **SQL Editor**, agrega cada una como
# MAGIC visualización y construye el dashboard "SAP Executive Summary".

# COMMAND ----------

# Viz 1: Revenue mensual — gráfico de líneas por org de ventas
display(spark.sql("""
    SELECT
    CONCAT(anio_creacion, '-', LPAD(mes_creacion, 2, '0')) AS periodo,
    org_ventas,
    ROUND(SUM(valor_neto), 2) AS revenue_total
FROM laboratory_sap_dev.sap_course.vbak_gold
WHERE es_devolucion = FALSE
GROUP BY periodo, org_ventas
ORDER BY periodo
"""))

# COMMAND ----------

# Viz 2: Top 10 clientes — gráfico de barras horizontal
display(spark.sql("""
    SELECT
    codigo_cliente,
    ROUND(SUM(valor_neto), 2)      AS revenue_total,
    COUNT(DISTINCT numero_orden)   AS total_ordenes
FROM laboratory_sap_dev.sap_course.vbak_gold
WHERE es_devolucion = FALSE
GROUP BY codigo_cliente
ORDER BY revenue_total DESC
LIMIT 10
"""))

# COMMAND ----------

# Viz 3: KPIs ejecutivos 2023 — tarjetas métricas
display(spark.sql("""
    SELECT
    ROUND(SUM(valor_neto), 2)           AS revenue_total,
    COUNT(DISTINCT numero_orden)        AS total_ordenes,
    COUNT(DISTINCT codigo_cliente)      AS clientes_activos,
    ROUND(AVG(valor_neto), 2)           AS ticket_promedio,
    SUM(CASE WHEN es_devolucion THEN 1 ELSE 0 END) AS devoluciones
FROM laboratory_sap_dev.sap_course.vbak_gold
"""))

# COMMAND ----------

# Viz 4: Revenue por país — gráfico de torta
display(spark.sql("""
    SELECT
    canal_distribucion,
    ROUND(SUM(valor_neto), 2)    AS revenue_total,
    COUNT(DISTINCT numero_orden) AS ordenes
FROM laboratory_sap_dev.sap_course.vbak_gold
WHERE es_devolucion = FALSE
GROUP BY canal_distribucion
ORDER BY revenue_total DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.8 Genie Space — Consultas en lenguaje natural sobre SAP
# MAGIC
# MAGIC ### Setup en la UI (SQL Editor → Genie):
# MAGIC 1. Nuevo Genie Space → agrega las tablas Gold
# MAGIC 2. Pega este contexto de negocio:
# MAGIC
# MAGIC ```
# MAGIC VBAK = órdenes de venta. VBELN = número orden. KUNNR = cliente.
# MAGIC NETWR = valor neto. WAERK = moneda. VKORG = org ventas
# MAGIC (1000=Colombia, 2000=Internacional). AUART = tipo orden
# MAGIC (ZOR=estándar, OR=urgente, RE=devolución, CR=crédito).
# MAGIC KNA1 = maestro clientes. NAME1 = nombre. LAND1 = país.
# MAGIC BKPF = documentos contables. BELNR = número documento.
# MAGIC GJAHR = ejercicio fiscal. DMBTR = monto en moneda local.
# MAGIC ```
# MAGIC
# MAGIC ### Preguntas de ejemplo con datos SAP:
# MAGIC - *"¿Cuánto vendimos en total en 2023?"*
# MAGIC - *"¿Cuál es el cliente con mayor revenue este año?"*
# MAGIC - *"Muéstrame las ventas mensuales de la organización 1000"*
# MAGIC - *"¿Cuántas órdenes de devolución hubo en 2024?"*
# MAGIC - *"¿Qué país tiene el ticket promedio más alto?"*

# COMMAND ----------

# Verificar tablas disponibles para Genie Space
tablas_genie = [
    "sap_course.vbak_silver",
    "sap_course.kna1_bronze",
    "sap_course.bkpf_bronze",
    "sap_course.gold_sales_kpis",
    "sap_course.gold_customer_360"
]

print("=== Tablas listas para Genie Space ===")
for t in tablas_genie:
    try:
        n    = spark.table(t).count()
        cols = len(spark.table(t).columns)
        print(f"  OK  {t:<45} {n:>8,} filas | {cols} cols")
    except Exception as e:
        print(f"  ERR {t} — ejecutar modulos anteriores primero")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del Módulo 3
# MAGIC
# MAGIC OK SQL Warehouse serverless para analytics, Pro para dashboards en produccion  
# MAGIC OK CTEs para consultas SAP complejas legibles y reutilizables  
# MAGIC OK Window Functions: LAG, YTD, media movil, ranking y Pareto sobre datos SAP  
# MAGIC OK Parametros: dashboards interactivos filtrados por sociedad y ejercicio  
# MAGIC OK Query Profile: diagnostico visual de consultas lentas  
# MAGIC OK 4 visualizaciones listas para dashboard ejecutivo SAP  
# MAGIC OK Genie Space configurado para preguntas en lenguaje natural  
# MAGIC
# MAGIC Proximo modulo: Optimizacion y control de costos
