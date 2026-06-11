# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 4: Optimización y Control de Costos
# MAGIC
# MAGIC ## Enfoque del módulo
# MAGIC No vamos a hacer "tuning avanzado de Spark". Vamos a aprender **tips accionables
# MAGIC con números concretos** para que sus pipelines SAP sean rápidos y baratos.
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Reconocer los 3 cuellos de botella en el Query Profile: small files, shuffle, spill
# MAGIC - Decidir entre Particionamiento, Z-Order y Liquid Clustering con reglas claras
# MAGIC - Dejar que Databricks optimice solo: Predictive Optimization + Runtime moderno
# MAGIC - Controlar costos: tipos de cluster, tagging, system.billing, precios de IA
# MAGIC
# MAGIC > **Prerequisito**: tablas Bronze/Silver/Gold de los módulos 2 y 3.

# COMMAND ----------

# ── Celda autosuficiente ──
CATALOG = "laboratory_sap_dev"
SCHEMA  = "sap_course"
spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"Usando: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4.1 ¿Por qué una query es lenta? — Los 3 cuellos de botella
# MAGIC
# MAGIC En cualquier sistema MPP (Massively Parallel Processing) como Databricks,
# MAGIC el 90% de los problemas de performance son uno de estos tres:
# MAGIC
# MAGIC | Cuello de botella | Qué es | Cómo se ve en Query Profile |
# MAGIC |---|---|---|
# MAGIC | **Small files** | Miles de archivos pequeños → overhead de metadata | "Files read: 3,600" en una tabla de 50 MB |
# MAGIC | **Shuffle** | Mover datos entre nodos para un JOIN o GROUP BY | Nodo "Exchange" grande en el grafo |
# MAGIC | **Spill** | La memoria no alcanza → escribir a disco temporal | "Spill (disk): 2.5 GB" en el detalle del nodo |
# MAGIC
# MAGIC **Regla de diagnóstico**: ejecutar la query → abrir Query Profile → buscar el nodo
# MAGIC más ancho del grafo → leer sus métricas. El nodo más ancho ES el problema.

# COMMAND ----------

# Diagnóstico práctico: ver cuántos archivos tiene cada tabla del curso
# Muchos archivos pequeños = candidata a OPTIMIZE
print(f"{'Tabla':<22} {'Archivos':>9} {'Tamaño MB':>10} {'MB/archivo':>11}")
print("─" * 56)
for t in ["bkpf_bronze", "bseg_bronze", "vbak_silver", "vbak_gold", "vbap_gold", "kna1_gold"]:
    try:
        d = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.{t}").collect()[0]
        n, mb = d["numFiles"], d["sizeInBytes"] / 1024 / 1024
        print(f"{t:<22} {n:>9} {mb:>10.1f} {mb/max(n,1):>11.2f}")
    except Exception:
        print(f"{t:<22} {'N/A':>9}")
print()
print("Tip: si MB/archivo < 10 y la tabla tiene +50 archivos → small file problem")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4.2 Data Skipping — el mecanismo que hace todo posible
# MAGIC
# MAGIC Delta Lake guarda automáticamente estadísticas **min/max de las primeras 32
# MAGIC columnas** de cada archivo en el transaction log. Cuando filtras:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM bkpf_bronze WHERE GJAHR = 2024
# MAGIC ```
# MAGIC
# MAGIC Delta lee las stats y **salta los archivos** donde 2024 no puede estar
# MAGIC (ej: un archivo con min=2020, max=2022 ni se abre).
# MAGIC
# MAGIC **Todo lo que sigue (particiones, Z-Order, Liquid) son estrategias para que
# MAGIC los datos similares queden en los mismos archivos y el skipping sea máximo.**

# COMMAND ----------

# Ver el data skipping en acción: misma query, con y sin filtro de partición
import time

q1 = f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bkpf_bronze"
q2 = f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.bkpf_bronze WHERE GJAHR = 2024"

t0 = time.time(); spark.sql(q1).collect(); t1 = time.time() - t0
t0 = time.time(); spark.sql(q2).collect(); t2 = time.time() - t0

print(f"Sin filtro (lee todo)      : {t1:.2f}s")
print(f"Con filtro GJAHR (skipping): {t2:.2f}s")
print()
print("Con tablas de millones de filas la diferencia es 10x-100x.")
print("Ver en Query Profile: 'Files pruned' = archivos saltados por skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4.3 Particionamiento vs Z-Order vs Liquid Clustering — la decisión
# MAGIC
# MAGIC | | **Particionamiento** | **Z-Order** | **Liquid Clustering** |
# MAGIC |---|---|---|---|
# MAGIC | Cómo funciona | Carpetas físicas por valor | Reordena datos dentro de archivos | Clustering incremental automático |
# MAGIC | Mantenimiento | Ninguno, pero rígido | OPTIMIZE manual periódico | Automático en background |
# MAGIC | Cambiar columnas | Reescribir TODA la tabla | Reescribir con OPTIMIZE | `ALTER TABLE` sin costo |
# MAGIC | Riesgo | Over-partitioning (miles de carpetas pequeñas) | Costo de reescritura total | Ninguno relevante |
# MAGIC | Estado 2026 | Solo casos específicos | **Legacy** — no usar en tablas nuevas | ✅ **El default moderno** |
# MAGIC
# MAGIC ### Reglas con números (para anotar):
# MAGIC
# MAGIC 1. **Tabla < 1 TB → NUNCA particionar.** Liquid Clustering o nada.
# MAGIC 2. **Particionar solo si**: tabla > 1 TB **Y** el patrón de filtro es estable y de baja
# MAGIC    cardinalidad (GJAHR en SAP: ~10 valores, siempre se filtra → candidata perfecta).
# MAGIC 3. **Z-Order**: solo si ya existe en tablas legacy. Para tablas nuevas → Liquid.
# MAGIC 4. **Liquid Clustering**: elegir 1-4 columnas de filtro frecuente.
# MAGIC    Con Predictive Optimization se puede usar `CLUSTER BY AUTO` y Databricks elige solas.
# MAGIC 5. **Cardinalidad para clustering**: columnas con muchos valores distintos (KUNNR,
# MAGIC    VBELN) funcionan BIEN en Liquid (a diferencia de particiones, donde serían un desastre).
# MAGIC
# MAGIC ### Aplicado a las tablas SAP del curso:
# MAGIC
# MAGIC | Tabla | Tamaño real | Decisión correcta |
# MAGIC |---|---|---|
# MAGIC | bkpf_bronze (5K filas) | ~1 MB | Nada — demasiado pequeña. La partición por GJAHR es **didáctica** |
# MAGIC | BKPF real de Argos (100M+ filas) | ~50 GB | Liquid por (BUKRS, GJAHR, BLART) |
# MAGIC | BSEG real (1B+ filas, multi-TB) | > 1 TB | Partición GJAHR + Liquid (BUKRS, HKONT) |
# MAGIC | vbak_gold | < 1 MB | Nada |

# COMMAND ----------

# Demo: crear una tabla con Liquid Clustering y verificarla
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.vbak_liquid
    CLUSTER BY (KUNNR, VKORG)
    COMMENT 'Demo Liquid Clustering — órdenes de venta clusterizadas por cliente y org'
    AS SELECT * FROM {CATALOG}.{SCHEMA}.vbak_silver
""")

d = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.vbak_liquid").collect()[0]
print(f"Tabla creada: vbak_liquid")
print(f"clusteringColumns: {d['clusteringColumns']}")
print()
print("Cambiar las columnas de clustering NO reescribe la tabla:")
print("  ALTER TABLE vbak_liquid CLUSTER BY (AUART, ERDAT)  -- solo aplica a datos nuevos")
print()
print("Con Predictive Optimization (workspaces enterprise):")
print("  CREATE TABLE ... CLUSTER BY AUTO  -- Databricks elige y ajusta las columnas")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4.4 Small Files + Auto Optimize — el problema que ya no deberías resolver a mano
# MAGIC
# MAGIC **El escenario SAP típico**: un pipeline incremental escribe cada hora un micro-batch
# MAGIC de BKPF. Cada escritura = archivos nuevos. En 1 mes: 3,600+ archivos pequeños.
# MAGIC La query que tardaba 5s ahora tarda 50s — sin que los datos hayan crecido.
# MAGIC
# MAGIC ### Las 3 capas de solución (de manual a automático):
# MAGIC
# MAGIC | Capa | Qué hace | Quién lo ejecuta |
# MAGIC |---|---|---|
# MAGIC | `OPTIMIZE tabla` | Compacta archivos pequeños en grandes | Tú, manual o job programado |
# MAGIC | Auto Optimize (`optimizeWrite` + `autoCompact`) | Compacta al escribir | Automático por tabla |
# MAGIC | **Predictive Optimization** | Databricks decide cuándo correr OPTIMIZE y VACUUM según el uso real | Automático a nivel de cuenta — **el estado del arte 2026** |
# MAGIC
# MAGIC > **Mensaje clave**: en runtime moderno con Unity Catalog y Predictive Optimization
# MAGIC > activo, NO programen jobs de OPTIMIZE manuales. Databricks lo hace mejor y más barato.

# COMMAND ----------

# Demo OPTIMIZE manual
print("ANTES del OPTIMIZE:")
d = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.bkpf_bronze").collect()[0]
print(f"  numFiles: {d['numFiles']}  |  size: {d['sizeInBytes']/1024/1024:.1f} MB")

resultado = spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.bkpf_bronze").collect()[0]

print("\nDESPUÉS del OPTIMIZE:")
d = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.bkpf_bronze").collect()[0]
print(f"  numFiles: {d['numFiles']}  |  size: {d['sizeInBytes']/1024/1024:.1f} MB")
print()
print("Nota: con tablas tan pequeñas el cambio es mínimo —")
print("en producción con miles de archivos la mejora es dramática.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4.5 Shuffle y Spill — cómo evitarlos en queries SAP
# MAGIC
# MAGIC **Shuffle** = mover datos entre nodos. Ocurre en JOINs y GROUP BY cuando los datos
# MAGIC relacionados están en nodos diferentes. Es la operación más cara de Spark.
# MAGIC
# MAGIC **Spill** = la memoria del nodo no alcanza y Spark escribe a disco temporal.
# MAGIC Una query con spill puede ser 10x más lenta.
# MAGIC
# MAGIC ### 3 tips prácticos para SAP:
# MAGIC
# MAGIC 1. **Broadcast join para tablas maestras pequeñas**: KNA1 (500 clientes) y MARA
# MAGIC    (300 materiales) caben en memoria → Spark las copia completas a cada nodo y
# MAGIC    elimina el shuffle del JOIN. Spark lo hace solo si la tabla < 10 MB (AQE),
# MAGIC    pero se puede forzar con hint.
# MAGIC 2. **Filtrar ANTES del JOIN**: `WHERE GJAHR = 2024` antes de unir BKPF con BSEG
# MAGIC    reduce los datos que viajan en el shuffle.
# MAGIC 3. **Nunca `collect()` masivos**: `df.collect()` trae TODO al driver → OOM.
# MAGIC    Usar `display()`, `limit()` o escribir a tabla.

# COMMAND ----------

#Tablas < 10 MB (nuestro caso): AQE lo hace solo, no tocar nada
#Tablas 10 MB - 1 GB: usar el hint BROADCAST o subir el threshold
#Tablas > 1 GB: nunca hacer broadcast — revisar la estrategia de layout

resultado = spark.sql(f"""
    SELECT /*+ BROADCAST(k) */
        v.VBELN,
        v.KUNNR,
        v.NETWR,
        k.nombre_cliente,
        k.pais,
        k.ciudad
    FROM {CATALOG}.{SCHEMA}.vbak_silver v
    LEFT JOIN {CATALOG}.{SCHEMA}.kna1_silver k
        ON v.KUNNR = k.codigo_cliente
""")

print(f"JOIN con broadcast: {resultado.count():,} filas")
resultado.show(5, truncate=False)
print()
print("Ver en Query Profile → el nodo JOIN dice 'BroadcastHashJoin'")
print("en lugar de 'SortMergeJoin' (que implica shuffle entre nodos)")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4.6 El mejor tuning es no tunear — Runtime moderno
# MAGIC
# MAGIC Todo lo siguiente ya viene activado por defecto en runtimes modernos (DBR 14+/LTS):
# MAGIC
# MAGIC | Optimización | Qué hace | Config manual necesaria |
# MAGIC |---|---|---|
# MAGIC | **Photon** | Motor vectorizado C++ — hasta 12x en SQL | Ninguna (Serverless lo incluye) |
# MAGIC | **AQE** (Adaptive Query Execution) | Re-optimiza el plan en runtime: ajusta particiones de shuffle, convierte a broadcast join, maneja skew | Ninguna |
# MAGIC | **Disk Cache** | Cachea en SSD local los datos Parquet leídos | Ninguna |
# MAGIC | **Auto stats** | Estadísticas para el Cost-Based Optimizer | Ninguna con Unity Catalog |
# MAGIC
# MAGIC > **Regla de oro**: usar siempre el último LTS Runtime + Serverless. Si encuentran
# MAGIC > código viejo con `spark.conf.set(...)` de tuning, lo más probable es que en
# MAGIC > runtime moderno sobre o incluso estorbe.
# MAGIC
# MAGIC > **Evitar UDFs de Python**: Photon no las acelera. Reescribir con funciones SQL
# MAGIC > nativas (`CASE WHEN`, `ai_classify`, etc.) — vimos esto en M3b.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4.7 Costos I — Tipos de cómputo: la decisión que más plata mueve
# MAGIC
# MAGIC | Tipo | DBU aprox | Cuándo usarlo |
# MAGIC |---|---|---|
# MAGIC | **All-purpose cluster** | ~0.55-0.75 DBU/h por core | Desarrollo interactivo en notebooks. **NUNCA en jobs** |
# MAGIC | **Jobs cluster** | ~0.15-0.30 DBU/h | Pipelines productivos — **5x más barato** que all-purpose |
# MAGIC | **SQL Warehouse Serverless** | ~0.70 DBU/h (2X-Small) | SQL, dashboards, Genie. Arranque instantáneo, auto-stop |
# MAGIC | **Serverless notebooks/jobs** | Por uso real | Cargas variables — pagas solo lo que ejecutas |
# MAGIC
# MAGIC ### Reglas de ahorro inmediato:
# MAGIC 1. Pipeline productivo en all-purpose → migrarlo a Jobs cluster = **-80% de costo**
# MAGIC 2. Auto-stop agresivo: 10 min para warehouses de desarrollo, 1-5 min serverless
# MAGIC 3. Serverless para cargas esporádicas — sin pagar idle
# MAGIC 4. Spot instances en Jobs clusters tolerantes a fallos = hasta -60% adicional

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4.8 Costos II — Tagging: saber QUIÉN gasta QUÉ
# MAGIC
# MAGIC Sin tags, la factura de Databricks es un número gigante imposible de explicar.
# MAGIC Con tags, cada peso se atribuye a un equipo, proyecto o ambiente.
# MAGIC
# MAGIC ### Dónde se aplican:
# MAGIC - **Clusters / Jobs / Warehouses**: campo "Tags" en la configuración
# MAGIC - Se propagan automáticamente a `system.billing.usage` → columna `custom_tags`
# MAGIC
# MAGIC ### Convención recomendada para Summa:
# MAGIC ```
# MAGIC equipo        = datos | finanzas | abastecimiento
# MAGIC proyecto      = sap-medallion | analytics-comercial | curso-databricks
# MAGIC ambiente      = dev | qa | prod
# MAGIC centro_costo  = CC-1020 (el código contable real)
# MAGIC ```
# MAGIC
# MAGIC > Con esto el área de finanzas de Summa puede hacer chargeback real por
# MAGIC > centro de costo — exactamente como lo hacen con SAP CO.

# COMMAND ----------

# Query de costos por tag — la base del chargeback
# (en Free Edition custom_tags puede venir vacío; en el workspace de Summa funciona completo)
spark.sql("""
    SELECT
        COALESCE(custom_tags['proyecto'], 'sin-tag')   AS proyecto,
        billing_origin_product                          AS producto,
        usage_unit,
        ROUND(SUM(usage_quantity), 2)                   AS consumo
    FROM system.billing.usage
    WHERE usage_date >= DATE_TRUNC('month', CURRENT_DATE())
    GROUP BY 1, 2, 3
    ORDER BY consumo DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4.9 Costos III — system.billing: la fuente de la verdad
# MAGIC
# MAGIC Dos tablas clave del sistema:
# MAGIC - `system.billing.usage` — qué se consumió (DBU, DSU) por día, SKU, recurso y tag
# MAGIC - `system.billing.list_prices` — el precio de lista de cada SKU
# MAGIC
# MAGIC Juntas permiten calcular el costo real en dólares sin esperar la factura.

# COMMAND ----------

# Costo del mes actual en USD, por producto — JOIN usage + list_prices
spark.sql("""
    SELECT
        u.billing_origin_product                            AS producto,
        u.sku_name,
        ROUND(SUM(u.usage_quantity), 3)                     AS unidades,
        ROUND(SUM(u.usage_quantity * lp.pricing.default), 2) AS costo_usd
    FROM system.billing.usage u
    JOIN system.billing.list_prices lp
        ON  u.sku_name = lp.sku_name
        AND u.usage_start_time >= lp.price_start_time
        AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
    WHERE u.usage_date >= DATE_TRUNC('month', CURRENT_DATE())
    GROUP BY 1, 2
    ORDER BY costo_usd DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4.10 Costos IV — ¿Cuánto cuesta la IA? (Genie + AI_QUERY)
# MAGIC
# MAGIC La IA consume **DSUs** (no DBUs) y aparece separada en system.billing.usage:
# MAGIC
# MAGIC | Consumo IA | Cómo se cobra | Tip de ahorro |
# MAGIC |---|---|---|
# MAGIC | `AI_QUERY()` y funciones `ai_*` | Por tokens procesados (pay-per-token) | `LIMIT` en desarrollo. En producción: job incremental que NO reprocesa lo ya clasificado (patrón Delta del M3b) |
# MAGIC | Genie (Chat) | Hoy: solo el compute del warehouse. **Desde julio 2026: pay-as-you-go con allowance mensual gratis** | Monitorear el tab Monitoring + presupuestos de Genie |
# MAGIC | Genie (Agent mode) | Igual — entra al pricing de julio 2026 | Usarlo para análisis profundos, no para preguntas simples que el modo Chat resuelve |
# MAGIC
# MAGIC ### ¿Y si traigo mi propio modelo para ahorrar?
# MAGIC
# MAGIC Sí se puede — 2 caminos:
# MAGIC 1. **Custom model**: registrar un modelo open-source (Hugging Face, fine-tuneado propio)
# MAGIC    en Unity Catalog → servir con Model Serving → consultarlo con `ai_query('mi-endpoint', ...)`
# MAGIC 2. **External model**: conectar la API key corporativa de OpenAI/Anthropic →
# MAGIC    endpoint unificado con governance del AI Gateway → pagan al proveedor directo
# MAGIC
# MAGIC > **La cuenta honesta**: modelo propio NO siempre es más barato. Pagas el compute
# MAGIC > del endpoint (GPU) mientras esté encendido. Regla: volumen bajo/esporádico →
# MAGIC > Foundation Models pay-per-token gana. Volumen masivo constante (clasificar
# MAGIC > millones de registros SAP diarios) → modelo propio con scale-to-zero puede ganar.

# COMMAND ----------

# Cuánto ha costado la IA en este workspace (DSUs del curso)
spark.sql("""
    SELECT
        usage_date,
        sku_name,
        ROUND(SUM(usage_quantity), 4)  AS dsus
    FROM system.billing.usage
    WHERE usage_unit = 'DSU'
    GROUP BY usage_date, sku_name
    ORDER BY usage_date DESC
    LIMIT 15
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumen del Módulo 4 — los 10 tips para llevarse
# MAGIC
# MAGIC 1. El nodo más ancho del Query Profile ES el problema (small files / shuffle / spill)
# MAGIC 2. Tabla < 1 TB → nunca particionar
# MAGIC 3. Tablas nuevas → Liquid Clustering. Z-Order es legacy
# MAGIC 4. `CLUSTER BY AUTO` con Predictive Optimization = cero mantenimiento
# MAGIC 5. No programar OPTIMIZE manual si Predictive Optimization está activo
# MAGIC 6. Broadcast join para maestras pequeñas (KNA1, MARA) — elimina el shuffle
# MAGIC 7. Filtrar antes del JOIN, nunca `collect()` masivo
# MAGIC 8. Último LTS Runtime + Serverless = Photon + AQE + cache gratis, sin configs
# MAGIC 9. Jobs cluster para pipelines = 5x más barato que all-purpose
# MAGIC 10. Tags en todos los recursos → chargeback por centro de costo con system.billing
# MAGIC
# MAGIC **Próximo módulo**: M5 — Arquitectura Medallion completa con pipeline incremental.
