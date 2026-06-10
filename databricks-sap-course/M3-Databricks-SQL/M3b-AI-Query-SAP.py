# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 3b: Funciones de IA en SQL — Enriquecimiento de datos SAP
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC - Conocer el catálogo completo de funciones de IA nativas de Databricks SQL
# MAGIC - Entender cuándo usar funciones especializadas vs AI_QUERY() genérica
# MAGIC - Entender la diferencia entre Genie Space (IA para analistas) y AI_QUERY() (IA para ingenieros)
# MAGIC - Aplicar modelos de lenguaje directamente en SQL y PySpark sobre tablas SAP
# MAGIC - Construir pipelines de enriquecimiento automático con IA
# MAGIC - Controlar costos de DSUs en consultas con funciones de IA
# MAGIC
# MAGIC ---
# MAGIC > **Prerequisito**: tener las tablas Bronze cargadas del Módulo 2.
# MAGIC > Este notebook usa el catálogo `laboratory_sap_dev.sap_course`.
# MAGIC
# MAGIC ---
# MAGIC ## Funciones de IA nativas de Databricks SQL
# MAGIC
# MAGIC Databricks tiene **dos tipos** de funciones de IA en SQL:
# MAGIC
# MAGIC ### Tipo 1 — Funciones especializadas (recomendadas cuando aplican)
# MAGIC Son más simples, más rápidas y optimizadas para un caso de uso específico.
# MAGIC No requieren escribir un prompt — solo el texto y los parámetros.
# MAGIC
# MAGIC | Función | Qué hace | Caso de uso SAP |
# MAGIC |---|---|---|
# MAGIC | `ai_classify(texto, [categorías])` | Clasifica texto en una de las categorías dadas | Clasificar BKTXT, tipo de proveedor, categoría de gasto |
# MAGIC | `ai_summarize(texto, num_palabras)` | Resume un texto largo en N palabras | Resumir notas de pedido, comentarios de auditoría |
# MAGIC | `ai_translate(texto, idioma_destino)` | Traduce texto a otro idioma | Traducir MAKTX de inglés a español |
# MAGIC | `ai_analyze_sentiment(texto)` | Devuelve: positive / negative / neutral / mixed | Analizar feedback de proveedores, comentarios de clientes |
# MAGIC | `ai_extract(texto, [campos])` | Extrae campos estructurados de texto libre | Extraer número de factura, fecha, monto de un texto |
# MAGIC | `ai_fix_grammar(texto)` | Corrige gramática y ortografía | Normalizar descripciones de materiales MAKTX |
# MAGIC | `ai_mask(texto, [entidades])` | Enmascara datos sensibles (nombre, email, teléfono) | Anonimizar datos de clientes/proveedores para analytics |
# MAGIC | `ai_gen(prompt)` | Genera texto libre desde un prompt | Generar descripciones, plantillas de email |
# MAGIC | `ai_parse_document(documento)` | Convierte PDFs/imágenes en tablas estructuradas *(beta)* | Procesar facturas PDF, contratos escaneados |
# MAGIC
# MAGIC ### Tipo 2 — AI_QUERY() genérica (para casos no cubiertos por las especializadas)
# MAGIC Cuando ninguna función especializada cubre el caso, `AI_QUERY()` permite enviar
# MAGIC cualquier prompt a cualquier modelo con control total.
# MAGIC
# MAGIC ```sql
# MAGIC AI_QUERY('databricks-meta-llama-3-3-70b-instruct', 'tu prompt aquí')
# MAGIC ```
# MAGIC
# MAGIC > **Regla práctica**: siempre intentar primero con una función especializada.
# MAGIC > Solo usar `AI_QUERY()` cuando necesites lógica de prompt personalizada.
# MAGIC
# MAGIC ---
# MAGIC ## ¿Cuándo usar Genie Space vs funciones de IA en SQL?
# MAGIC
# MAGIC | | Genie Space | Funciones IA en SQL |
# MAGIC |---|---|---|
# MAGIC | **Quién lo usa** | Analista de negocio | Ingeniero de datos |
# MAGIC | **Dónde vive** | UI de Databricks SQL | SQL / PySpark / notebook / pipeline |
# MAGIC | **Caso de uso** | "¿Cuánto vendimos en 2023?" | Enriquecer 50,000 registros SAP |
# MAGIC | **Se puede automatizar** | No | Sí — como job nocturno en Delta |
# MAGIC | **Costo** | DSUs por consulta | DSUs por registro procesado |
# MAGIC
# MAGIC > **Regla de producción**: siempre usar `LIMIT` en los labs. Procesar 5,000
# MAGIC > registros con IA consume muchos más DSUs que 20. En producción: job incremental.

# COMMAND ----------

# Configuración — mismo catalog que el resto del curso
CATALOG     = "laboratory_sap_dev"
SCHEMA      = "sap_course"
MODEL       = "databricks-meta-llama-3-3-70b-instruct"

spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"Catalog : {CATALOG}")
print(f"Schema  : {CATALOG}.{SCHEMA}")
print(f"Modelo  : {MODEL}")
print()

# Verificar tablas disponibles
tablas = ["bkpf_bronze", "bseg_bronze", "vbak_silver", "kna1_bronze", "mara_bronze"]
print("=== TABLAS DISPONIBLES ===")
for t in tablas:
    try:
        n = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
        print(f"  OK  {t:<25} {n:>8,} registros")
    except:
        print(f"  ERR {t} — ejecutar M2 primero")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo rápida: Funciones especializadas en acción
# MAGIC
# MAGIC Antes de los casos de uso SAP, veamos las funciones especializadas en 1 línea cada una.
# MAGIC Estas son las más útiles para datos SAP del día a día:

# COMMAND ----------

# Las funciones especializadas se ejecutan directamente en el SQL Editor
# Descomentar y pegar cada bloque en el SQL Editor para probar

# -- 1. ai_classify: clasificar tipo de documento por texto libre
# SELECT ai_classify(
#     'Pago a proveedor Cemex por materiales factura 90210',
#     ARRAY('Pago proveedor', 'Factura cliente', 'Asiento manual', 'Nómina', 'Activo fijo')
# ) AS tipo_documento;
# -- Resultado esperado: 'Pago proveedor'

# -- 2. ai_translate: traducir descripción de material de inglés a español
# SELECT ai_translate('Ordinary Portland Cement 50kg - Construction grade', 'Spanish') AS maktx_es;

# -- 3. ai_summarize: resumir notas largas de pedidos de compra SAP
# SELECT ai_summarize(
#     'Purchase Order for limestone, clay and iron ore from vendor LAFARGE
#      for Barranquilla plant. Q1 2024 production cycle. Approved 15-Jan-2024.',
#     50
# ) AS resumen_po;

# -- 4. ai_analyze_sentiment: analizar feedback de proveedores
# SELECT ai_analyze_sentiment(
#     'El proveedor entregó a tiempo pero la calidad no cumplió especificaciones.'
# ) AS sentimiento;  -- Resultado: 'mixed'

# -- 5. ai_extract: extraer campos de texto libre de nota SAP
# SELECT ai_extract(
#     'Factura F-2024-00892 de Holcim Colombia por COP 45.800.000 con fecha 20 enero 2024',
#     ARRAY('numero_factura', 'proveedor', 'monto', 'fecha_emision')
# ) AS campos_extraidos;

# -- 6. ai_fix_grammar: normalizar descripciones de materiales mal escritas
# SELECT ai_fix_grammar('cemto porltand tipo I pra construcin gral') AS descripcion_corregida;
# -- Resultado: 'Cemento Portland tipo I para construcción general'

# -- 7. ai_mask: anonimizar datos de clientes para ambientes de desarrollo
# SELECT ai_mask(
#     'Cliente Juan García NIT 900.123.456-7 tel 3001234567',
#     ARRAY('person', 'phone')
# ) AS texto_anonimizado;

print("Catálogo completo de funciones de IA nativas de Databricks SQL:")
print()
funciones = [
    ("ai_classify(texto, [categorías])",   "Clasifica en categorías predefinidas — ideal para BKTXT, BLART"),
    ("ai_translate(texto, idioma)",        "Traduce — ideal para MAKTX en inglés/alemán"),
    ("ai_summarize(texto, max_palabras)",  "Resume — notas largas de pedidos, comentarios auditoría"),
    ("ai_analyze_sentiment(texto)",        "Sentimiento: positive/negative/neutral/mixed"),
    ("ai_extract(texto, [campos])",        "Extrae campos estructurados de texto libre"),
    ("ai_fix_grammar(texto)",              "Corrige ortografía — normalizar MAKTX inconsistentes"),
    ("ai_mask(texto, [entidades])",        "Enmascara datos sensibles — anonimizar clientes/proveedores"),
    ("ai_gen(prompt)",                     "Genera texto libre desde un prompt"),
    ("ai_parse_document(doc) [beta]",      "Convierte PDFs/imágenes en tablas — facturas SAP escaneadas"),
    ("AI_QUERY(modelo, prompt)",           "Genérica — para cualquier caso no cubierto arriba"),
]
print(f"  {'Función':<45}  Caso de uso SAP")
print("  " + "-"*90)
for fn, desc in funciones:
    print(f"  {fn:<45}  {desc}")
print()
print("Regla: usar siempre la función especializada cuando existe.")
print("Solo AI_QUERY() cuando necesitas lógica de prompt personalizada.")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Caso 1: Clasificación automática de textos SAP (BKTXT)
# MAGIC
# MAGIC El campo `BKTXT` (texto de cabecera del documento) en SAP es texto libre —
# MAGIC cada usuario escribe lo que quiere. Clasificarlo con reglas `LIKE` es imposible.
# MAGIC Con AI_QUERY() se clasifica automáticamente en categorías estandarizadas.
# MAGIC
# MAGIC **Impacto**: permite agrupar documentos por categoría semántica para reportes
# MAGIC de auditoría, control interno y análisis de gasto.

# COMMAND ----------

# %sql  -- Descomentar para ejecutar en SQL Editor y ver en Query Profile
# SELECT
#     BUKRS,
#     BELNR,
#     GJAHR,
#     BKTXT                                           AS texto_original,
#     AI_QUERY(
#         'databricks-meta-llama-3-3-70b-instruct',
#         CONCAT(
#             'Clasifica este texto de documento contable SAP en UNA de estas categorías: ',
#             '[Pago proveedor, Factura cliente, Asiento manual, Devolución, Nómina, Activo fijo, Otro]. ',
#             'Responde SOLO con la categoría, sin explicación ni puntuación. ',
#             'Texto: ', COALESCE(BKTXT, 'sin texto')
#         )
#     )                                               AS categoria_ia
# FROM laboratory_sap_dev.sap_course.bkpf_bronze
# WHERE BKTXT IS NOT NULL
#   AND BKTXT != ''
# LIMIT 20

# Versión PySpark — misma lógica, ejecutable desde el notebook
resultado_1 = spark.sql(f"""
    SELECT
        BUKRS,
        BELNR,
        GJAHR,
        BKTXT                                           AS texto_original,
        AI_QUERY(
            '{MODEL}',
            CONCAT(
                'Clasifica este texto de documento contable SAP en UNA de estas categorías: ',
                '[Pago proveedor, Factura cliente, Asiento manual, Devolución, Nómina, Activo fijo, Otro]. ',
                'Responde SOLO con la categoría, sin explicación ni puntuación. ',
                'Texto: ', COALESCE(BKTXT, 'sin texto')
            )
        )                                               AS categoria_ia
    FROM {CATALOG}.{SCHEMA}.bkpf_bronze
    WHERE BKTXT IS NOT NULL
      AND BKTXT != ''
    LIMIT 20
""")

print("=== Clasificación IA de documentos SAP por texto BKTXT ===")
resultado_1.show(truncate=False)

# COMMAND ----------

# Análisis de la distribución de categorías detectadas
print("=== Distribución de categorías IA ===")
resultado_1.groupBy("categoria_ia").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Caso 2: Detección de anomalías en documentos contables
# MAGIC
# MAGIC Un auditor SAP revisa manualmente documentos de alto valor buscando patrones
# MAGIC inusuales: monedas raras, clases de documento inusuales para el monto, etc.
# MAGIC AI_QUERY() puede hacer un primer filtro automático antes de la revisión manual.
# MAGIC
# MAGIC **Impacto**: reduce el tiempo de revisión de auditoría al priorizar los
# MAGIC documentos con mayor riesgo.

# COMMAND ----------

resultado_2 = spark.sql(f"""
    SELECT
        b.BUKRS,
        b.BELNR,
        b.GJAHR,
        b.BLART,
        b.WAERK,
        ROUND(s.DMBTR, 2)                               AS monto,
        AI_QUERY(
            '{MODEL}',
            CONCAT(
                'Eres auditor SAP experto en control interno. ',
                'Analiza este documento contable y detecta anomalías o riesgos: ',
                'Sociedad=', b.BUKRS,
                ', Clase documento=', b.BLART,
                ', Moneda=', b.WAERK,
                ', Monto=', ROUND(s.DMBTR, 2),
                '. Responde en máximo 1 oración. Si no hay anomalía di: Sin anomalías detectadas.'
            )
        )                                               AS alerta_auditoria
    FROM {CATALOG}.{SCHEMA}.bkpf_bronze b
    JOIN {CATALOG}.{SCHEMA}.bseg_bronze s
        ON  b.BELNR = s.BELNR
        AND b.BUKRS = s.BUKRS
        AND b.GJAHR = s.GJAHR
    WHERE s.DMBTR > 100000
    LIMIT 10
""")

print("=== Alertas de auditoría IA sobre documentos SAP de alto valor ===")
resultado_2.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Caso 3: Resumen ejecutivo de cliente SAP
# MAGIC
# MAGIC Los reportes de cuenta de cliente en SAP muestran números — no narrativa.
# MAGIC AI_QUERY() genera automáticamente un párrafo ejecutivo para cada cliente
# MAGIC que puede usarse en presentaciones, correos o CRM.
# MAGIC
# MAGIC **Impacto**: acelera la preparación de reuniones con clientes estratégicos
# MAGIC sin tener que redactar manualmente cada resumen.

# COMMAND ----------

# Primero construimos la vista de clientes con métricas
from pyspark.sql.functions import col, count, sum as _sum, round as _round, when

df_clientes = spark.sql(f"""
    WITH metricas AS (
        SELECT
            v.KUNNR,
            COUNT(DISTINCT v.VBELN)         AS total_ordenes,
            ROUND(SUM(v.NETWR), 0)          AS revenue_total,
            ROUND(AVG(v.NETWR), 0)          AS ticket_promedio
        FROM {CATALOG}.{SCHEMA}.vbak_bronze v
        GROUP BY v.KUNNR
    )
    SELECT
        m.*,
        k.NAME1                             AS nombre_cliente,
        k.LAND1                             AS pais,
        k.ORT01                             AS ciudad,
        CASE
            WHEN m.revenue_total > 1000000 THEN 'A - Strategic'
            WHEN m.revenue_total > 500000  THEN 'B - Key Account'
            WHEN m.revenue_total > 100000  THEN 'C - Standard'
            ELSE 'D - Small'
        END                                 AS clasificacion_abc
    FROM metricas m
    LEFT JOIN {CATALOG}.{SCHEMA}.kna1_bronze k ON m.KUNNR = k.KUNNR
    ORDER BY revenue_total DESC
    LIMIT 5
""")

print("Top 5 clientes SAP para generar resúmenes ejecutivos:")
df_clientes.show(truncate=False)

# COMMAND ----------

resultado_3 = spark.sql(f"""
    WITH metricas AS (
        SELECT
            v.KUNNR,
            COUNT(DISTINCT v.VBELN)         AS total_ordenes,
            ROUND(SUM(v.NETWR), 0)          AS revenue_total,
            ROUND(AVG(v.NETWR), 0)          AS ticket_promedio,
            CASE
                WHEN SUM(v.NETWR) > 1000000 THEN 'A - Strategic'
                WHEN SUM(v.NETWR) > 500000  THEN 'B - Key Account'
                WHEN SUM(v.NETWR) > 100000  THEN 'C - Standard'
                ELSE 'D - Small'
            END                             AS clasificacion_abc
        FROM {CATALOG}.{SCHEMA}.vbak_bronze v
        GROUP BY v.KUNNR
    )
    SELECT
        k.NAME1                             AS nombre_cliente,
        k.LAND1                             AS pais,
        m.revenue_total,
        m.total_ordenes,
        m.clasificacion_abc,
        AI_QUERY(
            '{MODEL}',
            CONCAT(
                'Genera un resumen ejecutivo de 2 oraciones para este cliente SAP. ',
                'Usa tono profesional de negocios en español. ',
                'Cliente: ', k.NAME1,
                ', País: ', k.LAND1,
                ', Ciudad: ', COALESCE(k.ORT01, 'N/A'),
                ', Revenue total: $', m.revenue_total, ' USD',
                ', Órdenes: ', m.total_ordenes,
                ', Clasificación: ', m.clasificacion_abc,
                ', Ticket promedio: $', m.ticket_promedio, ' USD.'
            )
        )                                   AS resumen_ejecutivo_ia
    FROM metricas m
    LEFT JOIN {CATALOG}.{SCHEMA}.kna1_bronze k ON m.KUNNR = k.KUNNR
    ORDER BY m.revenue_total DESC
    LIMIT 5
""")

print("=== Resúmenes ejecutivos IA por cliente SAP ===")
resultado_3.select("nombre_cliente", "pais", "revenue_total", "clasificacion_abc", "resumen_ejecutivo_ia").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Caso 4: Normalización de descripciones de materiales (MARA)
# MAGIC
# MAGIC Las descripciones de materiales en SAP (campo MAKTX) suelen ser inconsistentes:
# MAGIC abreviaturas, mezcla de idiomas, errores tipográficos. Esto dificulta búsquedas
# MAGIC y consolidaciones.
# MAGIC
# MAGIC **Impacto**: mejora la calidad del maestro de materiales para búsquedas,
# MAGIC análisis de spend y catálogos de compras.

# COMMAND ----------

resultado_4 = spark.sql(f"""
    SELECT
        MATNR,
        MTART                                           AS tipo_material,
        MATKL                                           AS categoria,
        MAKTX                                           AS descripcion_original,
        AI_QUERY(
            '{MODEL}',
            CONCAT(
                'Normaliza esta descripción de material SAP siguiendo estas reglas: ',
                '1) Tradúcela al español si está en otro idioma. ',
                '2) Corrige errores tipográficos. ',
                '3) Usa formato: [Tipo] - [Nombre] - [Característica principal]. ',
                '4) Máximo 10 palabras. ',
                'Responde SOLO con la descripción normalizada. ',
                'Descripción: ', COALESCE(MAKTX, 'sin descripción')
            )
        )                                               AS descripcion_normalizada
    FROM {CATALOG}.{SCHEMA}.mara_bronze
    WHERE MAKTX IS NOT NULL
    LIMIT 15
""")

print("=== Normalización IA de descripciones de materiales SAP ===")
resultado_4.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Caso 5 (avanzado): Pipeline de enriquecimiento automático
# MAGIC
# MAGIC Los 4 casos anteriores ejecutan AI_QUERY() en tiempo real.
# MAGIC En producción conviene guardar los resultados como tabla Delta para:
# MAGIC - No pagar DSUs cada vez que se consulta
# MAGIC - Auditar qué clasificó la IA y cuándo
# MAGIC - Reentrenar o ajustar el prompt con el tiempo
# MAGIC
# MAGIC **Patrón recomendado**: tabla de enriquecimiento IA separada de la Bronze.

# COMMAND ----------

# Guardar las clasificaciones de BKTXT como tabla Delta
# Solo procesar los que no tienen clasificación todavía (incremental)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.bkpf_ia_enriched (
        BUKRS       STRING  COMMENT 'Sociedad SAP',
        BELNR       STRING  COMMENT 'Número documento',
        GJAHR       STRING  COMMENT 'Ejercicio fiscal',
        bktxt_orig  STRING  COMMENT 'Texto original BKTXT',
        categoria_ia STRING COMMENT 'Categoría clasificada por IA',
        modelo_ia   STRING  COMMENT 'Modelo de IA usado',
        procesado_en TIMESTAMP COMMENT 'Fecha de enriquecimiento'
    )
    USING DELTA
    COMMENT 'Enriquecimiento IA de documentos contables SAP — clasificación automática de BKTXT'
""")

# Insertar nuevos registros (solo los que no están ya clasificados)
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.bkpf_ia_enriched
    SELECT
        b.BUKRS,
        b.BELNR,
        b.GJAHR,
        b.BKTXT                                         AS bktxt_orig,
        AI_QUERY(
            '{MODEL}',
            CONCAT(
                'Clasifica en UNA categoría: ',
                '[Pago proveedor, Factura cliente, Asiento manual, Devolución, Nómina, Activo fijo, Otro]. ',
                'Solo la categoría, sin más texto. Texto: ',
                COALESCE(b.BKTXT, 'sin texto')
            )
        )                                               AS categoria_ia,
        '{MODEL}'                                       AS modelo_ia,
        current_timestamp()                             AS procesado_en
    FROM {CATALOG}.{SCHEMA}.bkpf_bronze b
    LEFT JOIN {CATALOG}.{SCHEMA}.bkpf_ia_enriched e
        ON  b.BELNR = e.BELNR
        AND b.BUKRS = e.BUKRS
        AND b.GJAHR = e.GJAHR
    WHERE e.BELNR IS NULL       -- Solo los que no están clasificados todavía
      AND b.BKTXT IS NOT NULL
    LIMIT 50                    -- En producción: sin LIMIT, ejecutar como job nocturno
""")

n = spark.table(f"{CATALOG}.{SCHEMA}.bkpf_ia_enriched").count()
print(f"Tabla bkpf_ia_enriched: {n:,} documentos enriquecidos")
print()

# Ver la distribución de categorías guardadas
print("=== Distribución de categorías guardadas en Delta ===")
spark.sql(f"""
    SELECT
        categoria_ia,
        COUNT(*)                AS documentos,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
    FROM {CATALOG}.{SCHEMA}.bkpf_ia_enriched
    GROUP BY categoria_ia
    ORDER BY documentos DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Resumen del Módulo 3b
# MAGIC
# MAGIC ✅ **AI_QUERY()** = IA programática dentro de SQL y PySpark — para ingenieros de datos
# MAGIC ✅ **Genie Space** = IA conversacional en la UI — para analistas de negocio
# MAGIC ✅ **4 casos de uso SAP**:
# MAGIC    - Clasificación automática de textos BKTXT
# MAGIC    - Detección de anomalías en documentos de alto valor
# MAGIC    - Resúmenes ejecutivos de clientes
# MAGIC    - Normalización de descripciones de materiales MAKTX
# MAGIC ✅ **Patrón de producción**: guardar resultados en tabla Delta — no procesar dos veces
# MAGIC ✅ **Control de costos**: siempre `LIMIT` en desarrollo, job nocturno incremental en producción
# MAGIC
# MAGIC **Próximo módulo**: Optimización y control de costos — cómo reducir el gasto en DBUs y DSUs

