# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 8: Cierre — Arquitectura de Referencia y Hoja de Ruta
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC - Consolidar la arquitectura de referencia completa SAP + Azure Databricks
# MAGIC - Revisar el checklist de buenas prácticas por dimensión
# MAGIC - Identificar los errores más comunes en implementaciones reales
# MAGIC - Definir la hoja de ruta para el cliente
# MAGIC - Responder preguntas específicas del equipo
# MAGIC
# MAGIC ---
# MAGIC ## 8.1 Arquitectura de referencia — SAP + Azure Databricks + Delta Sharing
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────────┐
# MAGIC │                         CAPA DE FUENTES SAP                              │
# MAGIC │                                                                          │
# MAGIC │   S/4HANA RISE    SAP ECC Legacy    SAP BW/4HANA    SuccessFactors       │
# MAGIC │       │                │                 │               │               │
# MAGIC │       └────────────────┴─────────────────┴───────────────┘               │
# MAGIC │                                    │                                     │
# MAGIC │                           Replication Flow                               │
# MAGIC │                          (CDS-based, ABAP)                               │
# MAGIC │                                    │                                     │
# MAGIC │                          SAP Datasphere                                  │
# MAGIC │                       Object Store (Parquet)                             │
# MAGIC │                                    │                                     │
# MAGIC │                      Managed Data Products                               │
# MAGIC │                       (curated, governed)                                │
# MAGIC └──────────────────────────────────────────────────────────────────────────┘
# MAGIC                                     │
# MAGIC                          BDC Connect │ Delta Sharing
# MAGIC                           (mTLS + OIDC, zero-copy)
# MAGIC                                     │
# MAGIC ┌──────────────────────────────────────────────────────────────────────────┐
# MAGIC │                    AZURE DATABRICKS LAKEHOUSE                            │
# MAGIC │                                                                          │
# MAGIC │  Unity Catalog (governance unificado de todos los activos)               │
# MAGIC │                                                                          │
# MAGIC │  BRONZE              SILVER              GOLD                            │
# MAGIC │  Raw SAP data  ───►  Cleaned + typed ──► KPIs + ML features             │
# MAGIC │  (Delta Lake)        (Delta Lake)         (Delta Lake)                   │
# MAGIC │                                                │                         │
# MAGIC │              ┌─────────────┬──────────────────┘                         │
# MAGIC │              ▼             ▼                                             │
# MAGIC │     ML Models /     Databricks SQL                                       │
# MAGIC │     AI Agents       Dashboards + Genie                                   │
# MAGIC │              │             │                                             │
# MAGIC │              └──────┬──────┘                                             │
# MAGIC │                     │ Delta Sharing (vuelta a SAP)                       │
# MAGIC └─────────────────────────────────────────────────────────────────────────┘
# MAGIC                        │
# MAGIC ┌──────────────────────▼──────────────────────────────────────────────────┐
# MAGIC │                   CAPA DE CONSUMO SAP                                   │
# MAGIC │                                                                          │
# MAGIC │   SAP Analytics Cloud     SAP Joule        SAP Datasphere               │
# MAGIC │   (dashboards, stories)   (AI agéntica)    (modelos enriquecidos)        │
# MAGIC └──────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

spark.sql("USE sap_course")
print("Módulo 8: Arquitectura de Referencia y Cierre")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 Checklist de buenas prácticas

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════╗
║           CHECKLIST DE BUENAS PRÁCTICAS                      ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  COSTOS                                                      ║
║  ─────────────────────────────────────────────────────────  ║
║  [ ] Usar Jobs clusters en pipelines productivos (5x barato) ║
║  [ ] Auto-stop en SQL Warehouses (máx 15 min)                ║
║  [ ] Tags en todos los clusters: proyecto, equipo, ambiente  ║
║  [ ] Revisar system.billing.usage semanalmente               ║
║  [ ] OPTIMIZE programado en tablas con cargas frecuentes     ║
║  [ ] Liquid Clustering en tablas nuevas (no Z-Order manual)  ║
║                                                              ║
║  GOVERNANCE                                                  ║
║  ─────────────────────────────────────────────────────────  ║
║  [ ] Unity Catalog habilitado (nunca Hive metastore legacy)  ║
║  [ ] Naming convention: env_dominio_tabla (ej: prod_fi_bkpf) ║
║  [ ] Column-level security en datos PII (KNA1.NAME1, etc.)   ║
║  [ ] Row-level filters por sociedad/organización             ║
║  [ ] Data lineage activado (sabe de dónde viene cada dato)   ║
║  [ ] Auditoría con system.access.audit                       ║
║                                                              ║
║  SEGURIDAD                                                   ║
║  ─────────────────────────────────────────────────────────  ║
║  [ ] IP allowlisting para recipients de Delta Sharing        ║
║  [ ] Rotación de tokens cada 90 días (Open Sharing)          ║
║  [ ] Service principals para jobs (no usuarios personales)   ║
║  [ ] Secrets en Databricks Secrets (no hardcoded)            ║
║  [ ] Private endpoints para acceso desde SAP on-premise      ║
║                                                              ║
║  DELTA SHARING / SAP                                         ║
║  ─────────────────────────────────────────────────────────  ║
║  [ ] Object Store habilitado en SAP Datasphere               ║
║  [ ] Tablas SAP en formato Parquet/Delta (no HANA nativo)    ║
║  [ ] Owner de conexión BDC = usuario individual (no grupo)   ║
║  [ ] Shares organizados por dominio (finance/, sales/, etc.) ║
║  [ ] Vistas enmascaradas para datos sensibles SAP            ║
║  [ ] Governance tags PersonalData propagados desde SAP       ║
║                                                              ║
║  ESCALABILIDAD                                               ║
║  ─────────────────────────────────────────────────────────  ║
║  [ ] Particionamiento por GJAHR+BUKRS (no por BELNR/BUDAT)  ║
║  [ ] Archivos entre 128MB y 1GB (evitar small files)         ║
║  [ ] Auto Loader para feeds SAP incrementales                ║
║  [ ] VACUUM con retención mínima 7 días (30 días ideal)      ║
║  [ ] Monitoreo de jobs con alertas en Databricks Workflows   ║
╚══════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.3 Errores más comunes en implementaciones reales SAP + Databricks

# COMMAND ----------

print("""
TOP 10 ERRORES MÁS COMUNES (y cómo evitarlos):

1. USAR ALL-PURPOSE CLUSTERS PARA JOBS PRODUCTIVOS
   Error: el pipeline de carga SAP corre en un cluster de $0.50/DBU
   Corrección: usar Jobs clusters ($0.10/DBU) — 5x más barato
   Impacto: puede multiplicar el costo por 5 sin beneficio alguno

2. PARTICIONAR POR COLUMNAS DE ALTA CARDINALIDAD SAP
   Error: partitionBy("BELNR") o partitionBy("BUDAT") en BKPF
   Corrección: partitionBy("GJAHR", "BUKRS") + Z-Order por BUDAT
   Impacto: millones de carpetas = queries 100x más lentas

3. NO HABILITAR CDF EN TABLAS MAESTRAS SAP
   Error: recargar KNA1/MARA completos cada vez
   Corrección: CDF + pipeline incremental = solo los cambios
   Impacto: de horas a minutos en cargas de datos maestros SAP

4. TOKENS DE DELTA SHARING HARDCODEADOS EN CÓDIGO
   Error: token = "dapi123..." directamente en el notebook
   Corrección: Databricks Secrets API + referencia segura
   Impacto: riesgo de seguridad crítico si el repo es público

5. USAR HIVE METASTORE LEGACY EN VEZ DE UNITY CATALOG
   Error: tablas en dbfs:/user/hive/warehouse sin UC
   Corrección: migrar a Unity Catalog desde el inicio
   Impacto: sin lineage, sin column-level security, sin Delta Sharing nativo

6. INTENTAR COMPARTIR TABLAS CON HISTORY A SAP BDC
   Error: CREATE SHARE ... ADD TABLE tabla_con_versiones
   Corrección: crear tabla snapshot sin history para el share
   Impacto: error en BDC Connect — limitación actual del conector

7. NO CONFIGURAR AUTO-STOP EN SQL WAREHOUSES
   Error: warehouse encendido el fin de semana sin uso
   Corrección: auto-stop a 10-15 minutos máximo
   Impacto: costos innecesarios de cientos de dólares

8. REESCRIBIR TABLAS COMPLETAS EN VEZ DE HACER MERGE
   Error: df.write.mode("overwrite") sobre tablas de millones de filas
   Corrección: MERGE INTO para actualizaciones incrementales
   Impacto: cargas de horas vs segundos, costos de storage duplicados

9. NO LIMPIAR ARCHIVOS DE AUTOLOADER DESPUÉS DEL PROCESAMIENTO
   Error: los archivos SAP procesados se acumulan en ADLS
   Corrección: configurar TTL o política de limpieza en el storage
   Impacto: costos de storage crecen indefinidamente

10. USAR PYTHON UDFs DONDE HAY FUNCIONES SQL NATIVAS
    Error: udf(lambda x: ...) para transformaciones simples
    Corrección: when(), regexp_replace(), to_date(), etc.
    Impacto: Photon no puede optimizar UDFs = 3-10x más lento
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.4 Hoja de ruta — De donde están a donde quieren llegar

# COMMAND ----------

spark.sql("""
    SELECT 
        'Fase 1 - Fundamentos (0-3 meses)'      AS fase,
        'Databricks Free Edition + datasets SAP' AS entorno,
        'Delta Lake, SQL, Optimización básica'   AS tecnologias,
        'Pipeline Bronze-Silver-Gold funcionando' AS entregable
    UNION ALL SELECT
        'Fase 2 - Delta Sharing (3-6 meses)',
        'Databricks Trial/Paid + Unity Catalog',
        'Delta Sharing, Unity Catalog governance',
        'Share de datos Gold con SAC vía token'
    UNION ALL SELECT
        'Fase 3 - Integración SAP (6-9 meses)',
        'Databricks + SAP BDC',
        'BDC Connect, SAP Datasphere, S/4HANA',
        'Pipeline en producción SAP → Databricks → SAC'
    UNION ALL SELECT
        'Fase 4 - AI & Analytics (9-12 meses)',
        'Full Databricks Platform',
        'ML, Genie, Joule, Agentes',
        'Forecasting, RFM, agentes sobre datos SAP'
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.5 Resumen final del curso — Todo lo construido

# COMMAND ----------

# Inventario completo de lo construido en el curso
print("=" * 70)
print("  RESUMEN COMPLETO DEL CURSO SAP + DATABRICKS")
print("=" * 70)

# Contar tablas por capa
capas = {
    "BRONZE" : [t for t in [r.tableName for r in spark.sql("SHOW TABLES IN sap_course").collect()] if "_bronze"  in t],
    "SILVER" : [t for t in [r.tableName for r in spark.sql("SHOW TABLES IN sap_course").collect()] if "_silver"  in t],
    "GOLD"   : [t for t in [r.tableName for r in spark.sql("SHOW TABLES IN sap_course").collect()] if "gold_"    in t],
    "OTHER"  : [t for t in [r.tableName for r in spark.sql("SHOW TABLES IN sap_course").collect()] if "_bronze" not in t and "_silver" not in t and "gold_" not in t],
}

total_filas = 0
for capa, tablas in capas.items():
    if not tablas:
        continue
    print(f"\n  [{capa}]")
    for tabla in sorted(tablas):
        try:
            n = spark.table(f"sap_course.{tabla}").count()
            total_filas += n
            print(f"    {tabla:<40} {n:>8,} filas")
        except:
            pass

print(f"\n  {'─'*60}")
print(f"  Total de registros en el Lakehouse SAP: {total_filas:>12,}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.6 Espacio abierto de preguntas
# MAGIC
# MAGIC ### Temas frecuentes de cierre:
# MAGIC
# MAGIC **¿Cuándo usar Lakeflow Pipelines vs Notebooks + Jobs?**
# MAGIC > Lakeflow (DLT) para pipelines declarativos con calidad de datos automática.
# MAGIC > Notebooks + Jobs para lógica compleja, ML o cuando necesitas control total.
# MAGIC
# MAGIC **¿Cómo manejar datos históricos de SAP (años anteriores)?**
# MAGIC > Carga histórica inicial: partitionBy("GJAHR") + OPTIMIZE + Z-Order.
# MAGIC > Luego CDC incremental con CDF o Auto Loader para cargas diarias.
# MAGIC
# MAGIC **¿Qué pasa si SAP Datasphere no está disponible?**
# MAGIC > Plan B: extraer tablas SAP via OData REST APIs o RFC con PyRFC directo a Databricks.
# MAGIC > El Delta Lake y el pipeline Medallion funcionan igual independientemente de la fuente.
# MAGIC
# MAGIC **¿Cómo empezar en producción sin interrumpir SAP?**
# MAGIC > Siempre read-only desde SAP. Databricks nunca escribe en SAP directamente.
# MAGIC > Delta Sharing a SAP (vuelta) solo si SAP BDC tiene el conector configurado.
# MAGIC
# MAGIC **¿Unity Catalog funciona con el Free Edition?**
# MAGIC > Sí, Free Edition incluye Unity Catalog con un metastore por workspace.
# MAGIC > Delta Sharing nativo como proveedor requiere trial o workspace pagado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del Módulo 8 y del Curso Completo
# MAGIC
# MAGIC ### Lo que dominan ahora:
# MAGIC
# MAGIC OK **Delta Lake**: ACID, Time Travel, CDF, Schema Evolution sobre datos SAP  
# MAGIC OK **Databricks SQL**: CTEs, Window Functions, Query Profile, Genie Space  
# MAGIC OK **Optimización**: DBUs, Liquid Clustering, Photon, caché, particionamiento  
# MAGIC OK **Medallion**: pipeline Bronze-Silver-Gold completo con 8 tablas SAP reales  
# MAGIC OK **Delta Sharing**: proveedor y consumidor, open sharing, Databricks-to-Databricks  
# MAGIC OK **SAP BDC**: conector, BDC Connect, flujo bidireccional, forecasting, RFM  
# MAGIC OK **Arquitectura**: checklist, errores comunes, hoja de ruta de 4 fases  
# MAGIC
# MAGIC ---
# MAGIC *"SAP is a goldmine for Data & AI. Databricks is the platform to unlock it."*  
# MAGIC *— Ali Ghodsi, CEO Databricks*
# MAGIC
# MAGIC **Repositorio del curso**: github.com/juancamiloblandon/databricks-sap-course
