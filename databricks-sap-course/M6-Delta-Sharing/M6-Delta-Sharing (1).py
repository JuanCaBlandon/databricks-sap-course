# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 6: Delta Sharing — Compartir Datos Sin Moverlos
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC - Entender por qué Delta Sharing cambia las reglas del juego
# MAGIC - Conocer la arquitectura: Share, Schema, Table, Recipient
# MAGIC - Configurar Delta Sharing como proveedor (outbound)
# MAGIC - Consumir un share como recipient (inbound)
# MAGIC - Implementar seguridad, auditoría y patrones multi-cloud
# MAGIC - Preparar el terreno para la integración SAP del Módulo 7
# MAGIC
# MAGIC ---
# MAGIC ## 📅 Este módulo se trabaja en DOS clases
# MAGIC
# MAGIC | Clase | Contenido | Secciones |
# MAGIC |---|---|---|
# MAGIC | **Jueves (C8)** — El lado del PROVIDER | Teoría, arquitectura, tipos de recipients, crear el share completo en el Trial | 6.1 — 6.4 + Lab Parte 1 |
# MAGIC | **Viernes (C9)** — El lado del RECIPIENT | Consumir el share, Time Travel y CDF cross-workspace, costos, auditoría, buenas prácticas | 6.5 — 6.10 + Lab Parte 2 |
# MAGIC
# MAGIC ---
# MAGIC ## 6.1 ¿Qué es Delta Sharing y por qué cambia las reglas?
# MAGIC
# MAGIC ### El problema que resuelve
# MAGIC
# MAGIC ```
# MAGIC ANTES (sin Delta Sharing):
# MAGIC   SAP → exportar CSV → enviar por email/SFTP → importar → duplicar datos
# MAGIC   Resultado: datos desactualizados, sin governance, storage duplicado
# MAGIC
# MAGIC CON Delta Sharing:
# MAGIC   SAP → Delta Lake → Share → Recipient accede en tiempo real
# MAGIC   Resultado: datos frescos, governance centralizado, zero-copy
# MAGIC ```
# MAGIC
# MAGIC ### Características clave
# MAGIC - **Open protocol**: estándar REST, no requiere Databricks en el lado consumidor
# MAGIC - **Zero-copy**: los datos no se mueven ni replican
# MAGIC - **Cross-cloud**: compartir entre AWS, Azure y GCP sin restricciones
# MAGIC - **Governance**: Unity Catalog controla quién ve qué con auditoría completa
# MAGIC - **Bidireccional**: SAP puede enviar datos a Databricks Y recibirlos de vuelta

# COMMAND ----------

CATALOG = "laboratory_sap_dev"
SCHEMA  = "sap_course"
spark.sql(f"USE {CATALOG}.{SCHEMA}")
print("Módulo 6: Delta Sharing")
print("NOTA: Las operaciones de CREATE SHARE requieren el trial de 14 días")
print("      con Unity Catalog habilitado y privilegios CREATE PROVIDER/RECIPIENT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Arquitectura del protocolo Delta Sharing
# MAGIC
# MAGIC ```
# MAGIC PROVEEDOR (Databricks con Unity Catalog)
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │  SHARE: sap_analytics_share                            │
# MAGIC │  ┌──────────────────────────────────────────────────┐  │
# MAGIC │  │  SCHEMA: finance                                  │  │
# MAGIC │  │    TABLE: gold_fin_summary      (read-only)       │  │
# MAGIC │  │    TABLE: gold_sales_kpis       (read-only)       │  │
# MAGIC │  │  SCHEMA: customers                                │  │
# MAGIC │  │    TABLE: gold_customer_360     (read-only)       │  │
# MAGIC │  └──────────────────────────────────────────────────┘  │
# MAGIC │                                                         │
# MAGIC │  RECIPIENTS:                                            │
# MAGIC │    - sap_analytics_cloud   (token, open sharing)        │
# MAGIC │    - selecta_workspace     (Databricks-to-Databricks)   │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC                           │ REST API
# MAGIC                           ▼ (zero-copy, datos no se mueven)
# MAGIC CONSUMIDOR (SAC, otro Databricks, Python client, etc.)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Delta Sharing como PROVEEDOR — Configuración completa
# MAGIC
# MAGIC ### Prerrequisitos (verificar antes del lab):
# MAGIC 1. Workspace con Unity Catalog habilitado
# MAGIC 2. Ser workspace admin O tener privilegios CREATE SHARE
# MAGIC 3. Las tablas Gold del Módulo 5 creadas

# COMMAND ----------

# Verificar que las tablas Gold están listas para compartir
print("=== Verificando tablas Gold para Delta Sharing ===")
tablas_gold = {
    "gold_fin_summary"      : "KPIs financieros SAP por sociedad/ejercicio",
    "gold_sales_kpis"       : "Métricas de ventas mensuales SAP",
    "gold_customer_360"     : "Vista 360 del cliente SAP con tier y LTV",
    "vbak_gold"             : "Órdenes de venta en español (Genie Space)",
}

for tabla, desc in tablas_gold.items():
    try:
        n = spark.table(f"{CATALOG}.{SCHEMA}.{tabla}").count()
        print(f"  OK  sap_course.{tabla:<30} {n:>8,} filas")
    except:
        print(f"  ERR sap_course.{tabla} — ejecutar Módulo 5 primero")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comandos para configurar el Share (ejecutar en SQL Editor con Unity Catalog)
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Crear el share principal para SAP Analytics Cloud
# MAGIC CREATE SHARE IF NOT EXISTS sap_analytics_share
# MAGIC   COMMENT 'Share de datos Gold SAP para SAC y partners externos';
# MAGIC
# MAGIC -- 2. Agregar tablas al share con alias de negocio
# MAGIC ALTER SHARE sap_analytics_share
# MAGIC   ADD TABLE sap_course.gold_fin_summary
# MAGIC   AS finance.financial_summary;
# MAGIC
# MAGIC ALTER SHARE sap_analytics_share
# MAGIC   ADD TABLE sap_course.gold_sales_kpis
# MAGIC   AS sales.monthly_kpis;
# MAGIC
# MAGIC ALTER SHARE sap_analytics_share
# MAGIC   ADD TABLE sap_course.gold_customer_360
# MAGIC   AS customers.customer_360;
# MAGIC
# MAGIC -- 3. Verificar el share creado
# MAGIC SHOW ALL IN SHARE sap_analytics_share;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 Tipos de Recipients — Open Sharing vs Databricks-to-Databricks

# COMMAND ----------

print("""
TIPO 1: Open Sharing (para SAC, Python client, cualquier sistema)
─────────────────────────────────────────────────────────────────
  - El recipient NO necesita cuenta Databricks
  - Se genera un token de activación (archivo credential.share)
  - Se comparte el token de forma segura con el consumidor
  - Compatible con SAP Analytics Cloud, SAP Datasphere, Power BI, Python

  SQL:
  CREATE RECIPIENT sap_analytics_cloud
    COMMENT 'SAP Analytics Cloud tenant del cliente';
  
  -- Obtener el token de activación (link de un solo uso)
  DESCRIBE RECIPIENT sap_analytics_cloud;


TIPO 2: Databricks-to-Databricks (para otro workspace Databricks)
──────────────────────────────────────────────────────────────────
  - El recipient SÍ necesita cuenta Databricks
  - Más seguro: autenticación via metastore ID de Unity Catalog
  - Sin tokens manuales — se renueva automáticamente
  - Ideal para compartir entre workspaces del mismo cliente

  SQL:
  CREATE RECIPIENT workspace_selecta
    USING ID '<metastore_id_del_workspace_destino>'
    COMMENT 'Workspace Databricks del equipo de analytics de Selecta';


TIPO 3: SAP BDC Connect (específico para SAP BDC)
──────────────────────────────────────────────────
  - Flujo especial con mTLS + OIDC (sin tokens manuales)
  - El recipient y provider se crean AUTOMÁTICAMENTE al conectar
  - Ver Módulo 7 para el paso a paso completo
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comandos para crear Recipients (ejecutar en SQL Editor)
# MAGIC
# MAGIC ```sql
# MAGIC -- RECIPIENT tipo Open Sharing (para SAC)
# MAGIC CREATE RECIPIENT IF NOT EXISTS sap_analytics_cloud
# MAGIC   COMMENT 'SAP Analytics Cloud - open sharing';
# MAGIC
# MAGIC -- Obtener link de activación (válido una sola vez, compartir de forma segura)
# MAGIC DESCRIBE RECIPIENT sap_analytics_cloud;
# MAGIC
# MAGIC -- RECIPIENT tipo Databricks-to-Databricks
# MAGIC -- Obtener el metastore sharing identifier del workspace destino:
# MAGIC -- Account Console → Unity Catalog → tu_metastore → Delta Sharing
# MAGIC CREATE RECIPIENT IF NOT EXISTS workspace_analytics
# MAGIC   USING ID 'aws:us-east-1:abc123-def456-...'
# MAGIC   COMMENT 'Workspace de analytics del cliente';
# MAGIC
# MAGIC -- Otorgar acceso al share
# MAGIC GRANT SELECT ON SHARE sap_analytics_share
# MAGIC   TO RECIPIENT sap_analytics_cloud;
# MAGIC
# MAGIC GRANT SELECT ON SHARE sap_analytics_share
# MAGIC   TO RECIPIENT workspace_analytics;
# MAGIC
# MAGIC -- Verificar permisos
# MAGIC SHOW GRANTS ON SHARE sap_analytics_share;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # ════════ FIN CLASE 8 (jueves) · INICIO CLASE 9 (viernes) ════════
# MAGIC **Recap C8**: qué es Delta Sharing, arquitectura Share/Recipient, tipos de sharing,
# MAGIC y el share `curso_sap_share` creado en el Trial con `WITH HISTORY`.
# MAGIC **Hoy C9**: el lado del recipient — consumir, Time Travel, CDF, costos y producción.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.5 Delta Sharing como CONSUMIDOR — Leer shares externos

# COMMAND ----------

# MAGIC %md
# MAGIC ### Caso A: Consumir con archivo de credenciales (Open Sharing)
# MAGIC
# MAGIC ```python
# MAGIC # El archivo .share contiene la URL del servidor y el token
# MAGIC # Lo recibes del proveedor (en nuestro caso, del admin SAP BDC)
# MAGIC
# MAGIC # Cargar el share con el archivo de credenciales
# MAGIC share_name  = "sap_analytics_share"
# MAGIC profile     = "/Volumes/laboratory_sap_dev/bronze/curso_databricks/credentials/sap_bdc.share"
# MAGIC
# MAGIC # Listar tablas disponibles en el share
# MAGIC df_shares = (spark.read
# MAGIC     .format("deltaSharing")
# MAGIC     .option("profile", profile)
# MAGIC     .load(f"{share_name}.finance.financial_summary"))
# MAGIC
# MAGIC df_shares.show()
# MAGIC ```
# MAGIC
# MAGIC ### Caso B: Consumir via Unity Catalog (Databricks-to-Databricks)
# MAGIC
# MAGIC ```sql
# MAGIC -- El admin del workspace consumidor ejecuta esto:
# MAGIC
# MAGIC -- 1. Crear un catalog que apunte al share del proveedor
# MAGIC CREATE CATALOG IF NOT EXISTS sap_shared_data
# MAGIC   USING SHARE <metastore_proveedor>.sap_analytics_share;
# MAGIC
# MAGIC -- 2. Ya puedes consultar como si fueran tablas locales
# MAGIC SELECT * FROM sap_shared_data.finance.financial_summary;
# MAGIC SELECT * FROM sap_shared_data.sales.monthly_kpis;
# MAGIC SELECT * FROM sap_shared_data.customers.customer_360;
# MAGIC
# MAGIC -- Los datos vienen del proveedor en tiempo real, sin copiar nada
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.6 Control de acceso granular y auditoría

# COMMAND ----------

# MAGIC %md
# MAGIC ### Control de acceso granular
# MAGIC
# MAGIC ```sql
# MAGIC -- Compartir solo columnas específicas (sin datos sensibles)
# MAGIC -- Útil cuando las tablas SAP tienen campos de PII (nombre, dirección)
# MAGIC ALTER SHARE sap_analytics_share
# MAGIC   ADD TABLE laboratory_sap_dev.sap_course.kna1_bronze
# MAGIC   PARTITION (LAND1 = 'CO')              -- Solo clientes Colombia
# MAGIC   AS customers.colombia_customers;
# MAGIC
# MAGIC -- Crear vista para enmascarar columnas sensibles
# MAGIC CREATE VIEW laboratory_sap_dev.sap_course.vw_customer_masked AS
# MAGIC   SELECT
# MAGIC     KUNNR,
# MAGIC     SUBSTR(NAME1, 1, 3) || '***'    AS nombre_masked,
# MAGIC     LAND1,
# MAGIC     ORT01
# MAGIC   FROM laboratory_sap_dev.sap_course.kna1_bronze;
# MAGIC
# MAGIC -- Compartir la vista enmascarada en vez de la tabla original
# MAGIC ALTER SHARE sap_analytics_share
# MAGIC   ADD TABLE laboratory_sap_dev.sap_course.vw_customer_masked
# MAGIC   AS customers.customers_masked;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auditoría — quién accedió a qué datos SAP
# MAGIC
# MAGIC ```sql
# MAGIC -- Ver eventos de acceso a los shares (requiere Unity Catalog)
# MAGIC SELECT
# MAGIC     event_time,
# MAGIC     user_identity.email        AS usuario,
# MAGIC     request_params.share_name  AS share,
# MAGIC     request_params.schema_name AS schema,
# MAGIC     request_params.table_name  AS tabla,
# MAGIC     response.status_code       AS estado
# MAGIC FROM system.access.audit
# MAGIC WHERE event_type = 'deltaSharingQueryTable'
# MAGIC   AND event_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
# MAGIC ORDER BY event_time DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.7 Patrones multi-cloud y consideraciones de producción

# COMMAND ----------

print("""
PATRONES MULTI-CLOUD con Delta Sharing:

1. SAP RISE en Azure → Databricks en AWS
   ─────────────────────────────────────
   SAP BDC (Azure) → BDC Connect → Delta Sharing → Databricks (AWS)
   Cross-cloud sharing soportado desde noviembre 2025.
   Latencia adicional: ~50-100ms extra vs mismo cloud.
   Costo: egress de datos del cloud origen (usualmente centavos por GB).

2. Databricks (AWS) → SAP Analytics Cloud (cualquier cloud)
   ──────────────────────────────────────────────────────────
   Open Sharing con token → SAC consume via REST.
   SAC no necesita estar en el mismo cloud.

3. Multi-región dentro del mismo cloud
   ─────────────────────────────────────
   Proveedor en us-east-1 → Recipient en eu-west-1.
   Delta Sharing maneja la transferencia automáticamente.
   Misma API, misma configuración.


LIMITACIONES REALES DE PRODUCCIÓN (para el M8):

  NO soportado hoy:
    - Tablas con HISTORY (time travel) al compartir a SAP BDC
    - Z-tables (tablas custom) de SAP en BDC Connect (en roadmap)
    - El owner de la conexión BDC debe ser usuario individual (no SP)
    - Streaming sobre Delta Sharing aún en preview

  Considerar siempre:
    - Rotación de tokens cada 90 días (Open Sharing)
    - IP allowlisting para recipients en redes corporativas
    - Las tablas shared son READ-ONLY desde el lado del recipient
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.8 Lab — Configurar un share completo end-to-end
# MAGIC
# MAGIC ### Parte 1: Proveedor (en el workspace trial de 14 días)

# COMMAND ----------

# Preparar tablas para el lab de Delta Sharing
# Verificar que todas las tablas Gold están disponibles
print("=== Verificación previa al lab de Delta Sharing ===\n")

tablas_requeridas = [
    ("laboratory_sap_dev.sap_course.gold_fin_summary",  "Share finance"),
    ("laboratory_sap_dev.sap_course.gold_sales_kpis",   "Share sales"),
    ("laboratory_sap_dev.sap_course.gold_customer_360", "Share customers"),
    ("laboratory_sap_dev.sap_course.vbak_gold",         "Share comercial (la del Genie)"),
]

todas_ok = True
for tabla, desc in tablas_requeridas:
    try:
        n = spark.table(tabla).count()
        print(f"  OK  {tabla:<40} {n:>8,} filas  — {desc}")
    except:
        print(f"  ERR {tabla:<40}  FALTA — ejecutar módulos anteriores")
        todas_ok = False

if todas_ok:
    print("\nTodas las tablas están listas. Puedes proceder con el lab.")
else:
    print("\nAlgunas tablas faltan. Ejecutar los módulos anteriores primero.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Secuencia completa del lab (ejecutar en SQL Editor con UC habilitado):
# MAGIC
# MAGIC ```sql
# MAGIC -- PASO 1: Crear el share
# MAGIC CREATE SHARE IF NOT EXISTS curso_sap_share
# MAGIC   COMMENT 'Share del curso SAP + Databricks';
# MAGIC
# MAGIC -- PASO 2: Agregar tablas
# MAGIC ALTER SHARE curso_sap_share ADD TABLE laboratory_sap_dev.sap_course.gold_fin_summary;
# MAGIC ALTER SHARE curso_sap_share ADD TABLE laboratory_sap_dev.sap_course.gold_sales_kpis;
# MAGIC ALTER SHARE curso_sap_share ADD TABLE laboratory_sap_dev.sap_course.gold_customer_360;
# MAGIC ALTER SHARE curso_sap_share ADD TABLE laboratory_sap_dev.sap_course.vbak_gold;
# MAGIC
# MAGIC -- PASO 2b: compartir vbak_gold CON HISTORIA Y CDF (la versión completa)
# MAGIC -- Primero la tabla debe tener Change Data Feed habilitado:
# MAGIC ALTER TABLE laboratory_sap_dev.sap_course.vbak_gold
# MAGIC   SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- Quitar y volver a agregar con history + CDF:
# MAGIC ALTER SHARE curso_sap_share REMOVE TABLE laboratory_sap_dev.sap_course.vbak_gold;
# MAGIC ALTER SHARE curso_sap_share
# MAGIC   ADD TABLE laboratory_sap_dev.sap_course.vbak_gold
# MAGIC   WITH HISTORY;
# MAGIC -- WITH HISTORY habilita: Time Travel + Streaming + CDF en el recipient
# MAGIC
# MAGIC -- PASO 3: Crear recipient (Open Sharing para simular SAC)
# MAGIC CREATE RECIPIENT IF NOT EXISTS recipient_sac_demo
# MAGIC   COMMENT 'Recipient demo que simula SAP Analytics Cloud';
# MAGIC
# MAGIC -- PASO 4: Otorgar acceso
# MAGIC GRANT SELECT ON SHARE curso_sap_share TO RECIPIENT recipient_sac_demo;
# MAGIC
# MAGIC -- PASO 5: Obtener token de activación
# MAGIC -- Guarda el link que aparece — es de un solo uso
# MAGIC DESCRIBE RECIPIENT recipient_sac_demo;
# MAGIC
# MAGIC -- PASO 6: Verificar el share
# MAGIC SHOW ALL IN SHARE curso_sap_share;
# MAGIC SHOW GRANTS ON SHARE curso_sap_share;
# MAGIC
# MAGIC -- PASO 7: Consumir el share (desde otro workspace o con el cliente Python)
# MAGIC -- Si tienes un segundo workspace:
# MAGIC CREATE CATALOG IF NOT EXISTS datos_sap_compartidos
# MAGIC   USING SHARE <tu_metastore_id>.curso_sap_share;
# MAGIC
# MAGIC SELECT * FROM datos_sap_compartidos.sap_course.gold_sales_kpis LIMIT 10;
# MAGIC
# MAGIC -- PASO 8: EL MOMENTO DEL LAB — datos en vivo, zero-copy
# MAGIC -- En el PROVIDER (Trial): modificar un registro
# MAGIC UPDATE laboratory_sap_dev.sap_course.vbak_gold
# MAGIC   SET valor_neto = valor_neto * 2
# MAGIC   WHERE numero_orden = (SELECT MIN(numero_orden) FROM laboratory_sap_dev.sap_course.vbak_gold);
# MAGIC
# MAGIC -- En el RECIPIENT (Free): consultar inmediatamente
# MAGIC SELECT numero_orden, valor_neto
# MAGIC FROM datos_sap_compartidos.sap_course.vbak_gold
# MAGIC ORDER BY numero_orden LIMIT 1;
# MAGIC -- El cambio aparece AL INSTANTE — sin sync, sin ETL, sin copia. Eso es Delta Sharing.
# MAGIC
# MAGIC -- PASO 9: TIME TRAVEL sobre la tabla compartida (gracias a WITH HISTORY)
# MAGIC -- En el RECIPIENT:
# MAGIC SELECT * FROM datos_sap_compartidos.sap_course.vbak_gold VERSION AS OF 0 LIMIT 5;
# MAGIC -- Comparar con la versión actual — el recipient ve el historial completo
# MAGIC
# MAGIC -- PASO 10: IDENTIFICAR DATA NUEVA con CDF cross-workspace
# MAGIC -- En el RECIPIENT (Python):
# MAGIC -- cambios = (spark.read
# MAGIC --     .option("readChangeFeed", "true")
# MAGIC --     .option("startingVersion", ultima_version_procesada)
# MAGIC --     .table("datos_sap_compartidos.sap_course.vbak_gold")
# MAGIC --     .filter("_change_type IN ('insert', 'update_postimage')"))
# MAGIC -- El mismo patrón CDF+MERGE del Módulo 3 — ahora ENTRE workspaces/empresas
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.9 Delta Sharing OSS — Sin necesidad de Databricks
# MAGIC
# MAGIC El servidor Open Source de Delta Sharing permite compartir datos Delta
# MAGIC sin necesidad de licencia Databricks. Útil para Free Edition o on-premise.

# COMMAND ----------

print("""
Delta Sharing Open Source Server:
  Repositorio: https://github.com/delta-io/delta-sharing
  
  Levantar el servidor localmente (Docker):
  
  docker run -p 8080:8080 \\
    -v /ruta/a/config.yaml:/config/delta-sharing-server-config.yaml \\
    deltaio/delta-sharing-server

  Archivo config.yaml:
  ─────────────────────
  shares:
    - name: sap_demo_share
      schemas:
        - name: gold
          tables:
            - name: sales_kpis
              location: /dbfs/delta/gold_sales_kpis
              id: tabla-001

  Consumir con Python (cualquier entorno, sin Databricks):
  ─────────────────────────────────────────────────────────
  import delta_sharing
  
  client = delta_sharing.SharingClient("profile.json")
  tables = client.list_all_tables()
  
  df = delta_sharing.load_as_pandas("profile.json#sap_demo_share.gold.sales_kpis")
  print(df.head())
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.10 Buenas prácticas de producción + costos
# MAGIC
# MAGIC ### ✅ Checklist del provider (lo que Summa haría como publicador)
# MAGIC
# MAGIC 1. **Solo Gold**: nunca compartir Bronze/Silver — datos de negocio con nombres claros
# MAGIC 2. **WITH HISTORY + CDF** en tablas que el recipient necesita sincronizar incrementalmente
# MAGIC 3. **Vistas enmascaradas** para PII (NAME1, direcciones) — compartir la vista, no la tabla
# MAGIC 4. **Particiones en el share** cuando el recipient solo necesita un subconjunto (LAND1='CO')
# MAGIC 5. **Un share por dominio de negocio** (comercial, financiero) — no un mega-share de todo
# MAGIC 6. **Rotar tokens cada 90 días** en Open Sharing · preferir D2D cuando ambos son Databricks
# MAGIC 7. **Auditar mensualmente**: system.access.audit con event_type deltaSharing*
# MAGIC
# MAGIC ### 💰 Quién paga qué en Delta Sharing
# MAGIC
# MAGIC | Concepto | Quién paga | Nota |
# MAGIC |---|---|---|
# MAGIC | El share (la feature) | Nadie | Compartir es gratis |
# MAGIC | Compute de las queries | El **recipient** | Su propio warehouse/cluster |
# MAGIC | Egress misma región | $0 | El caso Summa: ambos workspaces en la misma región |
# MAGIC | Egress cross-region/cloud | El **provider** | Centavos/GB — pero suma con volumen |
# MAGIC
# MAGIC ### 💡 El tip de oro: CDF también controla el egress
# MAGIC Si el recipient está en otra región/cloud, en vez de consultar la tabla remota cada vez
# MAGIC (egress por query), mantiene una **réplica local que refresca solo con los cambios del CDF**.
# MAGIC El egress se limita a los deltas — no a releer toda la tabla. CDF no es solo para
# MAGIC identificar data nueva: es la estrategia de control de costos cross-region.
# MAGIC
# MAGIC ### 📦 Qué metadatos viajan con el share
# MAGIC - ✅ Schema y tipos de columnas — siempre
# MAGIC - ✅ Comments de tablas y columnas (los que hicieron en español) — en D2D
# MAGIC - ❌ TBLPROPERTIES, constraints, permisos internos — el recipient solo recibe lectura

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del Módulo 6
# MAGIC
# MAGIC OK Delta Sharing: protocolo abierto para compartir datos sin moverlos  
# MAGIC OK Arquitectura: Share contiene Schemas/Tables, Recipients consumen con permisos  
# MAGIC OK Como proveedor: CREATE SHARE, ADD TABLE, CREATE RECIPIENT, GRANT  
# MAGIC OK Como consumidor: Open Sharing con token o Databricks-to-Databricks con metastore ID  
# MAGIC OK Control granular: particiones, vistas enmascaradas, auditoría con system.access.audit  
# MAGIC OK Multi-cloud: AWS, Azure, GCP — cross-cloud soportado desde nov 2025  
# MAGIC OK Limitaciones reales: sin history sharing a BDC, tokens cada 90 días  
# MAGIC OK Delta Sharing OSS: disponible sin Databricks para Free Edition  
# MAGIC OK WITH HISTORY: Time Travel + Streaming + CDF en el recipient  
# MAGIC OK CDF cross-workspace: identificar data nueva Y controlar egress  
# MAGIC OK Costos: compartir gratis, recipient paga su compute, egress solo cross-region  
# MAGIC
# MAGIC Proximo modulo: SAP + Databricks — usar todo esto para la integracion real con SAP BDC

