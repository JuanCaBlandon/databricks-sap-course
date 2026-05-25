# 🏗️ Databricks + SAP — Curso de Especialización (30 horas)
### Delta Sharing, Arquitectura Lakehouse e Integración SAP Avanzada

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com)
[![SAP](https://img.shields.io/badge/SAP-0FAAFF?style=flat&logo=sap&logoColor=white)](https://sap.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat)](https://delta.io)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📋 Sobre el curso

Programa de **30 horas** (26h instrucción + 4h labs) diseñado para equipos con experiencia
en Databricks sobre Azure que necesitan alcanzar dominio real en arquitectura avanzada,
optimización y la integración especializada con el ecosistema SAP.

**Instructor**: Juan Camilo Blandón Urrea  
**Certificaciones**: Databricks Certified Data Engineer Associate · Databricks Certified Data Analyst Associate  
**Modalidad**: Presencial — sesiones de 2h, 2-3 veces por semana

---

## 🗂️ Estructura del repositorio

```
databricks-sap-course/
│
├── 📁 datasets/                          # Datos de muestra SAP (8 tablas)
│   ├── generate_sap_data.py              # Script generador de datos realistas
│   ├── BKPF.csv                          # Documentos contables (FI)
│   ├── BSEG.csv                          # Posiciones contables (FI)
│   ├── KNA1.csv                          # Maestro de clientes (SD)
│   ├── MARA.csv                          # Maestro de materiales (MM)
│   ├── VBAK.csv                          # Órdenes de venta (SD)
│   ├── VBAP.csv                          # Posiciones de venta (SD)
│   ├── LFA1.csv                          # Maestro de proveedores (MM)
│   └── EKKO.csv                          # Órdenes de compra (MM)
│
├── 📁 M1-Databricks-World/               # 2h | Contexto y ecosistema
│   └── M1-Databricks-World.py
│
├── 📁 M2-Delta-Lake/                     # 3.5h | El corazón del Lakehouse
│   └── M2-Delta-Lake.py
│
├── 📁 M3-Databricks-SQL/                 # 2.5h | Capa analítica enterprise
│   └── M3-Databricks-SQL.py
│
├── 📁 M4-Optimization-Cost-Control/      # 4h | Performance y costos
│   └── M4-Optimization-Cost-Control.py
│
├── 📁 M5-Ingestion-Medallion/            # 3h | Bronze → Silver → Gold
│   └── M5-Medallion-Architecture.py
│
├── 📁 M6-Delta-Sharing/                  # 4h | Compartir datos sin moverlos
│   └── M6-Delta-Sharing.py
│
├── 📁 M7-SAP-Databricks-Integration/     # 5h | El módulo estrella
│   └── M7-SAP-Databricks-Integration.py
│
└── 📁 M8-Reference-Architecture/         # 2h | Arquitectura y hoja de ruta
    └── M8-Reference-Architecture.py
```

---

## 🧰 Datasets SAP incluidos

| Tabla | Módulo SAP | Descripción | Registros |
|---|---|---|---|
| `BKPF` | FI — Finanzas | Cabeceras de documentos contables | 5,000 |
| `BSEG` | FI — Finanzas | Posiciones de documentos contables | ~7,400 |
| `KNA1` | SD — Ventas | Maestro de clientes | 500 |
| `MARA` | MM — Materiales | Maestro de materiales | 300 |
| `VBAK` | SD — Ventas | Cabeceras de órdenes de venta | 2,000 |
| `VBAP` | SD — Ventas | Posiciones de órdenes de venta | ~4,500 |
| `LFA1` | MM — Compras | Maestro de proveedores | 200 |
| `EKKO` | MM — Compras | Cabeceras de órdenes de compra | 1,000 |

> Los datos son **generados aleatoriamente** con estructura idéntica a SAP real.
> Seguros para uso en cualquier entorno (no contienen información real).

---

## 🚀 Cómo empezar

### Paso 1: Registrarse en Databricks Free Edition
👉 [signup.databricks.com](https://signup.databricks.com)  
Sin costo, sin tarjeta de crédito, no expira.

### Paso 2: Generar los datasets SAP
```bash
cd datasets/
python3 generate_sap_data.py
```

### Paso 3: Subir los CSV a Databricks
En la UI de Databricks: **+ New → Add data → Upload files**  
Ruta destino: `/FileStore/sap_course/datasets/`

### Paso 4: Importar los notebooks
**File → Import → URL** o arrastra el archivo `.py` a la UI.

### Paso 5: Ejecutar en orden
Empezar por `M1-Databricks-World.py` y seguir en orden numérico.

---

## 📚 Temario detallado

| Módulo | Tema | Horas | Entorno |
|---|---|---|---|
| M1 | El mundo Databricks: historia, Lakehouse, ecosistema | 2h | Free Edition |
| M2 | Delta Lake: ACID, Time Travel, CDF, Schema Evolution | 3.5h | Free Edition |
| M3 | Databricks SQL: warehouses, Genie AI/BI, dashboards | 2.5h | Free Edition |
| M4 | Optimización y costos: Photon, Liquid Clustering, caché | 4h | Free Edition |
| M5 | Ingesta y Medallion: Auto Loader, Lakeflow, Jobs | 3h | Free Edition |
| **M6** | **Delta Sharing: shares, recipients, multi-cloud** | **4h** | **Trial 14 días** |
| **M7** | **SAP + Databricks: BDC, SAC, Datasphere, S/4HANA** | **5h** | **Trial + SAP BDC** |
| M8 | Arquitectura de referencia y hoja de ruta | 2h | Trial |
| | **Total** | **26h + 4h labs** | |

---

## 🏛️ Arquitectura Medallion del curso

```
 CSV SAP  ──► BRONZE ──► SILVER ──► GOLD ──► Delta Sharing ──► SAC
                                      │
                                      └──► ML Models ──► Forecasting
                                                   └──► RFM Segments
```

**Tablas Gold construidas en el curso:**
- `gold_fin_summary` — KPIs financieros por compañía/año
- `gold_sales_kpis` — Métricas de ventas mensuales
- `gold_customer_360` — Vista 360° del cliente con tier
- `gold_rfm_segmentation` — Segmentación RFM para marketing
- `gold_forecast_features` — Features de serie de tiempo para ML

---

## 🔗 Recursos adicionales

| Recurso | Enlace |
|---|---|
| Databricks Documentation | [docs.databricks.com](https://docs.databricks.com) |
| Delta Lake | [delta.io](https://delta.io) |
| Delta Sharing | [delta.io/sharing](https://delta.io/sharing) |
| SAP BDC Connector docs | [docs.databricks.com/delta-sharing/sap-bdc](https://docs.databricks.com/aws/en/delta-sharing/sap-bdc/) |
| SAP BDC Trial | [sap.com/products/data-cloud/trial](https://www.sap.com/products/data-cloud/trial.html) |
| Databricks Academy | [customer-academy.databricks.com](https://customer-academy.databricks.com) |
| Databricks Free Edition | [signup.databricks.com](https://signup.databricks.com) |

---

## 👨‍💻 Instructor

**Juan Camilo Blandón Urrea** — Senior Data Engineer  
🏅 Databricks Certified Data Engineer Associate (2024)  
🏅 Databricks Certified Data Analyst Associate (2025)  
📍 Rionegro, Antioquia, Colombia  
🔗 [linkedin.com/in/juancamiloblandon](https://linkedin.com/in/juancamiloblandon)

---

## 📄 Licencia

MIT License — libre para uso educativo y personal.

---

*"SAP is a goldmine for Data & AI. Databricks is the platform to unlock it."*  
*— Ali Ghodsi, CEO Databricks*
