<div align="center">

# Alessandro Yusty Ceballos - 2240248

# REPORTE TECNICO: https://docs.google.com/document/d/1GFHqWCPsaghh6-7mqp65LV4q3xEYetbzTpcHYNU1mDM/edit?usp=sharing

# 📈 ETL Forex Pipeline

**Pipeline ETL end-to-end para datos del mercado forex con orquestación Airflow, streaming Kafka y dashboards en tiempo real.**

</div>

---

## 🎯 ¿Qué hace este proyecto?

Extrae datos de tres fuentes de divisas (**Yahoo Finance**, **Finnhub** WebSocket, **Alpha Vantage**), los valida con **Great Expectations**, los carga en un **modelo dimensional sobre PostgreSQL**, y los expone vía dos dashboards complementarios:

- 📊 **Metabase** para análisis histórico sobre el data warehouse.
- ⚡ **Streamlit + Kafka** para visualización en tiempo real.

Todo se levanta con **un solo comando**: `docker compose up -d`.

> **Caso de uso:** análisis de comportamiento de divisas como insumo para modelos predictivos de variación cambiaria, alineado al **ODS 1 – No pobreza** (apps de remesas, microcrédito, hedging para poblaciones vulnerables).

---

## 🏗 Arquitectura

```
   Yahoo Finance ──┐
   Finnhub WS ─────┼──► Airflow DAG ──► PostgreSQL ──┬──► Metabase (analítico)
   Alpha Vantage ──┘   (extract,        (modelo      │
                        transform,      dimensional) └──► Kafka Producer
                        validate GX,                       │
                        load)                          quotes_stream
                          │                                │
                          ▼                                ▼
                  quarantine_quotes                  Streamlit (real-time)
```

> 📄 Documento técnico: [`docs/technical_report.md`](docs/technical_report.md)
> 📑 Informe ejecutivo: [`docs/informe_entrega_final.pdf`](docs/informe_entrega_final.pdf)
> 📊 Dashboard estático: [`docs/Metabase - Dashboard estatico.pdf`](docs/Metabase%20-%20Dashboard%20estatico.pdf)

### Stack

| Capa | Tecnología |
|---|---|
| **Orquestación** | Apache Airflow 2.9 |
| **Data Warehouse** | PostgreSQL 15 (esquema en estrella) |
| **Streaming** | Apache Kafka 7.6 (Confluent) |
| **Validación** | Great Expectations 1.x+ (API moderna + GX Cloud opcional) |
| **BI analítico** | Metabase |
| **Real-time dashboard** | Streamlit + Plotly |
| **Infraestructura** | Docker Compose (9 servicios) |

---

## 🚀 Quick Start

### Requisitos

- Docker Desktop con WSL2 (Windows) o nativo (Linux/Mac)
- 6 GB de RAM asignados a Docker
- Puertos libres: `8080`, `8501`, `3000`, `5433`, `9092`

### Instalación

```bash
# Clonar el repositorio
git clone https://github.com/AlessandroYustyCeballos/FinalDeliveryETL.git
cd FinalDeliveryETL/ETL-Proyect-main

# Levantar todo el stack
docker compose up -d
```

⏱ La primera vez tarda ~2 minutos (descarga de imágenes). Las siguientes, ~20 segundos.

### Inicializar el pipeline

```bash
# Generar los CSVs base (yahoo.csv y finhub.csv)
docker compose exec airflow-scheduler python /opt/airflow/preparar.py
# → Inicializar?: escribir 1
# → Escoger divisa: escribir índice (ej. 0 = EURUSD)
# → Ctrl+C después de ~30s para detener Finnhub
```

### Acceder a las UIs

| Servicio | URL | Credenciales |
|---|---|---|
| 🌬 Airflow | http://localhost:8080 | `admin` / `admin` |
| 📊 Metabase | http://localhost:3000 | Configurar en primer arranque |
| ⚡ Streamlit | http://localhost:8501 | Sin auth |

En Airflow, activa el DAG `ETL_Delivery3` con el toggle de la izquierda. Se ejecutará cada 2 minutos.

### Configurar Metabase (primera vez)

1. Crear cuenta admin en el setup wizard.
2. **Add data → PostgreSQL**:
   - Host: `postgres` · Port: `5432` · DB: `trading_db`
   - User: `etl_user` · Pass: `etl_pass`

### (Opcional) Integración con GX Cloud

Si tienes una cuenta de [Great Expectations Cloud](https://greatexpectations.io/cloud), crea un archivo `.env` en la raíz con tus credenciales:

```bash
GX_CLOUD_ACCESS_TOKEN=tu_token
GX_CLOUD_ORGANIZATION_ID=tu_org_id
GX_CLOUD_WORKSPACE_ID=tu_workspace_id
```

Sin estas variables, la validación corre en modo **ephemeral local** (igualmente funciona). Con ellas, cada validación publica un `result_url` en GX Cloud con el reporte visual.

---

## 📁 Estructura

```
FinalDeliveryETL/
└── ETL-Proyect-main/
    ├── app/                          # Aplicación Python
    │   ├── producer.py               # Kafka producer (fact_quotes → topic)
    │   ├── consumer.py               # Consumer standalone (debug)
    │   └── dashboard.py              # Streamlit real-time
    ├── dags/
    │   └── ETL.py                    # DAG ETL_Delivery3
    ├── sql/                          # Init scripts de Postgres
    │   ├── 00_create_metabase_db.sql
    │   └── 01_init_schema.sql        # Modelo dimensional
    ├── docs/
    │   ├── technical_report.md       # Documento técnico
    │   ├── informe_entrega_final.pdf # Informe ejecutivo (auto-generado)
    │   ├── generate_report.py        # Generador del PDF (reproducible)
    │   ├── kpis.sql                  # Biblioteca de queries para Metabase
    │   └── Metabase - Dashboard estatico.pdf
    ├── notebooks/
    │   ├── eda_dataset.ipynb         # EDA del batch (Yahoo)
    │   ├── eda_api.ipynb             # EDA del stream (Finnhub)
    │   └── data/                     # CSVs de muestra
    ├── Utils/                        # Helpers de extracción
    ├── docker-compose.yml            # 9 servicios
    ├── preparar.py                   # Bootstrap interactivo de CSVs
    ├── Save.py                       # Helper de persistencia
    ├── requirements.txt
    └── README.md
```

---

## 🔄 Flujo del pipeline

```
1️⃣  preparar.py          → genera yahoo.csv + finhub.csv

2️⃣  Airflow DAG (cada 2 min)
    ├── extraccion_*      → carga CSVs / hace request a Alpha
    ├── transformacion_*  → schema unificado (timestamp, symbol, OHLC, source)
    ├── validar_*         → Great Expectations + BranchPythonOperator
    ├── cargar_db         → INSERT real en dim_* y fact_quotes
    └── cuarentena        → JSONB en quarantine_quotes si falla validación

3️⃣  Kafka Producer       → lee fact_quotes → publica a topic quotes_stream

4️⃣  Streamlit            → consume Kafka → grafica en vivo

5️⃣  Metabase             → consulta vw_quotes_flat → dashboards analíticos
```

---

## 📊 Modelo dimensional

Esquema en estrella sobre PostgreSQL. Definido en [`sql/01_init_schema.sql`](sql/01_init_schema.sql).

```
                   ┌──────────────┐
                   │  dim_time    │
                   └──────┬───────┘
                          │
┌──────────────┐    ┌─────▼────────┐    ┌──────────────┐
│  dim_symbol  ├───►│ fact_quotes  ◄────│  dim_source  │
└──────────────┘    └──────────────┘    └──────────────┘
                          │
                          ▼
                  vw_quotes_flat (vista para BI)
```

- **`fact_quotes`** — OHLC + price, con `UNIQUE(time_id, symbol_id, source_id)` para idempotencia.
- **`quarantine_quotes`** — lotes rechazados por GX con payload JSONB y razón.
- **`vw_quotes_flat`** — vista que joinea fact + dims, lista para Metabase.

---

## ✅ Validación con Great Expectations

Cada rama del DAG pasa por un `BranchPythonOperator` que usa el **patrón moderno de Great Expectations** (mismo flujo del notebook de referencia del curso):

```python
context  = gx.get_context(mode="cloud" if token else "ephemeral")
ds       = context.data_sources.add_pandas(name=...)
asset    = ds.add_dataframe_asset(name=...)
batch    = asset.add_batch_definition_whole_dataframe(...)
suite    = gx.ExpectationSuite(name=...)
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="symbol"))
suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="close", min_value=0, strict_min=True))
vd       = gx.ValidationDefinition(data=batch, suite=suite, name=...)
results  = vd.run(batch_parameters={"dataframe": df})
```

Reglas aplicadas:

| Regla | Aplica a |
|---|---|
| `ExpectColumnValuesToNotBeNull` | `symbol`, `timestamp` |
| `ExpectColumnValuesToNotBeNull` + positivos | `open`, `high`, `low`, `close` (Yahoo / Finnhub) |
| `ExpectColumnValuesToBeBetween(min=0)` | `price` (Alpha) |

Si una sola expectativa falla, todo el lote se redirige a `quarantine_quotes`.

---

## 🛠 Decisiones técnicas

| Decisión | Justificación |
|---|---|
| Postgres en lugar de SQLite | Concurrencia + conexión nativa a Metabase + esquema dimensional. |
| Metabase para BI estático | Open source, integra en compose, auto-detecta el schema. |
| Streamlit para real-time | Python-friendly según el rubro, hot-reload, simple. |
| Producer lee de `fact_quotes` | Garantiza calidad ya validada (no del stream crudo). |
| Agregación a 1 min en el DAG | Schema compatible entre Yahoo (OHLC) y Finnhub (ticks). |
| `UNIQUE(time, symbol, source)` | Idempotencia: reejecutar el DAG no duplica filas. |
| GE API moderna + GX Cloud | Suite persistente, ValidationDefinition, mismo patrón del docente. |
| NaN→NULL al cargar | Evita NUMERIC 'NaN' literal de Postgres que rompía `COALESCE` en BI. |

---

## ✅ Atención al feedback de la entrega 2

> *"Tareas fantasmas... simulan el merge y la carga con bashoperator. La orquestación falla desde ahí. Las visualizaciones deben ser desde la base de datos consolidada."*

| Observación | Solución |
|---|---|
| Tareas fantasma | Eliminadas. Solo quedan tareas con efecto real. |
| Carga simulada con `BashOperator` | `cargar_db` ahora es `PythonOperator` con SQLAlchemy. |
| Orquestación rota | DAG end-to-end funcional, idempotente, con cuarentena real. |
| Dashboards desde fuentes | Metabase consume `vw_quotes_flat`. Producer lee de `fact_quotes`. |

---

## 🔧 Comandos útiles

```bash
# Ver estado de los servicios
docker compose ps

# Logs de un servicio (producer, scheduler, etc.)
docker compose logs -f producer

# Trigger del DAG por CLI
docker compose exec airflow-scheduler airflow dags trigger ETL_Delivery3

# Verificar datos en el warehouse
docker exec -it postgres psql -U etl_user -d trading_db -c "SELECT source_name, COUNT(*) FROM vw_quotes_flat GROUP BY source_name;"

# Limpiar NaN literales si quedaron (después de cambios de esquema)
docker exec -it postgres psql -U etl_user -d trading_db -c "UPDATE fact_quotes SET open=NULL WHERE open::text='NaN'; UPDATE fact_quotes SET close=NULL WHERE close::text='NaN'; UPDATE fact_quotes SET price=NULL WHERE price::text='NaN';"

# Reiniciar el producer si su cursor quedó desincronizado
docker compose restart producer

# Apagar (mantiene datos)
docker compose down

# Reset total (⚠ borra volúmenes)
docker compose down -v

# Regenerar el PDF del informe
python docs/generate_report.py
```

---

## 🧪 Notebooks de EDA

```bash
cd notebooks
jupyter notebook
```

- **`eda_dataset.ipynb`** — EDA del batch (Yahoo): calidad, distribución del precio de cierre, cobertura temporal, comparativa entre pares.
- **`eda_api.ipynb`** — EDA del stream (Finnhub): tasa de mensajes, inter-arrival times, justificación cuantitativa de la agregación a 1 min.

Los notebooks son **autocontenidos**: leen los CSVs de muestra de `notebooks/data/` y no requieren que Docker esté corriendo.

---

## 🐛 Troubleshooting

| Síntoma | Causa probable | Solución |
|---|---|---|
| `validar_*` falla en rojo | GE no instalada o versión incompatible | `docker compose up -d --force-recreate airflow-scheduler` |
| `fact_quotes` vacía | DAG no ha corrido o falló | Trigger manual desde Airflow UI |
| Streamlit dice "Esperando mensajes..." | Producer no envía o ya envió todo | `docker compose restart producer` |
| Metabase muestra `NaN` en columnas | Filas viejas con NUMERIC 'NaN' literal | Ver comando de limpieza arriba |
| `preparar.py` no se encuentra | Mounts viejos en Airflow | `docker compose up -d --force-recreate airflow-scheduler` |

---

## 👥 Autores

| | |
|---|---|
| **Bryan Andres Herrera Betancur** | `2244008` |
| **Alessandro Yusty Ceballos** | `2240248` |

**Curso:** ETL (G51) — Daniel Felipe Romero Bernal
**Programa:** Ingeniería de Datos e Inteligencia Artificial
**Universidad:** Universidad Autónoma de Occidente

---

## 📚 Más información

- 📄 [Documento técnico completo](docs/technical_report.md)
- 📑 [Informe ejecutivo PDF](docs/informe_entrega_final.pdf)
- 📊 [Biblioteca de KPIs para Metabase](docs/kpis.sql)
- 🖼 [Dashboard estático de Metabase (PDF)](docs/Metabase%20-%20Dashboard%20estatico.pdf)

<div align="center">

**🎓 Proyecto académico — 2026**

</div>
