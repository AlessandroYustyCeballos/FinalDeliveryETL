<div align="center">

# 📈 ETL Forex Pipeline

**Pipeline ETL end-to-end para datos del mercado forex con orquestación Airflow, streaming Kafka y dashboards en tiempo real.**


---

## 🎯 ¿Qué hace este proyecto?

Extrae datos de tres fuentes de divisas (**Yahoo Finance**, **Finnhub** WebSocket, **Alpha Vantage**), los valida, los carga en un **modelo dimensional sobre PostgreSQL**, y los expone vía dos dashboards complementarios:

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
                        validate,                          │
                        load)                          quotes_stream
                          │                                │
                          ▼                                ▼
                  quarantine_quotes                  Streamlit (real-time)
```

> 📐 Diagrama detallado: [`docs/architecture.png`](docs/architecture.png)
> 📄 Documento técnico: [`docs/technical_report.md`](docs/technical_report.md)

### Stack

| Capa | Tecnología |
|---|---|
| **Orquestación** | Apache Airflow 2.9 |
| **Data Warehouse** | PostgreSQL 15 (esquema en estrella) |
| **Streaming** | Apache Kafka 7.6 |
| **Validación** | Reglas declarativas en pandas |
| **BI analítico** | Metabase |
| **Real-time** | Streamlit + Plotly |
| **Infraestructura** | Docker Compose |

---

## 🚀 Quick Start

### Requisitos

- Docker Desktop con WSL2 (Windows) o nativo (Linux/Mac)
- 6 GB de RAM asignados a Docker
- Puertos libres: `8080`, `8501`, `3000`, `5433`, `9092`

### Instalación

```bash
# Clonar el repositorio
git clone https://github.com/<usuario>/ETL-Proyect.git
cd ETL-Proyect

# Levantar todo el stack
docker compose up -d
```

⏱ La primera vez tarda ~2 minutos (descarga de imágenes). Las siguientes, ~20 segundos.

### Inicializar el pipeline

```bash
# Generar los CSVs base (yahoo.csv y finhub.csv)
docker compose exec airflow-scheduler python /opt/airflow/preparar.py
# → Opción 1 → seleccionar índice de divisa (ej. 0 = EURUSD)
# → Ctrl+C después de ~30 segundos para detener Finnhub
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

---

## 📁 Estructura

```
ETL-Proyect/
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
│   ├── architecture.png          # Diagrama
│   └── kpis.sql                  # Biblioteca de queries para Metabase
├── notebooks/
│   ├── eda_dataset.ipynb         # EDA del batch (Yahoo)
│   ├── eda_api.ipynb             # EDA del stream (Finnhub)
│   └── data/                     # CSVs de muestra
├── Utils/                        # Helpers de extracción
├── docker-compose.yml            # 9 servicios
├── preparar.py                   # Bootstrap de CSVs
└── requirements.txt
```

---

## 🔄 Flujo del pipeline

```
1️⃣  preparar.py          → genera yahoo.csv + finhub.csv

2️⃣  Airflow DAG (cada 2 min)
    ├── extraccion_*      → carga CSVs / hace request a Alpha
    ├── transformacion_*  → schema unificado (timestamp, symbol, OHLC, source)
    ├── validar_*         → reglas de calidad (BranchPythonOperator)
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
- **`quarantine_quotes`** — lotes rechazados con payload JSONB y razón.
- **`vw_quotes_flat`** — vista que joinea fact + dims, lista para Metabase.

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
| Validación pandas vs Great Expectations | API simple, sin dependencias frágiles, mismas reglas. |

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

# Logs de un servicio
docker compose logs -f airflow-scheduler

# Trigger del DAG por CLI
docker compose exec airflow-scheduler airflow dags trigger ETL_Delivery3

# Verificar datos en el warehouse
docker exec -it postgres psql -U etl_user -d trading_db -c "SELECT source_name, COUNT(*) FROM vw_quotes_flat GROUP BY source_name;"

# Apagar (mantiene datos)
docker compose down

# Reset total (⚠ borra volúmenes)
docker compose down -v
```

---

## 🧪 Notebooks de EDA

```bash
cd notebooks
jupyter notebook
```

- **`eda_dataset.ipynb`** — EDA del batch (Yahoo): calidad, distribución, cobertura.
- **`eda_api.ipynb`** — EDA del stream (Finnhub): tasa de mensajes, justificación de la agregación a 1 min.

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
- 📐 [Diagrama de arquitectura](docs/architecture.png)
- 📊 [Biblioteca de KPIs para Metabase](docs/kpis.sql)

<div align="center">

**🎓 Proyecto académico — 2026**

</div>
