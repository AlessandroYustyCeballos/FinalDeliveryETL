<div align="center">
  <h1>📈 Proyecto ETL Forex — Entrega Final</h1>
  <p>Pipeline ETL completo orquestado por Apache Airflow, con modelo dimensional en PostgreSQL, streaming en Kafka y dashboards en Metabase (analítico) + Streamlit (real-time).</p>

  <p>
    <img src="https://img.shields.io/badge/Python-3.11-blue?logo=python" />
    <img src="https://img.shields.io/badge/Airflow-2.9-017CEE?logo=apacheairflow" />
    <img src="https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql" />
    <img src="https://img.shields.io/badge/Kafka-7.6-231F20?logo=apachekafka" />
    <img src="https://img.shields.io/badge/Streamlit-latest-FF4B4B?logo=streamlit" />
    <img src="https://img.shields.io/badge/Metabase-latest-509EE3?logo=metabase" />
    <img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker" />
  </p>
</div>

---

## 👥 Integrantes

- **Bryan Andres Herrera Betancur** — 2244008
- **Alessandro Yusty Ceballos** — 2240248

**Curso:** ETL (G51) — Daniel Felipe Romero Bernal
**Programa:** Ingeniería de Datos e Inteligencia Artificial

---

## 🎯 Objetivo

Diseñar, implementar y automatizar un pipeline ETL reproducible que:

1. **Extrae** datos del mercado forex desde tres fuentes complementarias (Yahoo Finance batch, Finnhub WebSocket, Alpha Vantage REST).
2. **Transforma** los tres formatos heterogéneos a un esquema unificado.
3. **Valida** la calidad de los datos con reglas declarativas en pandas (no-nulos, tipos numéricos, precios > 0).
4. **Carga** los lotes validados a un modelo dimensional en PostgreSQL.
5. **Stremea** una métrica de la fact table vía Kafka simulando CDC.
6. **Visualiza** los datos con dos dashboards complementarios: uno analítico (Metabase) y uno operativo en tiempo real (Streamlit + Plotly).

Caso de uso: análisis de comportamiento de divisas como insumo para modelos predictivos futuros, alineado al **ODS 1 — No pobreza** (apps de remesas, microcrédito, hedging para poblaciones vulnerables).

---

## 🏗 Arquitectura

```
   Yahoo Finance ──┐
   Finnhub WS ─────┼──► Airflow DAG (ETL_Delivery3) ──► PostgreSQL (modelo dimensional)
   Alpha Vantage ──┘            │                              │
                                │ Validación calidad           ├──► Metabase (analítico)
                                ▼                              │
                          quarantine_quotes                    └──► Kafka Producer
                                                                        │
                                                                  topic: quotes_stream
                                                                        │
                                                                        ▼
                                                                Streamlit (real-time)
```

> 📄 Diagrama detallado y descripción completa en [`docs/technical_report.md`](docs/technical_report.md).

### Stack

| Capa                  | Tecnología                          |
|-----------------------|-------------------------------------|
| Orquestación          | Apache Airflow 2.9                  |
| Data Warehouse        | PostgreSQL 15 (esquema en estrella) |
| Streaming             | Apache Kafka 7.6                    |
| Validación            | Reglas declarativas en pandas       |
| Dashboard analítico   | Metabase                            |
| Dashboard real-time   | Streamlit + Plotly                  |
| Contenerización       | Docker Compose                      |

---

## 📁 Estructura del repositorio

```
ETL-Proyect/
├── app/
│   ├── producer.py            # Kafka producer: lee fact_quotes → topic
│   ├── consumer.py            # Consumer standalone (debug)
│   └── dashboard.py           # Streamlit real-time dashboard
├── dags/
│   └── ETL.py                 # DAG ETL_Delivery3 (con carga REAL a Postgres)
├── docs/
│   ├── technical_report.md    # Documento técnico completo
│   └── architecture.png       # Diagrama de arquitectura
├── notebooks/
│   ├── eda_dataset.ipynb      # EDA del dataset (batch Yahoo)
│   ├── eda_api.ipynb          # EDA de la API (stream Finnhub)
│   └── data/                  # CSVs de muestra para los notebooks
├── sql/
│   ├── 00_create_metabase_db.sql   # Crea BD para Metabase
│   └── 01_init_schema.sql          # Modelo dimensional (dim + fact)
├── Utils/                     # Utilidades de extracción (legacy)
├── data/                      # CSVs intermedios (no se versiona)
├── logs/                      # Logs de Airflow (no se versiona)
├── plugins/                   # Plugins de Airflow
├── docker-compose.yml         # Stack completo: 8 servicios
├── preparar.py                # Genera yahoo.csv y finhub.csv iniciales
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🚀 Setup rápido

### Requisitos

- **Docker Desktop** con WSL2 (Windows) o nativo (Linux/Mac).
- **6 GB RAM** mínimo asignados a Docker.
- Puertos libres: `8080` (Airflow), `8501` (Streamlit), `3000` (Metabase), `5433` (Postgres), `9092` (Kafka).

### Pasos

```bash
# 1. Clonar
git clone https://github.com/<usuario>/ETL-Proyect.git
cd ETL-Proyect

# 2. Levantar todo el stack (toma ~2 min la primera vez por las imágenes)
docker compose up -d

# 3. Verificar que todos los servicios estén Up / healthy
docker compose ps

# 4. Generar los CSVs base que necesita el DAG
docker compose exec airflow-scheduler python /opt/airflow/preparar.py
# Elegir opción 1 + una divisa

# 5. Abrir las UIs:
#    Airflow:   http://localhost:8080   (admin / admin)
#    Metabase:  http://localhost:3000   (setup wizard la primera vez)
#    Streamlit: http://localhost:8501
```

### Activar el pipeline

1. **Airflow** → activar el DAG `ETL_Delivery3` con el toggle de la izquierda. Corre cada 2 minutos.
2. **Metabase** → primer arranque te pide:
   - Crear cuenta admin (cualquier email/password).
   - Add data → PostgreSQL:
     - Host: `postgres` · Port: `5432` · DB: `trading_db`
     - User: `etl_user` · Pass: `etl_pass`
3. **Streamlit** → se conecta solo al topic Kafka. Verás "Esperando mensajes..." hasta que el producer envíe el primer dato (después de que el DAG cargue al menos una fila en `fact_quotes`).

### Reset total

```bash
docker compose down -v     # ⚠ borra volúmenes y datos
docker compose up -d
```

---

## 🔄 Flujo end-to-end

1. **`preparar.py`** genera los CSVs iniciales (`data/yahoo.csv`, `data/finhub.csv`).
2. **DAG `ETL_Delivery3`** corre cada 2 min:
   - Extrae de Yahoo, Finnhub y Alpha Vantage (3 ramas paralelas).
   - Transforma al esquema unificado.
   - Valida con reglas declarativas en pandas vía `BranchPythonOperator`.
   - Carga validados a `fact_quotes` (insert real con SQLAlchemy).
   - Rechazados van a `quarantine_quotes` con payload JSONB.
3. **Producer Kafka** consulta `fact_quotes` y publica al topic `quotes_stream` con `time.sleep(1)` entre mensajes (simula CDC, como pide el enunciado).
4. **Streamlit** consume Kafka en background y grafica con Plotly.
5. **Metabase** consulta `vw_quotes_flat` (vista que joinea fact + dims) para el dashboard analítico.

---

## ✅ Atención a la retroalimentación de la entrega 2

> *"Muy incompleto el trabajo. crearon muchas tareas fantasmas, que no tienen una conclusión real, por otro lado simulan el merge y la carga con bashoperator. La orquestación falla desde ahí. las visualizaciones deben ser desde la base de datos consolidada, no desde las fuentes obtenidas."*

| Observación                                          | Solución en esta entrega                                       |
|------------------------------------------------------|----------------------------------------------------------------|
| Tareas fantasma (Merge, validar_merge, etc.)         | Eliminadas. Solo quedan tareas con efecto real.                |
| Merge/carga simulados con `BashOperator`             | `cargar_db` ahora es `PythonOperator` con inserción real.      |
| Orquestación rota desde la carga                     | DAG `ETL_Delivery3` end-to-end funcional, idempotente.         |
| Visualizaciones desde las fuentes (no del warehouse) | Metabase consume de `vw_quotes_flat`; producer lee de fact.    |

---

## 📊 Dashboards

### Metabase — analítico (`http://localhost:3000`)

Sugerencia de gráficos sobre `vw_quotes_flat`:

- Serie temporal del cierre por símbolo.
- Cierre promedio por símbolo y fuente (Yahoo vs Finnhub vs Alpha).
- Volatilidad por hora del día (`avg(high - low)`).
- Cobertura del pipeline por día y fuente.

### Streamlit — real-time (`http://localhost:8501`)

- Selector de par de divisas.
- Multi-fuente: compara Yahoo / Finnhub / Alpha en el mismo gráfico.
- KPIs en vivo: último precio + Δ%, máx, mín de la ventana.
- Vista panorámica: último precio de todos los símbolos en el buffer.

---

## 🧪 Notebooks EDA

```bash
cd notebooks
jupyter notebook
```

- **`eda_dataset.ipynb`** — Análisis exploratorio de los datos batch de Yahoo: calidad, distribución, cobertura.
- **`eda_api.ipynb`** — Análisis del stream crudo de Finnhub: tasa de mensajes, justificación de la agregación a 1 min.

---

## 🛠 Decisiones clave

| Decisión                                  | Justificación                                                    |
|-------------------------------------------|------------------------------------------------------------------|
| Postgres en lugar de SQLite               | Concurrencia + conexión nativa a Metabase + esquema dimensional. |
| Metabase para BI estático                 | Open source, integra en compose, conecta nativo a Postgres.      |
| Streamlit para real-time                  | Python-friendly como pide el rubro, hot-reload, simple.          |
| Producer lee de `fact_quotes` (no del WS) | Garantiza que el stream tenga calidad ya validada.               |
| Agregación a 1 min en el DAG              | Compatibilidad de esquema entre Yahoo (OHLC) y Finnhub (ticks).  |
| `UNIQUE(time, symbol, source)` en fact    | Idempotencia: reejecutar el DAG no duplica filas.                |

---

## 🔮 Próximos pasos

- Migrar `_PIP_ADDITIONAL_REQUIREMENTS` a un `Dockerfile` propio.
- Consumer Kafka que persista alertas en `fact_alerts`.
- Entrenar modelo predictivo de variación cambiaria (ODS 1).
- CI con GitHub Actions: lint + tests unitarios de las funciones de transformación.

---

## 📚 Documentación adicional

- **Documento técnico completo:** [`docs/technical_report.md`](docs/technical_report.md)
- **Diagrama de arquitectura:** [`docs/architecture.png`](docs/architecture.png)
- **Diseño del DAG:** ver sección 4 del documento técnico.
- **Modelo dimensional:** ver sección 5 del documento técnico.
- **Value Generation Articulation:** ver sección 8 del documento técnico.

---

## 📝 Licencia

Proyecto académico — Universidad Autónoma de Occidente.
