# Proyecto ETL Forex — Documento Técnico (Entrega Final)

**Curso:** ETL (G51)
**Programa:** Ingeniería de Datos e Inteligencia Artificial
**Docente:** Daniel Felipe Romero Bernal
**Integrantes:**
- Bryan Andres Herrera Betancur — 2244008
- Alessandro Yusty Ceballos — 2240248

**Repositorio:** `https://github.com/<usuario>/ETL-Proyect`
**Fecha de entrega:** Mayo 2026

---

## 1. Resumen ejecutivo

Este proyecto implementa un **pipeline ETL reproducible y orquestado** que consume datos del mercado forex desde tres fuentes complementarias (Yahoo Finance, Finnhub vía WebSocket y Alpha Vantage), los valida con reglas declarativas de calidad, los consolida en un **modelo dimensional sobre PostgreSQL** y los expone a través de dos dashboards: uno **analítico** sobre los datos históricos consolidados (Metabase) y otro **operativo en tiempo real** que consume del flujo Kafka (Streamlit). Toda la infraestructura se levanta con `docker compose up -d`.

Esta entrega final corrige los problemas señalados en la retroalimentación de la segunda entrega:
- Se eliminaron las "tareas fantasma" del DAG que simulaban merge/carga con `BashOperator`.
- La carga al data warehouse es **real** (PythonOperator + SQLAlchemy) sobre un modelo dimensional formal en estrella.
- Las visualizaciones consumen **exclusivamente desde la base de datos consolidada**, no desde los CSVs intermedios.

---

## 2. Arquitectura

### 2.1. Diagrama (alto nivel)

> 📌 **Diagrama actualizado**: `docs/architecture.png` — generado con draw.io.

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Yahoo Finance│    │   Finnhub    │    │ Alpha Vantage│
│   (batch)    │    │  (WebSocket) │    │    (API)     │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           ▼
                  ┌──────────────────┐
                  │  Airflow DAG     │  ETL_Delivery3
                  │ Extract→Transform│
                  │ →Validate (rules)│
                  │ →Load            │
                  └────────┬─────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │  PostgreSQL          │
                │  Modelo dimensional  │
                │  (dim + fact)        │
                └──────┬───────┬───────┘
                       │       │
              ┌────────┘       └────────┐
              ▼                         ▼
        ┌──────────┐              ┌──────────────┐
        │ Metabase │              │ Kafka        │
        │ Dashboard│              │ Producer     │
        │ analítico│              │ (CDC sim)    │
        └──────────┘              └──────┬───────┘
                                         │ topic: quotes_stream
                                         ▼
                                  ┌──────────────┐
                                  │ Streamlit    │
                                  │ Real-time    │
                                  │ Dashboard    │
                                  └──────────────┘
```

### 2.2. Stack tecnológico

| Componente             | Tecnología                            | Justificación                                              |
|------------------------|---------------------------------------|------------------------------------------------------------|
| Orquestación           | Apache Airflow 2.9                    | Estándar de la industria, BranchPython para validación.    |
| Data Warehouse         | PostgreSQL 15                         | Soporte SQL completo, JSONB para cuarentena, integra BI.   |
| Streaming              | Apache Kafka (Confluent 7.6.1)        | Pub/sub robusto, simula CDC sobre la fact table.           |
| Validación             | Great Expectations 1.x+ (API moderna) | Suite persistente + ValidationDefinition + soporte GX Cloud. |
| Dashboard analítico    | Metabase                              | No-code, conecta nativo a Postgres, levanta en compose.    |
| Dashboard real-time    | Streamlit + Plotly                    | Python-friendly como pide el rubro, integra Kafka fácil.   |
| Contenerización        | Docker Compose                        | Reproducibilidad total, un comando para todo el stack.     |

---

## 3. Descripción del pipeline ETL

### 3.1. Extracción

Tres tareas paralelas, cada una con su propia lógica de acceso:

| Fuente        | Método                                  | Frecuencia            | Estructura del payload       |
|---------------|-----------------------------------------|-----------------------|------------------------------|
| Yahoo Finance | `yfinance.download()` HTTP              | Histórico, batch      | DataFrame OHLCV multi-index  |
| Finnhub       | WebSocket `wss://ws.finnhub.io`         | Tiempo real (ticks)   | JSON `{p, s, t, v}`          |
| Alpha Vantage | REST `CURRENCY_EXCHANGE_RATE`           | Bajo demanda          | JSON anidado, precio simple  |

La generación inicial de los CSV base (`yahoo.csv`, `finhub.csv`) se realiza con `preparar.py` antes de activar el DAG.

### 3.2. Transformación

Las tres transformaciones convergen al **mismo esquema unificado**: `timestamp, symbol, open, high, low, close, price, source`.

- **Yahoo**: aplanado de MultiIndex (`Close_EURUSD=X → close`), estandarización de timestamp a `YYYY-MM-DD HH:MM:SS`, extracción del símbolo desde el nombre de columna.
- **Finnhub**: renombrado de llaves cortas (`p→price, s→symbol, t→timestamp, v→volume`), conversión Unix-ms → datetime UTC, **agregación OHLC por minuto** (`groupby(minute).agg(open=first, high=max, low=min, close=last)`).
- **Alpha**: extracción de la respuesta anidada, construcción del símbolo (`From + To`), normalización del timestamp. No tiene OHLC, solo `price`.

> **Decisión clave**: el stream de Finnhub se agrega a 1 minuto para ser **schema-compatible** con los datos batch de Yahoo. Esto permite que ambas fuentes se inserten en la misma tabla `fact_quotes` y se consulten juntas desde Metabase. (Justificación cuantitativa en `notebooks/eda_api.ipynb`).

### 3.3. Validación de calidad

Implementada como `BranchPythonOperator` (`validar_yahoo`, `validar_finhub`, `validar_alpha`) usando **Great Expectations** con la API moderna (1.x+) siguiendo el patrón `ExpectationSuite + ValidationDefinition`. Las reglas:

| Regla                                                 | Aplica a               |
|-------------------------------------------------------|------------------------|
| Columnas obligatorias sin nulos                       | symbol, timestamp      |
| Tipo numérico y sin nulos en métricas                 | open, high, low, close |
| Valores estrictamente positivos (`> 0`)               | open, high, low, close, price |
| Detección automática de OHLC vs price (Alpha)         | según presencia        |

**Patrón implementado** (mismo flujo que el material del docente):

1. `gx.get_context(mode="cloud")` si hay token de GX Cloud en variables de entorno, o `mode="ephemeral"` como fallback local. Esto permite que el reporte de cada validación se publique automáticamente en el dashboard de GX Cloud con `result_url`.
2. `context.data_sources.add_pandas(...)` → `add_dataframe_asset(...)` → `add_batch_definition_whole_dataframe(...)` para formalizar la fuente.
3. `gx.ExpectationSuite(name=...)` con `suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(...))` y `gxe.ExpectColumnValuesToBeBetween(...)` (clases de la API moderna, no métodos sobre el DataFrame).
4. `gx.ValidationDefinition(data=..., suite=..., name=...)` que vincula el batch con la suite.
5. `validation_definition.run(batch_parameters={"dataframe": df})` ejecuta la auditoría y devuelve un objeto con `.success` y, si hay cloud, `.result_url` con el reporte visual.

**Si una sola expectativa falla, todo el lote se redirige a cuarentena**. El bloque está envuelto en `try/except` para reutilizar suites, datasources y validation definitions si ya existen en GX Cloud entre corridas del DAG.

**Rutas según resultado:**
- ✅ Pasa → `cargar_db` (insert al modelo dimensional)
- ❌ Falla → `cuarentena` (insert en tabla `quarantine_quotes` con payload JSONB + razón)

### 3.4. Carga

`cargar_db` es un **`PythonOperator` real** (no un `BashOperator` con `echo`, como en la entrega 2). Operaciones:

1. Lee los CSVs validados de los tres XComs.
2. Concatena en un único DataFrame.
3. Por cada fila:
   - Upsert en `dim_symbol` (devuelve `symbol_id`).
   - Upsert en `dim_time` (devuelve `time_id`).
   - Lookup en `dim_source` (precargado por seed).
   - `INSERT ... ON CONFLICT DO NOTHING` en `fact_quotes` (idempotencia garantizada por `UNIQUE(time_id, symbol_id, source_id)`).
4. Borra los CSVs temporales.

---

## 4. Diseño del DAG (`ETL_Delivery3`)

### 4.1. Árbol de dependencias

```
extraccion_yahoo  → transformacion_yahoo  → validar_yahoo  ─┐
extraccion_finhub → transformacion_finhub → validar_finhub ─┼→ [cargar_db | cuarentena]
extraccion_alpha  → transformacion_alpha  → validar_alpha  ─┘
```

### 4.2. Tareas

| Task ID                | Tipo                     | Responsabilidad                                             |
|------------------------|--------------------------|-------------------------------------------------------------|
| `extraccion_yahoo`     | PythonOperator           | Copia `yahoo.csv` a `/data/temp/`.                          |
| `extraccion_finhub`    | PythonOperator           | Copia `finhub.csv` a `/data/temp/`.                         |
| `extraccion_alpha`     | PythonOperator           | GET a Alpha Vantage, guarda JSON como CSV.                  |
| `transformacion_*`     | PythonOperator (x3)      | Normaliza al esquema unificado, devuelve ruta por XCom.     |
| `validar_*`            | BranchPythonOperator (x3)| Aplica suite de Great Expectations, decide la siguiente rama.   |
| `cargar_db`            | PythonOperator           | **Inserción real** en `dim_*` y `fact_quotes`.              |
| `cuarentena`           | PythonOperator           | **Persistencia real** en `quarantine_quotes` con JSONB.     |

### 4.3. Configuración

- **Schedule:** `timedelta(minutes=2)` — suficiente para acumular ticks en `stream.csv`.
- **Retries:** 1 por tarea con `retry_delay=15s`.
- **Catchup:** `False` — solo corre futuras ventanas.
- **`trigger_rule="none_failed_min_one_success"`** en `cargar_db` y `cuarentena` para que ejecuten aunque solo una rama haya tenido éxito.

### 4.4. Diferencias con la entrega 2

| Aspecto                          | Entrega 2                            | Entrega final                           |
|----------------------------------|--------------------------------------|-----------------------------------------|
| `cargar_db`                      | `BashOperator` con `echo`            | `PythonOperator` con SQLAlchemy         |
| `Merge`                          | `BashOperator` con `echo`            | **Eliminada** (tarea fantasma)          |
| `validar_merge`                  | `BranchPythonOperator` sobre un echo | **Eliminada** (tarea fantasma)          |
| `verificar_db` / `crear_db`      | Funciones que no se ejecutaban       | **Eliminadas**; el schema lo crea Docker|
| `cuarentena`                     | `BashOperator` con `echo`            | `PythonOperator` que persiste en BD     |
| Destino de la carga              | SQLite local sin modelo dimensional  | PostgreSQL con esquema en estrella      |

---

## 5. Modelo dimensional

Esquema en estrella sobre PostgreSQL. Definido en `sql/01_init_schema.sql`. Se crea automáticamente al primer arranque del contenedor `postgres`.

### 5.1. Tablas

**Dimensiones**

- **`dim_symbol`** — catálogo de pares de divisas
  ```
  symbol_id PK | symbol | base_currency | quote_currency | description
  ```

- **`dim_source`** — origen del dato (Yahoo / Finnhub / Alpha), seed precargado
  ```
  source_id PK | source_name | source_type | description
  ```

- **`dim_time`** — tabla de tiempo desnormalizada para BI
  ```
  time_id PK | ts | year | month | day | hour | minute | day_of_week | date_only
  ```

**Hechos**

- **`fact_quotes`** — métricas OHLC + price
  ```
  quote_id PK | time_id FK | symbol_id FK | source_id FK |
  open | high | low | close | price | loaded_at
  UNIQUE (time_id, symbol_id, source_id)   -- idempotencia
  ```

**Soporte**

- **`quarantine_quotes`** — lotes que fallaron la validación de calidad, con payload JSONB y razón.
- **`vw_quotes_flat`** — vista que joinea fact + dimensiones (consumida por Metabase y por el Kafka producer).

### 5.2. Justificación de la elección

- **Estrella, no copo de nieve**: las dimensiones son pequeñas y no requieren jerarquías profundas. Simplicidad > normalización pura para este volumen.
- **`UNIQUE(time, symbol, source)` en fact**: permite reejecuciones idempotentes del DAG sin duplicar registros.
- **`dim_source` con seed**: las 3 fuentes se conocen de antemano; cargarlas vía SQL evita races durante el primer DAG run.

---

## 6. Streaming con Kafka

### 6.1. Producer (`app/producer.py`)

Implementa lo que el enunciado solicita textualmente: *"a Python producer that iterates over the rows in your fact table and sends them to the Kafka topic with a slight time delay"*.

- Lee de `fact_quotes` joineada con dimensiones (consulta a `vw_quotes_flat` equivalente).
- Mantiene un **cursor** (`last_id`) para no reenviar mensajes ya publicados.
- Publica al topic `quotes_stream` con `time.sleep(1)` entre mensajes.
- Particiona por `symbol` para preservar orden por par.
- Cuando se queda sin datos nuevos, hace polling cada 10s (configurable).
- Reconexión automática a Kafka.

### 6.2. Consumer + Dashboard (`app/dashboard.py`)

- Thread daemon consumiendo `quotes_stream` y alimentando un `deque(maxlen=2000)` thread-safe.
- Streamlit re-renderiza cada 2s con snapshot del buffer (`st.cache_resource` para que el thread se inicie una sola vez).
- **Filtros reactivos** en sidebar: par de divisa, fuentes a incluir, tamaño de ventana.
- **KPIs:** último precio + Δ%, máx, mín, ticks en ventana.
- **Gráfico principal:** línea Plotly con una traza por fuente, permitiendo comparar Yahoo vs Finnhub en vivo.
- **Vista panorámica:** tabla con el último precio de todos los símbolos en el buffer.

---

## 7. Visualizaciones e insights

### 7.1. Dashboard analítico (Metabase, `http://localhost:3000`)

Conectado a la BD `trading_db`, consume principalmente de `vw_quotes_flat`.

| Visualización                                  | Fuente                              | Insight                                    |
|------------------------------------------------|-------------------------------------|--------------------------------------------|
| Serie temporal del precio de cierre por símbolo| `vw_quotes_flat`                    | Evolución histórica intradiaria.           |
| Cierre promedio por símbolo y fuente           | GROUP BY symbol, source_name        | Discrepancias entre proveedores de datos.  |
| Volatilidad por hora                           | `avg(high-low)` agrupado por `hour` | Detecta horas de mayor actividad de mercado.|
| Cobertura del pipeline                         | `count()` por `date_only` y source  | Demuestra que las 3 fuentes alimentan la BD.|

> **Comentario del profesor (entrega 2):** "las visualizaciones deben ser desde la base de datos consolidada, no desde las fuentes obtenidas". Este dashboard responde directamente a esa observación.

### 7.2. Dashboard en tiempo real (Streamlit, `http://localhost:8501`)

- Visualiza el flujo de Kafka en vivo.
- Permite seleccionar el símbolo y la fuente.
- Muestra simultáneamente datos del producer (que itera sobre `fact_quotes`) → mismo origen que el dashboard analítico.

### 7.3. EDA notebooks (`notebooks/`)

- **`eda_dataset.ipynb`** — EDA del batch (Yahoo): calidad, distribuciones, cobertura temporal, comparativa entre pares.
- **`eda_api.ipynb`** — EDA del stream (Finnhub): tasa de mensajes, distribución intra-arrival, justificación cuantitativa de la agregación a 1 minuto.

---

## 8. Value Generation Articulation

Esta arquitectura genera valor en dos planos distintos pero complementarios, atendiendo procesos operativos y analíticos del negocio.

### 8.1. Valor operativo — Stream en tiempo real

El **Kafka producer + consumer Streamlit** soporta procesos **operativos** donde la latencia importa: un trader, un sistema de alertas o un motor de toma de decisiones automatizada necesita ver el precio actual con segundos de retardo, no horas. El topic `quotes_stream` actúa como un bus de eventos al que pueden conectarse múltiples consumidores (no solo el dashboard): por ejemplo, un servicio futuro de alertas que dispare notificaciones cuando un par cruce un umbral, o un microservicio de risk management que recalcule exposición en vivo. La arquitectura **desacopla la producción de eventos del consumo**, lo que es la base de cualquier sistema reactivo en producción. Para el caso de uso del proyecto (ODS 1 — No pobreza, mediante análisis predictivo de divisas), el componente operativo abre la puerta a productos como apps móviles de remesas que ajusten tasas en tiempo real para beneficiar a poblaciones vulnerables.

### 8.2. Valor analítico — Modelo dimensional + dashboard estático

El **modelo dimensional sobre PostgreSQL + Metabase** soporta procesos **analíticos** donde la profundidad histórica y la capacidad de cruzar dimensiones pesan más que la latencia. El esquema en estrella permite responder preguntas como "¿cuál fue la volatilidad promedio del EUR/USD los lunes vs los viernes del último mes?" o "¿qué fuente de datos reporta sistemáticamente precios más altos en horas de baja liquidez?" — preguntas imposibles de responder con un stream efímero. Esta capa es la **base sobre la que se construirán los modelos predictivos futuros** mencionados como objetivo del proyecto: tener los datos limpios, validados y dimensionados en un warehouse es prerrequisito de cualquier proyecto de machine learning serio. El dashboard de Metabase materializa este valor para usuarios no técnicos (gestores de producto, analistas financieros), permitiéndoles explorar los datos sin escribir SQL.

### 8.3. Convergencia de ambos valores

Crucialmente, **ambos planos consumen del mismo modelo dimensional**: el producer de Kafka **NO** lee del stream crudo de Finnhub, sino de `fact_quotes` ya validada. Esto garantiza que lo que se ve en el dashboard real-time es **el mismo dato** que se ve en el dashboard analítico (con el desfase de la simulación), eliminando la temida "verdad doble" que afecta a arquitecturas Lambda mal diseñadas. La validación declarativa en el DAG es el **gate único de calidad**, por lo que ningún dato sucio llega ni al BI ni al stream.

---

## 9. Decisiones técnicas y trade-offs

| Decisión                                       | Alternativa considerada            | Trade-off                                                  |
|------------------------------------------------|------------------------------------|------------------------------------------------------------|
| PostgreSQL en lugar de SQLite                  | Mantener SQLite de la entrega 2    | + Conexión nativa a BI / + concurrencia. Más memoria.      |
| Metabase para BI estático                      | Power BI / Looker Studio           | + Open source, integra en compose. − Menos polish que PBI. |
| Streamlit para real-time                       | Dash / Bokeh                       | + Más simple, hot-reload. − Modelo de threading limitado.  |
| Producer lee de `fact_quotes` (no de Finnhub)  | Stream directo de Finnhub a Kafka  | + Garantiza calidad ya validada. − No es CDC "real".       |
| Tareas paralelas en el DAG                     | Pipeline secuencial                | + Throughput. − Más complejidad en branching.              |
| Esquema dimensional vs tabla por símbolo (E2)  | Mantener tablas dinámicas por par  | + Soporta BI. − Requiere upserts en dimensiones.           |
| GE 1.x+ API moderna (Suite + ValidationDef)    | API legacy `from_pandas()`         | + Suite persistente, integración con GX Cloud, mismo patrón del docente. |

---

## 10. Setup y ejecución

### 10.1. Requisitos

- Docker Desktop con WSL2 backend (Windows) o equivalente en Linux/Mac.
- 6 GB de RAM asignados a Docker mínimo.
- Puertos libres: 8080 (Airflow), 8501 (Streamlit), 3000 (Metabase), 5433 (Postgres), 9092 (Kafka).

### 10.2. Pasos

```bash
# 1. Clonar el repo
git clone https://github.com/<usuario>/ETL-Proyect.git
cd ETL-Proyect

# 2. Levantar todo el stack
docker compose up -d

# 3. Esperar ~2 min a que Airflow + Metabase terminen de inicializar
docker compose ps   # todos los servicios "Up" o "healthy"

# 4. Generar los CSVs base que el DAG necesita
docker compose exec airflow-scheduler python /opt/airflow/preparar.py
# (opción 1 + elegir divisa)

# 5. Abrir las UIs:
#    - Airflow:   http://localhost:8080  (admin / admin)
#    - Metabase:  http://localhost:3000  (setup wizard la primera vez)
#    - Streamlit: http://localhost:8501

# 6. En Airflow, activar el DAG "ETL_Delivery3"
```

### 10.3. Configurar Metabase la primera vez

1. Setup wizard → crear usuario admin.
2. Add data → PostgreSQL:
   - Host: `postgres`, Port: `5432`, DB: `trading_db`
   - User: `etl_user`, Pass: `etl_pass`
3. Metabase auto-detectará `fact_quotes`, `dim_*` y la vista `vw_quotes_flat`.

### 10.4. Reset total

```bash
docker compose down -v   # cuidado: borra volúmenes (Postgres, Metabase, datos)
docker compose up -d
```

---

## 11. Próximos pasos

- Migrar el `_PIP_ADDITIONAL_REQUIREMENTS` de Airflow a un `Dockerfile` propio para imagen reproducible.
- Implementar un consumer Kafka adicional que persista métricas agregadas (alertas) en una tabla `fact_alerts`.
- Entrenar el modelo predictivo de variaciones cambiarias mencionado en el objetivo del proyecto (ODS 1).
- Añadir CI con GitHub Actions: linter de Python + tests unitarios sobre las funciones de transformación.

---

## 12. Anexos

- **Repositorio:** `https://github.com/<usuario>/ETL-Proyect`
- **Diagrama de arquitectura:** `docs/architecture.png`
- **DAG:** `dags/ETL.py`
- **Schema:** `sql/01_init_schema.sql`
- **Producer:** `app/producer.py`
- **Dashboard real-time:** `app/dashboard.py`
- **EDA:** `notebooks/eda_dataset.ipynb`, `notebooks/eda_api.ipynb`
