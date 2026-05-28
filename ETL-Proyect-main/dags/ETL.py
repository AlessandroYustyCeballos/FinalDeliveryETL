"""
ETL_Delivery3 - Pipeline Forex con carga REAL al modelo dimensional en Postgres.

Cambios vs Delivery2 (atendiendo feedback del profesor):
  * Eliminadas tareas fantasma: Merge, validar_merge, verificar_db, crear_db.
  * cargar_db ya NO es un BashOperator con echo. Ahora es un PythonOperator
    que inserta en dim_symbol / dim_time / fact_quotes vía SQLAlchemy.
  * La cuarentena persiste los lotes rechazados en la tabla quarantine_quotes
    de Postgres, no es un simple echo.
  * Las tres ramas (Yahoo, Finnhub, Alpha) llegan directo a cargar_db si
    pasan validación, o a cuarentena si fallan.
"""

from datetime import datetime, timedelta
import os
import json

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------
BASE_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
yahoo_data  = f"{BASE_PATH}/data/yahoo.csv"
finhub_data = f"{BASE_PATH}/data/finhub.csv"
Temp_path   = f"{BASE_PATH}/data/temp"

# Conexión al Postgres del proyecto (servicio docker-compose "postgres")
POSTGRES_URI = os.environ.get(
    "TRADING_DB_URI",
    "postgresql+psycopg2://etl_user:etl_pass@postgres:5432/trading_db",
)

ALPHA_API_KEY = os.environ.get("ALPHA_API_KEY", "3E1NL1R2CK7L2AIW")

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}


def _engine():
    return create_engine(POSTGRES_URI, pool_pre_ping=True)


# ============================================================
# EXTRACCIÓN
# ============================================================
def extraccion_yahoo():
    os.makedirs(Temp_path, exist_ok=True)
    if not os.path.exists(yahoo_data):
        raise FileNotFoundError("No se encontró yahoo.csv. Ejecuta preparar.py primero.")
    pd.read_csv(yahoo_data).to_csv(f"{Temp_path}/yahoo.csv", index=False)
    print("[OK] Yahoo extraído")


def extraccion_finhub():
    os.makedirs(Temp_path, exist_ok=True)
    if not os.path.exists(finhub_data):
        raise FileNotFoundError("No se encontró finhub.csv. Ejecuta preparar.py primero.")
    pd.read_csv(finhub_data).to_csv(f"{Temp_path}/finhub.csv", index=False)
    print("[OK] Finnhub extraído")


def extraccion_alpha():
    """Extracción Alpha con manejo graceful del rate limit (25 calls/día gratis).

    Si Alpha no responde con la estructura esperada (típicamente por rate
    limit), marcamos la rama como SKIPPED en vez de FAILED. Así las otras
    ramas (Yahoo, Finnhub) siguen sin que cargar_db caiga en upstream_failed.
    """
    os.makedirs(Temp_path, exist_ok=True)
    url = (
        "https://www.alphavantage.co/query"
        "?function=CURRENCY_EXCHANGE_RATE&from_currency=EUR&to_currency=USD"
        f"&apikey={ALPHA_API_KEY}"
    )
    try:
        data = requests.get(url, timeout=30).json()
    except Exception as e:
        raise AirflowSkipException(f"Alpha inalcanzable: {e}")

    if "Realtime Currency Exchange Rate" not in data:
        # Caso típico: rate limit. Alpha devuelve {"Information": "..."} o {"Note": "..."}
        msg = data.get("Information") or data.get("Note") or str(data)[:200]
        raise AirflowSkipException(f"Alpha sin datos (probable rate limit): {msg}")

    payload = data["Realtime Currency Exchange Rate"]
    pd.DataFrame([payload]).to_csv(f"{Temp_path}/alpha.csv", index=False)
    print("[OK] Alpha extraído")


# ============================================================
# TRANSFORMACIÓN
# ============================================================
def transformacion_yahoo():
    src = f"{Temp_path}/yahoo.csv"
    if not os.path.exists(src):
        raise FileNotFoundError(src)
    data = pd.read_csv(src)

    symbol = None
    for col in data.columns:
        if "Close_" in col:
            symbol = col.split("Close_")[1]
            break

    if symbol:
        data = data.rename(columns={
            f"Close_{symbol}":  "close",
            f"Open_{symbol}":   "open",
            f"High_{symbol}":   "high",
            f"Low_{symbol}":    "low",
            f"Volume_{symbol}": "volume",
        })
    else:
        symbol = "UNKNOWN"
        data.columns = [str(c).lower() for c in data.columns]

    data = data.rename(columns={
        "Datetime": "timestamp", "Date": "timestamp",
        "datetime": "timestamp", "date": "timestamp",
    })
    if "timestamp" not in data.columns:
        data = data.reset_index().rename(columns={"index": "timestamp"})
    if "symbol" not in data.columns:
        data["symbol"] = symbol

    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, errors="coerce")
    data["timestamp"] = data["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    data["source"] = "Yahoo"
    out = data[["timestamp", "symbol", "open", "high", "low", "close", "source"]]
    dst = f"{Temp_path}/yahoo_transformado.csv"
    out.to_csv(dst, index=False)
    os.remove(src)
    print(f"[OK] Yahoo transformado: {len(out)} filas")
    return dst


def transformacion_finhub():
    src = f"{Temp_path}/finhub.csv"
    if not os.path.exists(src):
        raise FileNotFoundError(src)
    data = pd.read_csv(src)
    data = data.rename(columns={"p": "price", "s": "symbol", "t": "timestamp", "v": "volume"})

    if pd.api.types.is_numeric_dtype(data["timestamp"]):
        data["timestamp"] = pd.to_datetime(data["timestamp"], unit="ms", utc=True)
    else:
        data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, errors="coerce")

    ohlc = (
        data.groupby(["symbol", data["timestamp"].dt.floor("min")])["price"]
        .agg(open="first", high="max", low="min", close="last")
        .reset_index()
    )
    ohlc["timestamp"] = ohlc["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    ohlc["symbol"] = ohlc["symbol"].str.replace("OANDA:", "", regex=False).str.replace("_", "", regex=False)
    ohlc["source"] = "Finnhub"
    out = ohlc[["timestamp", "symbol", "open", "high", "low", "close", "source"]]
    dst = f"{Temp_path}/finhub_transformado.csv"
    out.to_csv(dst, index=False)
    os.remove(src)
    print(f"[OK] Finnhub transformado: {len(out)} filas")
    return dst


def transformacion_alpha():
    src = f"{Temp_path}/alpha.csv"
    if not os.path.exists(src):
        raise FileNotFoundError(src)
    data = pd.read_csv(src)
    symbol = data["1. From_Currency Code"].astype(str) + data["3. To_Currency Code"].astype(str)
    price  = data["5. Exchange Rate"].astype(float)
    ts     = pd.to_datetime(data["6. Last Refreshed"], utc=True, errors="coerce")
    ts     = ts.dt.strftime("%Y-%m-%d %H:%M:%S")

    out = pd.DataFrame({
        "timestamp": ts, "symbol": symbol,
        "open":  None, "high":  None, "low":   None, "close": None,
        "price": price, "source": "Alpha",
    })
    dst = f"{Temp_path}/alpha_transformado.csv"
    out.to_csv(dst, index=False)
    os.remove(src)
    print(f"[OK] Alpha transformado: {len(out)} filas")
    return dst


# ============================================================
# VALIDACIÓN (Great Expectations - API moderna, mismo patrón
# que el notebook GX_cloud.ipynb del docente).
# ------------------------------------------------------------
# Patrón:
#   1. get_context (cloud si hay token, ephemeral local si no)
#   2. Data Source pandas -> Asset DataFrame -> Batch Definition
#   3. ExpectationSuite con gxe.Expect* (clases, no métodos)
#   4. ValidationDefinition que une suite + batch
#   5. .run() devuelve resultado con success bool + result_url
# ============================================================
def Validar_gx(**kwargs):
    import great_expectations as gx
    import great_expectations.expectations as gxe

    ti = kwargs["ti"]
    target_task_id = kwargs.get("target_task_id")
    branch_ok = kwargs.get("branch_ok")
    source_label = target_task_id.replace("transformacion_", "")  # yahoo / finhub / alpha
    ruta_csv  = ti.xcom_pull(task_ids=target_task_id)

    if not ruta_csv or not os.path.exists(ruta_csv):
        print(f"[FAIL] no se pudo leer {ruta_csv}")
        return "cuarentena"

    df = pd.read_csv(ruta_csv)

    # ------------------------------------------------------------
    # 1. Contexto: cloud si hay credenciales, ephemeral local si no
    # ------------------------------------------------------------
    if os.environ.get("GX_CLOUD_ACCESS_TOKEN"):
        context = gx.get_context(mode="cloud")
        print(f"[GX] Conectado a GX Cloud ({type(context).__name__})")
    else:
        context = gx.get_context(mode="ephemeral")
        print(f"[GX] Contexto ephemeral local (sin cloud)")

    # ------------------------------------------------------------
    # 2. Data Source -> Asset -> Batch Definition
    # ------------------------------------------------------------
    ds_name    = f"forex_{source_label}_ds"
    asset_name = f"forex_{source_label}_asset"
    batch_def  = f"forex_{source_label}_batch"

    try:
        datasource = context.data_sources.add_pandas(name=ds_name)
    except Exception:
        datasource = context.data_sources.get(ds_name)

    try:
        asset = datasource.add_dataframe_asset(name=asset_name)
    except Exception:
        asset = datasource.get_asset(asset_name)

    try:
        batch_definition = asset.add_batch_definition_whole_dataframe(batch_def)
    except Exception:
        batch_definition = asset.get_batch_definition(batch_def)

    # ------------------------------------------------------------
    # 3. ExpectationSuite con clases gxe.Expect*
    # ------------------------------------------------------------
    suite_name = f"suite_forex_{source_label}"
    try:
        suite = gx.ExpectationSuite(name=suite_name)

        # Reglas comunes a todas las fuentes
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="symbol"))
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="timestamp"))

        # Reglas OHLC (Yahoo / Finnhub)
        if "close" in df.columns and df["close"].notna().any():
            for col in ("open", "high", "low", "close"):
                suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))
                suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
                    column=col, min_value=0, strict_min=True
                ))

        # Reglas para price (Alpha)
        if "price" in df.columns and df["price"].notna().any():
            suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
                column="price", min_value=0, strict_min=True
            ))

        context.suites.add(suite)
    except Exception:
        suite = context.suites.get(name=suite_name)

    # ------------------------------------------------------------
    # 4. Validation Definition
    # ------------------------------------------------------------
    vd_name = f"vd_forex_{source_label}"
    try:
        validation_definition = gx.ValidationDefinition(
            data=batch_definition,
            suite=suite,
            name=vd_name,
        )
        context.validation_definitions.add(validation_definition)
    except Exception:
        validation_definition = context.validation_definitions.get(vd_name)

    # ------------------------------------------------------------
    # 5. Ejecutar la auditoría pasando el DataFrame como batch_parameters
    # ------------------------------------------------------------
    results = validation_definition.run(batch_parameters={"dataframe": df})

    print(f"[GX] Suite: {suite_name} | success={results.success}")
    if getattr(results, "result_url", None):
        print(f"[GX] Reporte cloud: {results.result_url}")

    if results.success:
        print(f"[OK] Validación GX exitosa: {ruta_csv} ({len(df)} filas)")
        return branch_ok

    print(f"[FAIL] {ruta_csv} | expectativas no cumplidas")
    return "cuarentena"


# ============================================================
# CARGA REAL al modelo dimensional
# ============================================================
def _get_or_create_symbol(conn, symbol: str) -> int:
    """Upsert dim_symbol y devolver symbol_id."""
    base  = symbol[:3] if len(symbol) >= 6 else symbol
    quote = symbol[3:6] if len(symbol) >= 6 else ""
    row = conn.execute(
        text("SELECT symbol_id FROM dim_symbol WHERE symbol = :s"),
        {"s": symbol},
    ).fetchone()
    if row:
        return row[0]
    return conn.execute(
        text("""
            INSERT INTO dim_symbol (symbol, base_currency, quote_currency)
            VALUES (:s, :b, :q)
            RETURNING symbol_id
        """),
        {"s": symbol, "b": base, "q": quote},
    ).fetchone()[0]


def _get_or_create_time(conn, ts: pd.Timestamp) -> int:
    """Upsert dim_time y devolver time_id."""
    row = conn.execute(
        text("SELECT time_id FROM dim_time WHERE ts = :ts"),
        {"ts": ts},
    ).fetchone()
    if row:
        return row[0]
    return conn.execute(
        text("""
            INSERT INTO dim_time (ts, year, month, day, hour, minute, day_of_week, date_only)
            VALUES (:ts, :y, :m, :d, :h, :mi, :dow, :dt)
            RETURNING time_id
        """),
        {
            "ts": ts, "y": ts.year, "m": ts.month, "d": ts.day,
            "h": ts.hour, "mi": ts.minute, "dow": ts.weekday(),
            "dt": ts.date(),
        },
    ).fetchone()[0]


def _get_source_id(conn, source_name: str) -> int:
    row = conn.execute(
        text("SELECT source_id FROM dim_source WHERE source_name = :s"),
        {"s": source_name},
    ).fetchone()
    if row:
        return row[0]
    return conn.execute(
        text("INSERT INTO dim_source (source_name, source_type) VALUES (:s, 'unknown') RETURNING source_id"),
        {"s": source_name},
    ).fetchone()[0]


def cargar_db(**kwargs):
    """Consolida los CSVs validados y los inserta en fact_quotes."""
    ti = kwargs["ti"]
    rutas = [
        ti.xcom_pull(task_ids="transformacion_yahoo"),
        ti.xcom_pull(task_ids="transformacion_finhub"),
        ti.xcom_pull(task_ids="transformacion_alpha"),
    ]
    rutas = [r for r in rutas if r and os.path.exists(r)]
    if not rutas:
        print("[WARN] No hay CSVs validados para cargar.")
        return

    dfs = [pd.read_csv(r) for r in rutas]
    df  = pd.concat(dfs, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "symbol", "source"])
    print(f"[INFO] Consolidado: {len(df)} filas listas para carga")

    engine = _engine()
    inserted = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            try:
                symbol_id = _get_or_create_symbol(conn, str(row["symbol"]))
                time_id   = _get_or_create_time(conn, row["timestamp"])
                source_id = _get_source_id(conn, str(row["source"]))
                conn.execute(
                    text("""
                        INSERT INTO fact_quotes
                            (time_id, symbol_id, source_id, open, high, low, close, price)
                        VALUES (:t, :s, :src, :o, :h, :l, :c, :p)
                        ON CONFLICT (time_id, symbol_id, source_id) DO NOTHING
                    """),
                    {
                        "t": time_id, "s": symbol_id, "src": source_id,
                        # Convertimos NaN de pandas a None para que Postgres
                        # lo guarde como NULL (no como el valor especial 'NaN'
                        # de NUMERIC, que rompe COALESCE en los dashboards).
                        "o": None if pd.isna(row.get("open"))  else float(row["open"]),
                        "h": None if pd.isna(row.get("high"))  else float(row["high"]),
                        "l": None if pd.isna(row.get("low"))   else float(row["low"]),
                        "c": None if pd.isna(row.get("close")) else float(row["close"]),
                        "p": None if pd.isna(row.get("price")) else float(row["price"]),
                    },
                )
                inserted += 1
            except Exception as e:
                print(f"[ERR] fila descartada: {e}")

    print(f"[OK] {inserted} filas insertadas en fact_quotes")

    # Limpieza de temporales validados
    for r in rutas:
        try: os.remove(r)
        except OSError: pass


def cuarentena(**kwargs):
    """Persiste en BD los lotes rechazados por validación."""
    ti = kwargs["ti"]
    engine = _engine()
    candidates = {
        "Yahoo":   ti.xcom_pull(task_ids="transformacion_yahoo"),
        "Finnhub": ti.xcom_pull(task_ids="transformacion_finhub"),
        "Alpha":   ti.xcom_pull(task_ids="transformacion_alpha"),
    }
    with engine.begin() as conn:
        for source, ruta in candidates.items():
            if not ruta or not os.path.exists(ruta):
                continue
            df = pd.read_csv(ruta)
            for _, row in df.iterrows():
                conn.execute(
                    text("""
                        INSERT INTO quarantine_quotes
                            (source_name, symbol, timestamp_raw, payload, reason)
                        VALUES (:src, :sym, :ts, CAST(:pl AS JSONB), :r)
                    """),
                    {
                        "src": source,
                        "sym": str(row.get("symbol", "")),
                        "ts":  str(row.get("timestamp", "")),
                        "pl":  json.dumps(row.dropna().to_dict(), default=str),
                        "r":   "fallo Great Expectations",
                    },
                )
            print(f"[QUAR] {len(df)} filas de {source} en cuarentena")


# ============================================================
# DAG
# ============================================================
with DAG(
    "ETL_Delivery3",
    default_args=default_args,
    description="ETL Forex con carga real al modelo dimensional",
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2026, 4, 16),
    catchup=False,
    tags=["delivery3", "forex", "dimensional"],
) as dag:

    # Extracción
    ext_yahoo  = PythonOperator(task_id="extraccion_yahoo",  python_callable=extraccion_yahoo)
    ext_finhub = PythonOperator(task_id="extraccion_finhub", python_callable=extraccion_finhub)
    ext_alpha  = PythonOperator(task_id="extraccion_alpha",  python_callable=extraccion_alpha)

    # Transformación
    tr_yahoo  = PythonOperator(task_id="transformacion_yahoo",  python_callable=transformacion_yahoo)
    tr_finhub = PythonOperator(task_id="transformacion_finhub", python_callable=transformacion_finhub)
    tr_alpha  = PythonOperator(task_id="transformacion_alpha",  python_callable=transformacion_alpha)

    # Validación con branching
    val_yahoo = BranchPythonOperator(
        task_id="validar_yahoo",
        python_callable=Validar_gx,
        op_kwargs={"target_task_id": "transformacion_yahoo", "branch_ok": "cargar_db"},
    )
    val_finhub = BranchPythonOperator(
        task_id="validar_finhub",
        python_callable=Validar_gx,
        op_kwargs={"target_task_id": "transformacion_finhub", "branch_ok": "cargar_db"},
    )
    val_alpha = BranchPythonOperator(
        task_id="validar_alpha",
        python_callable=Validar_gx,
        op_kwargs={"target_task_id": "transformacion_alpha", "branch_ok": "cargar_db"},
    )

    # Carga REAL al modelo dimensional (ya no es un echo)
    cargar = PythonOperator(
        task_id="cargar_db",
        python_callable=cargar_db,
        trigger_rule="none_failed_min_one_success",  # corre si al menos una rama validó OK
    )

    # Cuarentena REAL (persiste en quarantine_quotes, ya no es echo)
    quar = PythonOperator(
        task_id="cuarentena",
        python_callable=cuarentena,
        trigger_rule="none_failed_min_one_success",
    )

    # Dependencias
    ext_yahoo  >> tr_yahoo  >> val_yahoo  >> [cargar, quar]
    ext_finhub >> tr_finhub >> val_finhub >> [cargar, quar]
    ext_alpha  >> tr_alpha  >> val_alpha  >> [cargar, quar]
