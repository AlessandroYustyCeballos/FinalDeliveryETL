
# Streamlit y Kafka Consumer http://localhost:8501


import json
import os
import threading
import time
from collections import deque
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "quotes_stream")
BUFFER_SIZE     = int(os.environ.get("BUFFER_SIZE", "2000"))   # ticks max en memoria
AUTO_REFRESH    = float(os.environ.get("AUTO_REFRESH", "2.0")) # repeticiones

st.set_page_config(
    page_title="Forex Real-Time Dashboard",
    layout="wide",
)


@st.cache_resource
def get_shared_state():
    return {
        "buffer": deque(maxlen=BUFFER_SIZE),
        "lock":   threading.Lock(),
        "status": {"connected": False, "messages": 0, "started_at": None, "error": None},
    }


def _kafka_loop(state):
    """Loop infinito que consume Kafka y llena el buffer."""
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=f"streamlit-{datetime.utcnow().timestamp()}",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            state["status"]["connected"]  = True
            state["status"]["started_at"] = datetime.utcnow()
            state["status"]["error"]      = None

            for msg in consumer:
                with state["lock"]:
                    state["buffer"].append(msg.value)
                    state["status"]["messages"] += 1
        except NoBrokersAvailable as e:
            state["status"]["connected"] = False
            state["status"]["error"] = f"Kafka no disponible: {e}"
            time.sleep(5)
        except Exception as e:
            state["status"]["connected"] = False
            state["status"]["error"] = f"Consumer error: {e}"
            time.sleep(5)


@st.cache_resource
def start_consumer_thread():
    state = get_shared_state()
    t = threading.Thread(target=_kafka_loop, args=(state,), daemon=True)
    t.start()
    return t


# consumer 
start_consumer_thread()
state = get_shared_state()

# ---------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------
st.title("📈 Forex Real-Time Dashboard")
st.caption(f"Stream desde Kafka topic **`{KAFKA_TOPIC}`** vía `{KAFKA_BOOTSTRAP}`")


with state["lock"]:
    buf_snapshot = list(state["buffer"])
    status = dict(state["status"])


col_s1, col_s2, col_s3, col_s4 = st.columns(4)
col_s1.metric(" Conexión", "OK" if status["connected"] else "DOWN")
col_s2.metric(" Mensajes recibidos", status["messages"])
col_s3.metric(" En buffer", len(buf_snapshot))
col_s4.metric(" Refresh cada", f"{AUTO_REFRESH}s")

if status["error"]:
    st.warning(f" {status['error']}")

if not buf_snapshot:
    st.info(" Esperando mensajes...")
    time.sleep(AUTO_REFRESH)
    st.rerun()


df = pd.DataFrame(buf_snapshot)
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.sort_values("timestamp")

# ---------------------------------------------------------------------
# selectores
# ---------------------------------------------------------------------
st.sidebar.header("⚙️ Controles")

available_symbols = sorted(df["symbol"].dropna().unique().tolist())
selected_symbol = st.sidebar.selectbox(
    "Par de divisas",
    options=available_symbols,
    index=0 if available_symbols else None,
)

available_sources = sorted(df["source"].dropna().unique().tolist())
selected_sources = st.sidebar.multiselect(
    "Fuente(s)",
    options=available_sources,
    default=available_sources,
)

window_size = st.sidebar.slider(
    "Ticks a mostrar (ventana)",
    min_value=20, max_value=BUFFER_SIZE, value=200, step=20,
)

st.sidebar.markdown("---")
st.sidebar.caption(f"Buffer máx: {BUFFER_SIZE} ticks")
st.sidebar.caption(f"Conectado desde: {status.get('started_at', '—')}")

# ---------------------------------------------------------------------
# Filtrado
# ---------------------------------------------------------------------
df_sym = df[(df["symbol"] == selected_symbol) & (df["source"].isin(selected_sources))]
df_sym = df_sym.tail(window_size)

if df_sym.empty:
    st.warning(f"Sin datos para **{selected_symbol}** con las fuentes seleccionadas. Esperando...")
    time.sleep(AUTO_REFRESH)
    st.rerun()

# Metrica útil: 
df_sym = df_sym.copy()
df_sym["metric"] = df_sym["close"].fillna(df_sym["price"])

# ---------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------
last  = df_sym.iloc[-1]
first = df_sym.iloc[0]
change      = (last["metric"] - first["metric"]) if pd.notna(last["metric"]) and pd.notna(first["metric"]) else 0
change_pct  = (change / first["metric"] * 100) if first["metric"] else 0

col_k1, col_k2, col_k3, col_k4 = st.columns(4)
col_k1.metric(f"💱 {selected_symbol} — último", f"{last['metric']:.5f}" if pd.notna(last['metric']) else "—",
              f"{change:+.5f} ({change_pct:+.3f}%)")
col_k2.metric("🔼 Máximo (ventana)", f"{df_sym['metric'].max():.5f}")
col_k3.metric("🔽 Mínimo (ventana)", f"{df_sym['metric'].min():.5f}")
col_k4.metric("📊 Ticks en ventana", len(df_sym))

# ---------------------------------------------------------------------
# linea de precios
# ---------------------------------------------------------------------
fig = go.Figure()
for src in selected_sources:
    sub = df_sym[df_sym["source"] == src]
    if sub.empty:
        continue
    fig.add_trace(go.Scatter(
        x=sub["timestamp"], y=sub["metric"],
        mode="lines+markers", name=f"{src}",
        hovertemplate="<b>%{x}</b><br>" + src + ": %{y:.5f}<extra></extra>",
    ))

fig.update_layout(
    title=f"Evolución en tiempo real — {selected_symbol}",
    xaxis_title="Timestamp",
    yaxis_title="Precio",
    height=480,
    hovermode="x unified",
    showlegend=True,
)
st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------
# Vista panoramica
# ---------------------------------------------------------------------
st.subheader("🌐 Vista panorámica — último precio por símbolo")
panorama = (
    df.assign(metric=df["close"].fillna(df["price"]))
      .dropna(subset=["metric"])
      .sort_values("timestamp")
      .groupby("symbol")
      .agg(ultimo=("metric", "last"),
           ticks=("metric", "count"),
           ts=("timestamp", "last"),
           fuente=("source", "last"))
      .reset_index()
      .sort_values("ultimo", ascending=False)
)
st.dataframe(panorama, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------
# Tabla de ultimos ticks (texto)
# ---------------------------------------------------------------------
with st.expander(f"🧾 Últimos {min(20, len(df_sym))} ticks de {selected_symbol}"):
    st.dataframe(
        df_sym[["timestamp", "symbol", "source", "open", "high", "low", "close", "price"]].tail(20),
        use_container_width=True, hide_index=True,
    )

# ---------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------
time.sleep(AUTO_REFRESH)
st.rerun()
