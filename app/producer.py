

import json
import os
import time
import signal
import sys

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "quotes_stream")


POSTGRES_URI = os.environ.get(
    "TRADING_DB_URI",
    "postgresql+psycopg2://etl_user:etl_pass@localhost:5433/trading_db",
)

DELAY_SECONDS    = float(os.environ.get("PRODUCER_DELAY", "1.0"))
POLL_INTERVAL    = float(os.environ.get("PRODUCER_POLL_INTERVAL", "10.0"))
BATCH_SIZE       = int(os.environ.get("PRODUCER_BATCH_SIZE", "500"))

_running = True


def _shutdown(signum, frame):
    global _running
    print(f"\n[INFO] Señal {signum} recibida. Cerrando productor...")
    _running = False


signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def _connect_kafka() -> KafkaProducer:
    """Reintenta hasta que Kafka esté disponible."""
    while _running:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                acks="all",
                linger_ms=10,
            )
            print(f"[OK] Conectado a Kafka en {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            print(f"[WAIT] Kafka no disponible en {KAFKA_BOOTSTRAP}, reintentando en 5s...")
            time.sleep(5)


def _fetch_batch(engine, last_id: int, limit: int) -> pd.DataFrame:
    """Lee las próximas N filas de fact_quotes (con dims) desde el cursor."""
    query = text("""
        SELECT
            f.quote_id,
            t.ts        AS timestamp,
            s.symbol,
            s.base_currency,
            s.quote_currency,
            src.source_name,
            f.open, f.high, f.low, f.close, f.price
        FROM fact_quotes f
        JOIN dim_time   t   ON f.time_id   = t.time_id
        JOIN dim_symbol s   ON f.symbol_id = s.symbol_id
        JOIN dim_source src ON f.source_id = src.source_id
        WHERE f.quote_id > :last_id
        ORDER BY f.quote_id ASC
        LIMIT :lim
    """)
    return pd.read_sql(query, engine, params={"last_id": last_id, "lim": limit})


def main():
    print("=" * 60)
    print(" KAFKA PRODUCER - quotes_stream")
    print("=" * 60)
    print(f"Postgres : {POSTGRES_URI}")
    print(f"Kafka    : {KAFKA_BOOTSTRAP}")
    print(f"Topic    : {KAFKA_TOPIC}")
    print(f"Delay    : {DELAY_SECONDS}s por mensaje")
    print(f"Polling  : cada {POLL_INTERVAL}s cuando no hay datos nuevos")
    print("=" * 60)

    engine   = create_engine(POSTGRES_URI, pool_pre_ping=True)
    producer = _connect_kafka()

    last_id    = 0
    total_sent = 0

    while _running:
        try:
            df = _fetch_batch(engine, last_id, BATCH_SIZE)
        except Exception as e:
            print(f"[ERR] Lectura de Postgres falló: {e}")
            time.sleep(POLL_INTERVAL)
            continue

        if df.empty:
            print(f"[WAIT] Sin nuevas filas (cursor={last_id}). Esperando {POLL_INTERVAL}s...")
            time.sleep(POLL_INTERVAL)
            continue

        for _, row in df.iterrows():
            if not _running:
                break
            payload = {
                "quote_id":   int(row["quote_id"]),
                "timestamp":  row["timestamp"].isoformat() if pd.notna(row["timestamp"]) else None,
                "symbol":     row["symbol"],
                "base":       row["base_currency"],
                "quote":      row["quote_currency"],
                "source":     row["source_name"],
                "open":       float(row["open"])  if pd.notna(row["open"])  else None,
                "high":       float(row["high"])  if pd.notna(row["high"])  else None,
                "low":        float(row["low"])   if pd.notna(row["low"])   else None,
                "close":      float(row["close"]) if pd.notna(row["close"]) else None,
                "price":      float(row["price"]) if pd.notna(row["price"]) else None,
            }
         
            producer.send(KAFKA_TOPIC, key=row["symbol"], value=payload)
            last_id      = int(row["quote_id"])
            total_sent  += 1

            if total_sent % 10 == 0:
                print(f"[SENT] total={total_sent}  último={row['symbol']}@{row['timestamp']}")

            time.sleep(DELAY_SECONDS)

        producer.flush()

    producer.flush()
    producer.close()
    print(f"[BYE] Productor cerrado. Total enviados: {total_sent}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
