"""
Kafka Consumer standalone - útil para verificar que el producer está
publicando bien sin tener que abrir el dashboard de Streamlit.

Uso:
  python app/consumer.py
"""

import json
import os
import signal
import sys

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "quotes_stream")
GROUP_ID        = os.environ.get("CONSUMER_GROUP", "debug-consumer")

_running = True


def _shutdown(signum, frame):
    global _running
    print(f"\n[INFO] Cerrando consumer...")
    _running = False


signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def _connect():
    while _running:
        try:
            c = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",  # leer desde el principio del topic
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
            print(f"[OK] Conectado. Escuchando topic '{KAFKA_TOPIC}'...")
            return c
        except NoBrokersAvailable:
            print(f"[WAIT] Kafka no disponible en {KAFKA_BOOTSTRAP}, reintento en 5s")
            time.sleep(5)


def main():
    print("=" * 60)
    print(f" KAFKA CONSUMER (debug) - topic={KAFKA_TOPIC}")
    print("=" * 60)
    consumer = _connect()
    received = 0
    for msg in consumer:
        if not _running:
            break
        received += 1
        v = msg.value
        print(f"#{received:5d} [{v.get('source'):8s}] {v.get('symbol'):8s} "
              f"@ {v.get('timestamp')} | close={v.get('close')} price={v.get('price')}")
    consumer.close()
    print(f"[BYE] Total recibidos: {received}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
