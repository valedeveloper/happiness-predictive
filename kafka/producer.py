"""Publica cada fila histórica como un evento JSON con clave country."""

import argparse
import csv
import json
import logging
import math
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer

from .config import BOOTSTRAP_SERVERS, DATA_PATH, EVENT_TO_MODEL, TOPIC
from .contract import validate_event
from .storage import utc_now
from .topic import ensure_topic

LOGGER = logging.getLogger(__name__)


def iter_events(data_path, run_id=None):
    run_id = run_id or uuid.uuid4().hex
    with Path(data_path).open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        required = {"country", "year", "happiness_score", *EVENT_TO_MODEL.values()}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f"El CSV no contiene las columnas de entrenamiento: {sorted(required)}")
        for number, row in enumerate(reader, 1):
            # Lectura fila a fila; el productor no necesita cargar todo el CSV.
            yield {
                "event_id": f"E{run_id}-{number:06d}",
                "country": row["country"],
                "year": int(row["year"]),
                **{source: float(row[name]) for source, name in EVENT_TO_MODEL.items()},
                "actual_happiness_score": float(row["happiness_score"]),
                "event_time": utc_now(),
            }


def publish_events(bootstrap_servers, data_path=DATA_PATH, delay=0.1, limit=None):
    if not math.isfinite(delay) or delay < 0:
        raise ValueError("delay debe ser finito y no negativo.")
    if limit is not None and limit < 1:
        raise ValueError("limit debe ser positivo.")
    ensure_topic(bootstrap_servers)
    producer = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True,
                         "acks": "all", "message.timeout.ms": 30000})
    sent = 0
    try:
        for event in iter_events(data_path):
            key = event["country"].encode("utf-8")
            validate_event(event, key)
            value = json.dumps(event, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
            delivery_errors = []

            def delivered(error, message):
                if error is not None:
                    delivery_errors.append(str(error))
                else:
                    LOGGER.info("Enviado %s | %s | partición=%s offset=%s", event["event_id"],
                                event["country"], message.partition(), message.offset())

            producer.produce(TOPIC, key=key, value=value, on_delivery=delivered)
            # Un registro confirmado antes del siguiente: simulación, no envío masivo.
            remaining = producer.flush(35)
            if remaining or delivery_errors:
                raise RuntimeError(f"Entrega no confirmada: pendientes={remaining}, errores={delivery_errors}")
            sent += 1
            if limit is not None and sent >= limit:
                break
            if delay:
                time.sleep(delay)
    finally:
        producer.flush(5)
    LOGGER.info("Simulación terminada: %s eventos confirmados en %s", sent, TOPIC)
    return sent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--data", type=Path, default=DATA_PATH)
    parser.add_argument("--delay", type=float, default=0.1, help="Segundos entre registros (por defecto: 0.1).")
    parser.add_argument("--limit", type=int, help="Límite opcional para una demostración corta.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        publish_events(args.bootstrap_servers, args.data, args.delay, args.limit)
    except KeyboardInterrupt:
        LOGGER.info("Productor detenido por el usuario.")


if __name__ == "__main__":
    main()
