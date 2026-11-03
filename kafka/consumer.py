"""Consume, conserva bytes originales, valida, predice y persiste resultados."""

import argparse
import logging
import math
import signal
import threading
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaException

from .config import BOOTSTRAP_SERVERS, DATABASE_PATH, GROUP_ID, METADATA_PATH, MODEL_PATH, TOPIC
from .inference import ModelPredictor
from .processing import process_message
from .storage import EventStore, IncomingMessage
from .topic import ensure_topic

LOGGER = logging.getLogger(__name__)


def consume_events(bootstrap_servers, database=DATABASE_PATH, model_path=MODEL_PATH,
                   metadata_path=METADATA_PATH, max_messages=None, idle_timeout=0, stop=None):
    if max_messages is not None and max_messages < 1:
        raise ValueError("max_messages debe ser positivo.")
    if not math.isfinite(idle_timeout) or idle_timeout < 0:
        raise ValueError("idle_timeout debe ser finito y no negativo.")
    predictor = ModelPredictor(model_path, metadata_path)
    ensure_topic(bootstrap_servers)
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "allow.auto.create.topics": False,
    })
    store = EventStore(database)
    stop = stop or threading.Event()
    count = 0
    try:
        consumer.subscribe([TOPIC])
        LOGGER.info("Grupo=%s | tópico=%s | modelo=%s | base=%s", GROUP_ID, TOPIC, predictor.name, database)
        last_received = time.monotonic()
        while not stop.is_set():
            message = consumer.poll(1.0)
            if message is None:
                if idle_timeout and time.monotonic() - last_received >= idle_timeout:
                    break
                continue
            if message.error():
                raise KafkaException(message.error())
            last_received = time.monotonic()
            # save_raw hace COMMIT antes de deserializar, validar o inferir.
            status = process_message(store, predictor, IncomingMessage.from_kafka(message))
            # Solo se avanza después de que datos y estado son durables.
            offsets = consumer.commit(message=message, asynchronous=False)
            for offset in offsets or []:
                if offset.error:
                    raise KafkaException(offset.error)
            count += 1
            LOGGER.info("Procesado %s[%s]@%s | %s", message.topic(), message.partition(), message.offset(), status)
            if max_messages is not None and count >= max_messages:
                break
    finally:
        consumer.close()
        store.close()
    LOGGER.info("Consumidor terminado: %s mensajes procesados", count)
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--database", type=Path, default=DATABASE_PATH)
    parser.add_argument("--model", type=Path, default=MODEL_PATH)
    parser.add_argument("--metadata", type=Path, default=METADATA_PATH)
    parser.add_argument("--max-messages", type=int, help="Detener después de N mensajes (válidos o inválidos).")
    parser.add_argument("--idle-timeout", type=float, default=0, help="Salir tras N segundos sin eventos; 0 espera continuamente.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    try:
        consume_events(args.bootstrap_servers, args.database, args.model, args.metadata,
                       args.max_messages, args.idle_timeout, stop)
    except KeyboardInterrupt:
        LOGGER.info("Consumidor detenido por el usuario.")


if __name__ == "__main__":
    main()
