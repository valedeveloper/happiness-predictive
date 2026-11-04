"""Paso 5: publicar el histórico preparado como eventos de Kafka."""

import argparse
import csv
import json
import math
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data/processed/happiness_modeling.csv"
BROKER = "127.0.0.1:9092"
TOPIC = "happiness-events"  # Requisito: nombre obligatorio del tópico.


def iter_events(data_path=DATA_PATH):
    # Contrato: UUID por ejecución + secuencia = event_id único y no vacío.
    run_id = uuid.uuid4().hex
    with Path(data_path).open(encoding="utf-8-sig", newline="") as file:
        # Requisito: leer el histórico registro por registro, sin cargarlo completo.
        for number, row in enumerate(csv.DictReader(file), start=1):
            if not row["country"].strip():
                raise ValueError("country no puede estar vacío.")
            # Requisito: JSON con los nombres solicitados y tipos del contrato.
            yield {
                "event_id": f"E{run_id}-{number:03d}",
                "country": row["country"],  # Contrato: string no vacío y clave Kafka.
                "year": int(row["year"]),  # Contrato: edición histórica como entero.
                # Contrato: seis valores numéricos del esquema unificado de entrenamiento.
                "gdp": float(row["gdp_per_capita"]),
                "family": float(row["social_support"]),
                "health": float(row["healthy_life_expectancy"]),
                "freedom": float(row["freedom"]),
                "generosity": float(row["generosity"]),
                "corruption": float(row["corruption"]),
                # Contrato: valor real para evaluación; NO es un predictor del modelo.
                "actual_happiness_score": float(row["happiness_score"]),
                # Contrato: hora de emisión del evento en formato ISO-8601 (UTC).
                "event_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }


def publish_events(delay=0.1, data_path=DATA_PATH):
    if not math.isfinite(delay) or delay < 0:
        raise ValueError("La demora debe ser un número finito no negativo.")

    # Requisito: tópico local con 3 particiones y factor de replicación 1.
    admin = AdminClient({"bootstrap.servers": BROKER})
    if TOPIC not in admin.list_topics(timeout=10).topics:
        admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])[TOPIC].result()
    topic = admin.list_topics(topic=TOPIC, timeout=10).topics[TOPIC]
    if topic.error or len(topic.partitions) != 3 or any(len(p.replicas) != 1 for p in topic.partitions.values()):
        raise RuntimeError("happiness-events debe tener 3 particiones y replicación 1.")

    producer = Producer({"bootstrap.servers": BROKER})
    sent = 0
    for event in iter_events(data_path):
        errors = []

        def delivered(error, message):
            if error:
                errors.append(str(error))

        # Requisitos: serializar a JSON y enviar UN evento usando country como clave.
        producer.produce(
            TOPIC,
            key=event["country"].encode("utf-8"),
            value=json.dumps(event, ensure_ascii=False, allow_nan=False).encode("utf-8"),
            on_delivery=delivered,
        )
        # Esperar confirmación del registro antes de continuar con el siguiente.
        if producer.flush(30) or errors:
            raise RuntimeError(f"No se pudo confirmar la entrega: {errors}")
        sent += 1
        print(f"Enviado {event['event_id']}: {event['country']} ({event['year']})")
        time.sleep(delay)  # Requisito: simular llegada con una demora configurable.
    return sent


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delay", type=float, default=0.1, help="Segundos entre registros.")
    parser.add_argument("--data", type=Path, default=DATA_PATH, help="CSV histórico preparado.")
    args = parser.parse_args()
    publish_events(args.delay, args.data)
