"""Prueba optativa con Kafka real; no ejecutar junto a otro consumidor del grupo."""

import json
import os
import sqlite3
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import numpy as np
import pandas as pd
from confluent_kafka import Producer

from kafka.config import BOOTSTRAP_SERVERS, DATABASE_PATH, DATA_PATH, GROUP_ID, PROJECT_ROOT, TOPIC
from kafka.consumer import consume_events
from kafka.inference import ModelPredictor
from kafka.producer import iter_events, publish_events
from kafka.storage import EventStore, utc_now
from kafka.topic import ensure_topic


@unittest.skipUnless(os.getenv("RUN_KAFKA_INTEGRATION") == "1", "Requiere Kafka activo y RUN_KAFKA_INTEGRATION=1")
class KafkaIntegrationTests(unittest.TestCase):
    def test_historical_stream_invalid_events_and_committed_offsets(self):
        # Esta prueba conserva su evidencia en la base local del ejercicio.
        store = EventStore(DATABASE_PATH)
        start_id = store.connection.execute("SELECT COALESCE(MAX(id), 0) FROM raw_happiness_events").fetchone()[0]
        store.close()
        topic = ensure_topic(BOOTSTRAP_SERVERS)
        with ThreadPoolExecutor(max_workers=1) as executor:
            consuming = executor.submit(consume_events, BOOTSTRAP_SERVERS, DATABASE_PATH,
                                        max_messages=782, idle_timeout=30)
            self.assertEqual(publish_events(BOOTSTRAP_SERVERS, delay=.001), 782)
            self.assertEqual(consuming.result(timeout=150), 782)

        with closing(sqlite3.connect(DATABASE_PATH)) as connection:
            connection.row_factory = sqlite3.Row
            raw = connection.execute("SELECT * FROM raw_happiness_events WHERE id>? ORDER BY id", (start_id,)).fetchall()
            self.assertEqual(len(raw), 782)
            self.assertTrue(all(row["processing_status"] == "VALID" for row in raw))
            historical = pd.read_sql_query(
                """SELECT p.* FROM happiness_predictions p JOIN raw_happiness_events r
                   ON r.id=p.raw_event_id WHERE r.id>? ORDER BY r.id""", connection, params=(start_id,))
            self.assertEqual(len(historical), 782)
        per_country = {}
        payloads = {}
        for row in raw:
            event = json.loads(row["raw_value"].decode("utf-8"))
            self.assertEqual(row["raw_key"], event["country"].encode("utf-8"))
            per_country.setdefault(event["country"], set()).add(row["partition_id"])
            payloads[event["event_id"]] = event
        self.assertTrue(all(len(partitions) == 1 for partitions in per_country.values()))
        predictor = ModelPredictor()
        frame = pd.DataFrame([predictor.feature_values(payloads[event_id]) for event_id in historical.event_id],
                             columns=predictor.features)
        np.testing.assert_allclose(historical.predicted_happiness_score, predictor.model.predict(frame),
                                   rtol=1e-12, atol=1e-12)

        run = uuid.uuid4().hex
        base = next(iter_events(DATA_PATH))

        def payload(number, **changes):
            return json.dumps({**base, "event_id": f"CHECK-{run}-{number}", **changes}, ensure_ascii=False).encode("utf-8")

        missing = {**base, "event_id": f"CHECK-{run}-missing"}
        del missing["gdp"]
        probes = [
            (b"{not-json", base["country"].encode(), "INVALID_SCHEMA"),
            (json.dumps(missing).encode(), base["country"].encode(), "INVALID_SCHEMA"),
            (payload(3, family="bad"), base["country"].encode(), "INVALID_SCHEMA"),
            (payload(4, gdp=-1), base["country"].encode(), "INVALID_VALUES"),
            (payload(5, gdp=float("nan")), base["country"].encode(), "INVALID_VALUES"),
            (payload(6), b"wrong-country", "INVALID_VALUES"),
            (raw[0]["raw_value"], raw[0]["raw_key"], "INVALID_VALUES"),
            (None, None, "INVALID_SCHEMA"),
            (payload(9), base["country"].encode(), "VALID"),
        ]
        producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": True})
        delivered = []

        def on_delivery(error, message):
            self.assertIsNone(error)
            delivered.append((message.partition(), message.offset()))

        for value, key, _ in probes:
            producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
            self.assertEqual(producer.flush(30), 0)
        self.assertEqual(consume_events(BOOTSTRAP_SERVERS, DATABASE_PATH,
                                       max_messages=len(probes), idle_timeout=20), len(probes))
        with closing(sqlite3.connect(DATABASE_PATH)) as connection:
            for (value, key, expected), (partition, offset) in zip(probes, delivered):
                row = connection.execute(
                    """SELECT raw_value, raw_key, processing_status FROM raw_happiness_events
                       WHERE topic=? AND partition_id=? AND offset_id=?""", (TOPIC, partition, offset)
                ).fetchone()
                self.assertEqual(row, (value, key, expected))
            statuses = dict(connection.execute(
                "SELECT processing_status, COUNT(*) FROM raw_happiness_events WHERE id>? GROUP BY processing_status",
                (start_id,)
            ).fetchall())
            self.assertEqual(statuses, {"VALID": 783, "INVALID_SCHEMA": 4, "INVALID_VALUES": 4})
            count = connection.execute(
                "SELECT COUNT(*) FROM happiness_predictions WHERE raw_event_id>?", (start_id,)
            ).fetchone()[0]
            self.assertEqual(count, 783)
        # Nuevo consumidor del mismo grupo: ya no recibe los mensajes confirmados.
        self.assertEqual(consume_events(BOOTSTRAP_SERVERS, DATABASE_PATH, idle_timeout=5), 0)
        report = {
            "verified_at": utc_now(), "topic": TOPIC, "partitions": len(topic.partitions),
            "replication_factor": 1, "consumer_group": GROUP_ID,
            "historical_events": 782, "probe_events": len(probes),
            "raw_events": 791, "statuses": statuses, "predictions": count,
            "raw_bytes_preserved": True, "model_predictions_match_batch": True,
            "one_partition_per_country": True, "messages_after_restart_without_new_events": 0,
            "model_sha256": predictor.sha256,
            "note": "Simulación histórica con model.pkl reentrenado en 2015–2019; no es evaluación independiente.",
        }
        destination = PROJECT_ROOT / "data/reports/streaming/verification.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
