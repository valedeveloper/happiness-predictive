"""Persistencia SQLite: los bytes originales se confirman antes de interpretar JSON."""

import base64
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .config import PROJECT_ROOT


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class IncomingMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple = ()
    timestamp_type: int = 0
    timestamp_ms: int | None = None

    @classmethod
    def from_kafka(cls, message):
        timestamp_type, timestamp_ms = message.timestamp()
        return cls(message.topic(), message.partition(), message.offset(), message.key(),
                   message.value(), tuple(message.headers() or ()), timestamp_type, timestamp_ms)


class EventStore:
    def __init__(self, database):
        database = Path(database)
        database.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(database, timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript((PROJECT_ROOT / "sql/streaming.sql").read_text(encoding="utf-8"))

    def close(self):
        self.connection.close()

    def save_raw(self, message):
        # Una lista conserva orden, claves repetidas y valores binarios de headers.
        headers = json.dumps([
            [name, None if value is None else base64.b64encode(value).decode("ascii")]
            for name, value in message.headers
        ], ensure_ascii=True)
        with self.connection:
            self.connection.execute(
                """INSERT INTO raw_happiness_events
                   (topic, partition_id, offset_id, raw_key, raw_value, headers_json,
                    kafka_timestamp_type, kafka_timestamp_ms, received_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(topic, partition_id, offset_id) DO NOTHING""",
                (message.topic, message.partition, message.offset, message.key, message.value,
                 headers, message.timestamp_type, message.timestamp_ms, utc_now()),
            )
        # La transacción de inserción ya terminó: incluso JSON inválido queda durable.
        row = self.connection.execute(
            "SELECT * FROM raw_happiness_events WHERE topic=? AND partition_id=? AND offset_id=?",
            (message.topic, message.partition, message.offset),
        ).fetchone()
        if row["raw_value"] != message.value or row["raw_key"] != message.key:
            raise RuntimeError("La posición Kafka ya existe con bytes diferentes; no reutilice la base tras recrear el tópico.")
        return row

    def mark(self, raw_id, status, error=None, event_id=None):
        self.connection.execute(
            """UPDATE raw_happiness_events SET processing_status=?, error_message=?,
               event_id=?, processed_at=? WHERE id=?""",
            (status, error, event_id, utc_now(), raw_id),
        )

    def save_prediction(self, raw_id, event, prediction, features, predictor):
        actual = float(event["actual_happiness_score"])
        residual = actual - prediction
        self.connection.execute(
            """INSERT INTO happiness_predictions
               (raw_event_id, event_id, country, year, event_time, input_features_json,
                actual_happiness_score, predicted_happiness_score, residual, absolute_error,
                model_name, model_sha256, predicted_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (raw_id, event["event_id"], event["country"], event["year"], event["event_time"],
             json.dumps(features, ensure_ascii=False, allow_nan=False), actual, prediction,
             residual, abs(residual), predictor.name, predictor.sha256, utc_now()),
        )
