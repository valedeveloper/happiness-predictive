"""Paso 6: guardar el original, validar el contrato y generar la predicción."""

import argparse
import base64
import hashlib
import json
import math
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from confluent_kafka import Consumer, KafkaException

ROOT = Path(__file__).resolve().parents[1]
BROKER = "127.0.0.1:9092"
TOPIC = "happiness-events"
GROUP = "happiness-ml-team"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres@localhost:5432/happiness_dw")
# Mismo orden de las seis columnas utilizado durante el entrenamiento.
FEATURES = {
    "gdp": "gdp_per_capita", "family": "social_support",
    "health": "healthy_life_expectancy", "freedom": "freedom",
    "generosity": "generosity", "corruption": "corruption",
}
REQUIRED = {"event_id", "country", "year", *FEATURES, "actual_happiness_score", "event_time"}


def now():
    return datetime.now(timezone.utc)


def parse_event_time(value):
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    # Convención del ejercicio: una hora sin zona se interpreta como UTC.
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def validate_event(event, key):
    # Contrato: campos obligatorios, tipos correctos y strings no vacíos.
    if not isinstance(event, dict):
        return "INVALID_SCHEMA", "El JSON debe ser un objeto."
    missing = REQUIRED - event.keys()
    if missing:
        return "INVALID_SCHEMA", f"Campos obligatorios ausentes: {', '.join(sorted(missing))}."
    for field in ["event_id", "country", "event_time"]:
        if not isinstance(event[field], str):
            return "INVALID_SCHEMA", f"{field} debe ser string."
        if not event[field].strip() or "\x00" in event[field]:
            return "INVALID_VALUES", f"{field} no puede estar vacío ni contener caracteres nulos."
        try:
            event[field].encode("utf-8")
        except UnicodeEncodeError:
            return "INVALID_VALUES", f"{field} contiene Unicode inválido."
    if type(event["year"]) is not int:
        return "INVALID_SCHEMA", "year debe ser entero."
    if not 2015 <= event["year"] <= 2019:
        return "INVALID_VALUES", "year no corresponde al histórico 2015–2019."
    for field in [*FEATURES, "actual_happiness_score"]:
        value = event[field]
        if type(value) not in (int, float):
            return "INVALID_SCHEMA", f"{field} debe ser numérico."
        try:
            if not math.isfinite(value) or not 0 <= value < 3.4028234663852886e38:
                return "INVALID_VALUES", f"{field} no es un número válido para el modelo."
        except OverflowError:
            return "INVALID_VALUES", f"{field} excede el rango numérico."
    if event["actual_happiness_score"] > 10:
        return "INVALID_VALUES", "El puntaje real debe estar entre 0 y 10."
    try:
        if "T" not in event["event_time"]:
            raise ValueError("Falta la hora de emisión.")
        parse_event_time(event["event_time"])
    except (ValueError, OverflowError):
        return "INVALID_VALUES", "event_time debe ser una fecha y hora ISO-8601 válida."
    if key != event["country"].encode("utf-8"):
        return "INVALID_VALUES", "La clave Kafka no coincide con country."
    return "VALID", None


def open_database(database_url=DATABASE_URL):
    # Autocommit evita una transacción implícita que retenga el original sin confirmar.
    db = psycopg.connect(database_url, autocommit=True, row_factory=dict_row)
    try:
        with db.transaction():
            db.execute((ROOT / "sql/streaming.sql").read_text(encoding="utf-8"), prepare=False)
            # Región del histórico preparado; no se añade al evento ni a los predictores.
            countries = pd.read_csv(ROOT / "data/processed/happiness_analysis.csv",
                                    usecols=["country", "region"]).drop_duplicates()
            if countries.groupby("country").region.nunique().gt(1).any():
                raise ValueError("Un país tiene regiones históricas contradictorias.")
            with db.cursor() as cursor:
                cursor.executemany("""INSERT INTO dim_country(country_name, region) VALUES (%s, %s)
                    ON CONFLICT (country_name) DO UPDATE SET region=EXCLUDED.region""",
                    countries.itertuples(index=False, name=None))
        db.execute("SET TIME ZONE 'UTC'")
    except Exception:
        db.close()
        raise
    return db


def process_message(db, model, message, model_info):
    # Paso 6: confirmar el original en PostgreSQL ANTES de interpretar su JSON.
    timestamp_type, timestamp_ms = message.timestamp()
    headers = json.dumps([[name, None if value is None else base64.b64encode(value).decode()]
                          for name, value in (message.headers() or [])])
    with db.transaction():
        db.execute("""INSERT INTO raw_happiness_events
            (topic, partition_id, offset_id, raw_key, raw_value, headers_json,
             kafka_timestamp_type, kafka_timestamp_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(topic, partition_id, offset_id) DO NOTHING""",
            (message.topic(), message.partition(), message.offset(), message.key(),
             message.value(), headers, timestamp_type, timestamp_ms))
    raw = db.execute("""SELECT * FROM raw_happiness_events
        WHERE topic=%s AND partition_id=%s AND offset_id=%s""",
        (message.topic(), message.partition(), message.offset())).fetchone()
    if raw["raw_value"] != message.value() or raw["raw_key"] != message.key():
        raise RuntimeError("La posición Kafka ya existe con otros bytes; conserve juntos tópico y base.")
    if raw["processing_status"] != "RECEIVED":
        return raw["processing_status"]

    event = None
    try:
        event = json.loads(message.value().decode("utf-8"))
        status, error = validate_event(event, message.key())
    except (AttributeError, UnicodeError, ValueError, RecursionError) as exc:
        status, error = "INVALID_SCHEMA", str(exc)
    event_id = event["event_id"] if status == "VALID" else None

    # Paso 8: dimensiones, hecho y estado final se confirman en una transacción.
    with db.transaction():
        current = db.execute("""SELECT processing_status FROM raw_happiness_events
            WHERE raw_event_id=%s FOR UPDATE""", (raw["raw_event_id"],)).fetchone()
        if current["processing_status"] != "RECEIVED":
            return current["processing_status"]
        if status == "VALID":
            # Serializa identificadores iguales sin una tabla técnica adicional.
            db.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (event_id,))
            duplicate = db.execute("""SELECT 1 FROM raw_happiness_events WHERE event_id=%s
                AND processing_status IN ('VALID','PREDICTION_ERROR')""", (event_id,)).fetchone()
            if duplicate:
                status, error = "INVALID_VALUES", "event_id duplicado."
            else:
                # Mismo orden y pipeline entrenado. actual_happiness_score NO entra en X.
                try:
                    inputs = {column: float(event[field]) for field, column in FEATURES.items()}
                    frame = pd.DataFrame([inputs], columns=list(FEATURES.values()))
                    prediction = float(model.predict(frame)[0])
                    if not math.isfinite(prediction):
                        raise ValueError("El modelo devolvió una predicción no finita.")
                except Exception as exc:
                    status, error = "PREDICTION_ERROR", str(exc)
                else:
                    country = db.execute("SELECT country_id FROM dim_country WHERE country_name=%s",
                                         (event["country"],)).fetchone()
                    if country is None:
                        country = db.execute("""INSERT INTO dim_country(country_name, region)
                            VALUES (%s, 'Desconocido') ON CONFLICT(country_name)
                            DO UPDATE SET country_name=EXCLUDED.country_name RETURNING country_id""",
                            (event["country"],)).fetchone()
                    event_time = parse_event_time(event["event_time"])
                    date_id = int(event_time.strftime("%Y%m%d"))
                    db.execute("""INSERT INTO dim_date(date_id, calendar_date) VALUES (%s, %s)
                        ON CONFLICT(date_id) DO NOTHING""", (date_id, event_time.date()))
                    # Granularidad: una fila por evento válido que genera una predicción.
                    # prediction_error se guarda automáticamente como real - predicho.
                    db.execute("""INSERT INTO fact_predictions
                        (raw_event_id, country_id, date_id, observation_year, actual_score,
                         predicted_score, event_time, prediction_timestamp, model_name, model_sha256)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (raw["raw_event_id"], country["country_id"], date_id, event["year"],
                         float(event["actual_happiness_score"]), prediction, event_time, now(),
                         model_info["name"], model_info["sha256"]))
        if error:
            error = error.encode("utf-8", errors="backslashreplace").decode().replace("\x00", "\\0")
        db.execute("""UPDATE raw_happiness_events SET processing_status=%s, error_message=%s,
            event_id=%s, processed_at=%s WHERE raw_event_id=%s""",
            (status, error, event_id, now(), raw["raw_event_id"]))
    return status


def consume_events(database_url=DATABASE_URL):
    # Requisito: cargar una sola vez el pipeline entrenado, sin volver a ajustarlo.
    model_bytes = (ROOT / "models/model.pkl").read_bytes()
    model = pickle.loads(model_bytes)
    if model.feature_names_in_.tolist() != list(FEATURES.values()):
        raise ValueError("El orden de columnas no coincide con el modelo entrenado.")
    metadata = json.loads((ROOT / "models/model_metadata.json").read_text(encoding="utf-8"))
    model_info = {"name": metadata["selected_model"], "sha256": hashlib.sha256(model_bytes).hexdigest()}
    db = open_database(database_url)
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP,  # Requisito: grupo happiness-ml-team.
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    print(f"Escuchando {TOPIC}; grupo {GROUP}. Ctrl+C para detener.")
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            status = process_message(db, model, message, model_info)
            # Confirmar solo después de guardar el original y su resultado/estado.
            consumer.commit(message=message, asynchronous=False)
            print(f"{message.topic()}[{message.partition()}]@{message.offset()}: {status}")
    except KeyboardInterrupt:
        print("Consumidor detenido.")
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=DATABASE_URL, help="Conexión PostgreSQL; también admite DATABASE_URL.")
    consume_events(parser.parse_args().database_url)
