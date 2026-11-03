"""Procesamiento independiente del transporte, verificable sin un broker activo."""

import logging

from .contract import ContractError, decode_event, validate_event

LOGGER = logging.getLogger(__name__)


def process_message(store, predictor, message):
    raw = store.save_raw(message)
    if raw["processing_status"] != "RECEIVED":
        return raw["processing_status"]

    event_id = None
    try:
        event = decode_event(message.value)
        if isinstance(event.get("event_id"), str):
            event_id = event["event_id"]
        validate_event(event, message.key)
    except ContractError as exc:
        with store.connection:
            store.mark(raw["id"], exc.status, str(exc), event_id)
        return exc.status

    # Predicción + estado + reserva del identificador se confirman juntos.
    # BEGIN IMMEDIATE evita carreras al compartir la base entre consumidores locales.
    with store.connection:
        store.connection.execute("BEGIN IMMEDIATE")
        current = store.connection.execute(
            "SELECT processing_status FROM raw_happiness_events WHERE id=?", (raw["id"],)
        ).fetchone()[0]
        if current != "RECEIVED":
            return current
        claimed = store.connection.execute(
            "SELECT raw_event_id FROM event_claims WHERE event_id=?", (event_id,)
        ).fetchone()
        if claimed is not None:
            store.mark(raw["id"], "INVALID_VALUES", "event_id duplicado en otro mensaje Kafka.", event_id)
            return "INVALID_VALUES"
        store.connection.execute("INSERT INTO event_claims(event_id, raw_event_id) VALUES (?, ?)",
                                 (event_id, raw["id"]))
        try:
            prediction, features = predictor.predict(event)
        except Exception as exc:
            # Los errores de un registro no impiden procesar los mensajes posteriores.
            store.mark(raw["id"], "PREDICTION_ERROR", f"{type(exc).__name__}: {exc}", event_id)
            LOGGER.error("Predicción fallida para %s: %s", event_id, exc)
            return "PREDICTION_ERROR"
        store.save_prediction(raw["id"], event, prediction, features, predictor)
        store.mark(raw["id"], "VALID", event_id=event_id)
    return "VALID"
