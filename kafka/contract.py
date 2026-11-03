"""Contrato de negocio estricto, aplicado después de persistir el mensaje crudo."""

import json
import math
import re
from datetime import datetime

from .config import EVENT_TO_MODEL

REQUIRED_FIELDS = {
    "event_id", "country", "year", *EVENT_TO_MODEL,
    "actual_happiness_score", "event_time",
}
FLOAT32_MAX = 3.4028234663852886e38
TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?")


class ContractError(ValueError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ContractError("INVALID_SCHEMA", f"Campo JSON repetido: {key!r}")
        result[key] = value
    return result


def _non_json_number(value):
    raise ContractError("INVALID_VALUES", f"Constante numérica no válida en JSON: {value}")


def decode_event(raw_value):
    if not isinstance(raw_value, bytes):
        raise ContractError("INVALID_SCHEMA", "El mensaje no contiene un valor JSON (tombstone).")
    try:
        event = json.loads(raw_value.decode("utf-8"), object_pairs_hook=_unique_object,
                           parse_constant=_non_json_number)
    except ContractError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise ContractError("INVALID_SCHEMA", f"JSON UTF-8 inválido: {exc}") from exc
    if not isinstance(event, dict):
        raise ContractError("INVALID_SCHEMA", "Se espera un objeto JSON.")
    return event


def validate_event(event, raw_key):
    missing = REQUIRED_FIELDS - event.keys()
    extra = event.keys() - REQUIRED_FIELDS
    if missing or extra:
        raise ContractError("INVALID_SCHEMA", f"Campos ausentes={sorted(missing)}; adicionales={sorted(extra)}")
    for field in ["event_id", "country", "event_time"]:
        if not isinstance(event[field], str):
            raise ContractError("INVALID_SCHEMA", f"{field} debe ser string.")
        if not event[field].strip():
            raise ContractError("INVALID_VALUES", f"{field} no puede estar vacío.")
        try:
            event[field].encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ContractError("INVALID_VALUES", f"{field} contiene Unicode inválido.") from exc
    if type(event["year"]) is not int:
        raise ContractError("INVALID_SCHEMA", "year debe ser entero; no se aceptan bool, float ni texto.")
    if not 2015 <= event["year"] <= 2019:
        raise ContractError("INVALID_VALUES", "year debe corresponder al histórico 2015–2019.")
    for field in [*EVENT_TO_MODEL, "actual_happiness_score"]:
        value = event[field]
        if type(value) not in (int, float):
            raise ContractError("INVALID_SCHEMA", f"{field} debe ser numérico; no bool, texto ni null.")
        try:
            valid = math.isfinite(value) and 0 <= value < FLOAT32_MAX
        except (OverflowError, ValueError):
            valid = False
        if not valid:
            raise ContractError("INVALID_VALUES", f"{field} debe ser finito, no negativo y compatible con float32.")
    if not 0 <= event["actual_happiness_score"] <= 10:
        raise ContractError("INVALID_VALUES", "actual_happiness_score debe estar entre 0 y 10.")
    timestamp = event["event_time"]
    try:
        if not TIMESTAMP.fullmatch(timestamp):
            raise ValueError("Se requiere fecha y hora ISO-8601, separadas por T.")
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError("INVALID_VALUES", f"event_time inválido: {exc}") from exc
    if raw_key != event["country"].encode("utf-8"):
        raise ContractError("INVALID_VALUES", "La clave Kafka debe coincidir exactamente con country en UTF-8.")
    return event
