"""Contrato, auditoría e inferencia: pruebas locales sin requerir Kafka activo."""

import json
import math
import pickle
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from kafka.config import DATA_PATH, EVENT_TO_MODEL, MODEL_PATH
from kafka.contract import ContractError, decode_event, validate_event
from kafka.inference import ModelPredictor
from kafka.processing import process_message
from kafka.producer import iter_events
from kafka.storage import EventStore, IncomingMessage


def valid_event():
    return {
        "event_id": "E001", "country": "Colombia", "year": 2019,
        "gdp": 1.2, "family": 0.8, "health": 0.9, "freedom": 0.6,
        "generosity": 0.3, "corruption": 0.1, "actual_happiness_score": 6.2,
        "event_time": "2026-10-30T15:30:00",
    }


class FixedPredictor:
    name = "test"
    sha256 = "unit-test"

    def __init__(self):
        self.calls = 0

    def predict(self, event):
        self.calls += 1
        return 6.0, {name: event[source] for source, name in EVENT_TO_MODEL.items()}


class ContractTests(unittest.TestCase):
    def test_required_example_and_timezone(self):
        for timestamp in ["2026-10-30T15:30:00", "2026-10-30T15:30:00Z", "2026-10-30T15:30:00.123-05:00"]:
            event = {**valid_event(), "event_time": timestamp}
            self.assertIs(validate_event(event, b"Colombia"), event)

    def test_all_missing_fields(self):
        for field in valid_event():
            with self.subTest(field=field):
                event = valid_event()
                del event[field]
                with self.assertRaises(ContractError) as context:
                    validate_event(event, b"Colombia")
                self.assertEqual(context.exception.status, "INVALID_SCHEMA")

    def test_wrong_types(self):
        cases = [("year", "2019"), ("year", 2019.0), ("year", True), ("gdp", True),
                 ("family", None), ("health", "0.9"), ("event_id", 123),
                 ("country", []), ("event_time", 123), ("actual_happiness_score", False)]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                with self.assertRaises(ContractError) as context:
                    validate_event({**valid_event(), field: value}, b"Colombia")
                self.assertEqual(context.exception.status, "INVALID_SCHEMA")

    def test_invalid_values(self):
        cases = [("year", 2020), ("gdp", -1), ("gdp", math.nan), ("family", math.inf),
                 ("health", 1e100), ("gdp", 10**400), ("event_id", " "),
                 ("country", ""), ("country", "\ud800"), ("event_id", "\ud800"),
                 ("event_time", "2026-02-30T12:00:00"), ("event_time", "2026-10-30"),
                 ("actual_happiness_score", 10.1), ("actual_happiness_score", -1)]
        for field, value in cases:
            with self.subTest(field=field, value=repr(value)):
                with self.assertRaises(ContractError) as context:
                    validate_event({**valid_event(), field: value}, b"Colombia")
                self.assertEqual(context.exception.status, "INVALID_VALUES")

    def test_bad_json_and_key(self):
        for payload in [None, b"", b"\xff", b"{", b"[]", b"null", b'{"country":"A","country":"B"}',
                        b'{"\\ud800":1,"\\ud800":2}']:
            with self.subTest(payload=payload), self.assertRaises(ContractError):
                decode_event(payload)
        for key in [None, b"Argentina", b"colombia"]:
            with self.subTest(key=key), self.assertRaises(ContractError):
                validate_event(valid_event(), key)
        for constant in [b"NaN", b"Infinity", b"-Infinity"]:
            with self.assertRaises(ContractError) as context:
                decode_event(b'{"gdp":' + constant + b'}')
            self.assertEqual(context.exception.status, "INVALID_VALUES")

    def test_extra_fields_rejected(self):
        with self.assertRaises(ContractError):
            validate_event({**valid_event(), "unexpected": 1}, b"Colombia")

    def test_prepared_historical_data_and_unique_runs(self):
        events = list(iter_events(DATA_PATH))
        self.assertEqual(len(events), 782)
        self.assertEqual(len({e["event_id"] for e in events}), 782)
        for event in events:
            validate_event(event, event["country"].encode("utf-8"))
        other = next(iter_events(DATA_PATH))
        self.assertNotEqual(events[0]["event_id"], other["event_id"])


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "test.db"
        self.store = EventStore(self.path)
        self.predictor = FixedPredictor()

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def message(self, event=None, offset=0, payload=None):
        value = payload if payload is not None else json.dumps(event or valid_event(), indent=2).encode()
        return IncomingMessage("happiness-events", 0, offset, b"Colombia", value,
                               (("x", b"\x00\xff"), ("x", None)), 1, 123)

    def raw(self, offset=0):
        return self.store.connection.execute("SELECT * FROM raw_happiness_events WHERE offset_id=?", (offset,)).fetchone()

    def test_original_bytes_committed_before_json_validation(self):
        message = self.message(payload=b' \n{ "invalid": true }  ')
        original_decode = decode_event

        def check_durable(payload):
            with closing(sqlite3.connect(self.path)) as external:
                row = external.execute("SELECT raw_value, raw_key, processing_status FROM raw_happiness_events").fetchone()
            self.assertEqual(row, (message.value, message.key, "RECEIVED"))
            return original_decode(payload)

        with patch("kafka.processing.decode_event", side_effect=check_durable):
            self.assertEqual(process_message(self.store, self.predictor, message), "INVALID_SCHEMA")
        self.assertEqual(self.predictor.calls, 0)
        self.assertEqual(self.raw()["raw_value"], message.value)

    def test_valid_prediction_and_input_excludes_target(self):
        self.assertEqual(process_message(self.store, self.predictor, self.message()), "VALID")
        row = self.store.connection.execute("SELECT * FROM happiness_predictions").fetchone()
        self.assertAlmostEqual(row["residual"], .2)
        features = json.loads(row["input_features_json"])
        self.assertEqual(list(features), list(EVENT_TO_MODEL.values()))
        self.assertNotIn("actual_happiness_score", features)
        self.assertNotIn("country", features)
        self.assertEqual(json.loads(self.raw()["headers_json"]), [["x", "AP8="], ["x", None]])

    def test_redelivery_is_idempotent(self):
        message = self.message()
        process_message(self.store, self.predictor, message)
        process_message(self.store, self.predictor, message)
        self.assertEqual(self.predictor.calls, 1)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM happiness_predictions").fetchone()[0], 1)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM raw_happiness_events").fetchone()[0], 1)

    def test_duplicate_event_id_at_other_offset_is_audited(self):
        process_message(self.store, self.predictor, self.message())
        self.assertEqual(process_message(self.store, self.predictor, self.message(offset=1)), "INVALID_VALUES")
        self.assertEqual(self.predictor.calls, 1)
        self.assertIn("duplicado", self.raw(1)["error_message"])

    def test_resume_after_raw_commit_without_processing(self):
        message = self.message()
        self.store.save_raw(message)
        self.store.close()
        self.store = EventStore(self.path)
        self.assertEqual(process_message(self.store, self.predictor, message), "VALID")

    def test_invalid_and_prediction_error_do_not_stop_next_records(self):
        for offset, payload, expected in [(0, b'broken', "INVALID_SCHEMA"),
                                          (1, json.dumps({**valid_event(), "gdp": -1}).encode(), "INVALID_VALUES")]:
            self.assertEqual(process_message(self.store, self.predictor, self.message(offset=offset, payload=payload)), expected)
        with patch.object(self.predictor, "predict", side_effect=RuntimeError("model failed")):
            self.assertEqual(process_message(self.store, self.predictor, self.message(offset=2)), "PREDICTION_ERROR")
        event = {**valid_event(), "event_id": "E002"}
        self.assertEqual(process_message(self.store, self.predictor, self.message(event=event, offset=3)), "VALID")

    def test_tombstone_and_invalid_utf8_stored_exactly(self):
        for offset, value in enumerate([None, b"\xff\x00"]):
            message = IncomingMessage("happiness-events", 1, offset, None, value)
            self.assertEqual(process_message(self.store, self.predictor, message), "INVALID_SCHEMA")
            self.assertEqual(self.raw(offset)["raw_value"], value)

    def test_database_write_failure_is_not_swallowed(self):
        with patch.object(self.store, "save_prediction", side_effect=sqlite3.OperationalError("disk full")):
            with self.assertRaises(sqlite3.OperationalError):
                process_message(self.store, self.predictor, self.message())
        self.assertEqual(self.raw()["processing_status"], "RECEIVED")
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM event_claims").fetchone()[0], 0)
        self.assertEqual(process_message(self.store, self.predictor, self.message()), "VALID")

    def test_raw_is_immutable(self):
        self.store.save_raw(self.message())
        with self.assertRaises(sqlite3.IntegrityError):
            with self.store.connection:
                self.store.connection.execute("UPDATE raw_happiness_events SET raw_value=?", (b"modified",))

    def test_invalid_unicode_json_key_is_auditable(self):
        message = self.message(payload=b'{"\\ud800":1,"\\ud800":2}')
        self.assertEqual(process_message(self.store, self.predictor, message), "INVALID_SCHEMA")
        self.assertIsNotNone(self.raw()["error_message"])


class SavedModelTests(unittest.TestCase):
    def test_streaming_mapping_matches_training_and_ignores_actual(self):
        predictor = ModelPredictor()
        events = list(iter_events(DATA_PATH))[:5]
        with MODEL_PATH.open("rb") as file:
            model = pickle.load(file)
        data = pd.read_csv(DATA_PATH)
        expected = model.predict(data[list(EVENT_TO_MODEL.values())].head(5))
        observed = [predictor.predict(event)[0] for event in events]
        np.testing.assert_allclose(observed, expected, rtol=1e-12, atol=1e-12)
        self.assertAlmostEqual(predictor.predict(events[0])[0],
                               predictor.predict({**events[0], "actual_happiness_score": 0})[0], places=12)


if __name__ == "__main__":
    unittest.main()
