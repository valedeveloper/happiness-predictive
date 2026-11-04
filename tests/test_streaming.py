"""Pruebas del productor y del consumidor, sin depender de un broker activo."""

import json
import os
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from kafka.consumer import DATABASE_URL, FEATURES, open_database, process_message, validate_event
from kafka.producer import DATA_PATH, TOPIC, iter_events, publish_events


@unittest.skipUnless(os.getenv("RUN_POSTGRES_TESTS") == "1", "Requiere PostgreSQL y RUN_POSTGRES_TESTS=1")
class StreamingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = "test_streaming_" + uuid.uuid4().hex
        cls.admin = psycopg.connect(DATABASE_URL, autocommit=True)
        cls.admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(cls.schema)))
        cls.url = make_conninfo(DATABASE_URL, options=f"-csearch_path={cls.schema}")
        cls.db = open_database(cls.url)

    @classmethod
    def tearDownClass(cls):
        cls.db.close()
        cls.admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(cls.schema)))
        cls.admin.close()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db.execute("TRUNCATE fact_predictions, raw_happiness_events, dim_date RESTART IDENTITY CASCADE")
        self.event = next(iter_events())
        self.model = Mock()
        self.model.predict.return_value = [6.0]
        self.info = {"name": "test", "sha256": "test"}

    def tearDown(self):
        self.directory.cleanup()

    def message(self, event=None, offset=0, value=None):
        message = Mock()
        message.topic.return_value = TOPIC
        message.partition.return_value = 0
        message.offset.return_value = offset
        message.key.return_value = self.event["country"].encode()
        message.value.return_value = value if value is not None else json.dumps(event or self.event).encode()
        message.timestamp.return_value = (1, 123)
        message.headers.return_value = []
        return message

    def test_all_prepared_events_satisfy_contract_and_have_unique_ids(self):
        events = list(iter_events())
        self.assertEqual(len(events), 782)
        self.assertEqual(len({e["event_id"] for e in events}), 782)
        for event in events:
            self.assertEqual(validate_event(event, event["country"].encode())[0], "VALID")
        self.assertNotEqual(events[0]["event_id"], next(iter_events())["event_id"])

    def test_missing_fields_types_and_values(self):
        key = self.event["country"].encode()
        for field in self.event:
            event = self.event.copy()
            del event[field]
            self.assertEqual(validate_event(event, key)[0], "INVALID_SCHEMA")
        for field, value, status in [
            ("year", "2019", "INVALID_SCHEMA"), ("year", True, "INVALID_SCHEMA"),
            ("gdp", None, "INVALID_SCHEMA"), ("family", "0.8", "INVALID_SCHEMA"),
            ("country", " ", "INVALID_VALUES"), ("gdp", -1, "INVALID_VALUES"),
            ("health", float("inf"), "INVALID_VALUES"), ("gdp", float("nan"), "INVALID_VALUES"),
            ("event_time", "2026-02-30T10:00:00", "INVALID_VALUES"),
        ]:
            self.assertEqual(validate_event({**self.event, field: value}, key)[0], status)
        self.assertEqual(validate_event(self.event, b"wrong-country")[0], "INVALID_VALUES")

    def test_raw_is_committed_before_decoding(self):
        original_loads = json.loads
        message = self.message(value=b'  {"missing": true}  ')

        def check_persistence(value):
            with psycopg.connect(self.url, autocommit=True, row_factory=dict_row) as other:
                row = other.execute("SELECT raw_value, processing_status FROM raw_happiness_events").fetchone()
            self.assertEqual((row["raw_value"], row["processing_status"]), (message.value(), "RECEIVED"))
            return original_loads(value)

        with patch("kafka.consumer.json.loads", side_effect=check_persistence):
            self.assertEqual(process_message(self.db, self.model, message, self.info), "INVALID_SCHEMA")
        self.model.predict.assert_not_called()

    def test_prediction_uses_only_six_ordered_features(self):
        self.assertEqual(process_message(self.db, self.model, self.message(), self.info), "VALID")
        frame = self.model.predict.call_args.args[0]
        self.assertEqual(frame.columns.tolist(), list(FEATURES.values()))
        self.assertNotIn("actual_happiness_score", frame.columns)
        row = self.db.execute("SELECT * FROM fact_predictions").fetchone()
        self.assertEqual(row["predicted_score"], 6.0)
        self.assertAlmostEqual(row["prediction_error"], self.event["actual_happiness_score"] - 6.0)

    def test_missing_model_feature_is_identified(self):
        event = self.event.copy()
        del event["health"]
        status, reason = validate_event(event, event["country"].encode())
        self.assertEqual(status, "INVALID_SCHEMA")
        self.assertIn("health", reason)

    def test_invalid_records_are_preserved_and_never_predicted(self):
        missing = self.event.copy()
        del missing["family"]
        cases = [
            (json.dumps(missing).encode(), "INVALID_SCHEMA"),
            (json.dumps({**self.event, "gdp": "1.2"}).encode(), "INVALID_SCHEMA"),
            (json.dumps({**self.event, "gdp": -1}).encode(), "INVALID_VALUES"),
            (json.dumps({**self.event, "health": float("nan")}).encode(), "INVALID_VALUES"),
            (b"\\xff", "INVALID_SCHEMA"),
            (None, "INVALID_SCHEMA"),
        ]
        for offset, (payload, expected) in enumerate(cases):
            with self.subTest(offset=offset):
                message = self.message(offset=offset)
                message.value.return_value = payload
                self.assertEqual(process_message(self.db, self.model, message, self.info), expected)
                raw = self.db.execute("SELECT raw_value, processing_status FROM raw_happiness_events WHERE offset_id=%s",
                                      (offset,)).fetchone()
                self.assertEqual((raw["raw_value"], raw["processing_status"]), (payload, expected))
        self.model.predict.assert_not_called()
        self.assertEqual(self.db.execute("SELECT COUNT(*) AS n FROM fact_predictions").fetchone()["n"], 0)

    def test_invalid_and_prediction_error_allow_next_event(self):
        self.assertEqual(process_message(self.db, self.model, self.message(value=b"not-json"), self.info), "INVALID_SCHEMA")
        self.model.predict.side_effect = RuntimeError("failed")
        self.assertEqual(process_message(self.db, self.model, self.message(offset=1), self.info), "PREDICTION_ERROR")
        self.model.predict.side_effect = None
        event = {**self.event, "event_id": "next-event"}
        self.assertEqual(process_message(self.db, self.model, self.message(event, 2), self.info), "VALID")

    def test_redelivery_and_duplicate_identifiers(self):
        message = self.message()
        process_message(self.db, self.model, message, self.info)
        process_message(self.db, self.model, message, self.info)
        self.model.predict.assert_called_once()
        self.assertEqual(process_message(self.db, self.model, self.message(offset=1), self.info), "INVALID_VALUES")
        self.assertEqual(self.db.execute("SELECT COUNT(*) AS n FROM fact_predictions").fetchone()["n"], 1)

    def test_fact_dates_country_and_traceability(self):
        event = {**self.event, "year": 2019, "event_time": "2026-10-30T23:30:00-05:00"}
        message = self.message(event)
        self.assertEqual(process_message(self.db, self.model, message, self.info), "VALID")
        row = self.db.execute("SELECT * FROM v_prediction_detail").fetchone()
        self.assertEqual(row["observation_year"], 2019)
        self.assertEqual(row["date_id"], 20261031)
        self.assertEqual(row["country_name"], self.event["country"])
        self.assertEqual(row["region"], "Western europe")
        raw = self.db.execute("SELECT raw_value FROM raw_happiness_events WHERE raw_event_id=%s",
                              (row["raw_event_id"],)).fetchone()
        self.assertEqual(raw["raw_value"], message.value())
        self.assertIsNotNone(row["prediction_timestamp"].tzinfo)
        date = self.db.execute("SELECT * FROM dim_date").fetchone()
        self.assertEqual((date["year"], date["month"], date["day"], date["quarter"]), (2026, 10, 31, 4))

    def test_analytical_kpis(self):
        self.model.predict.return_value = [7.0]
        for offset, actual in enumerate([6.0, 8.0]):
            event = {**self.event, "event_id": f"metrics-{offset}", "actual_happiness_score": actual}
            process_message(self.db, self.model, self.message(event, offset), self.info)
        metrics = self.db.execute("SELECT * FROM v_prediction_metrics").fetchone()
        self.assertEqual((metrics["mae"], metrics["rmse"], metrics["r2"]), (1.0, 1.0, 0.0))
        self.assertEqual((metrics["mean_actual_score"], metrics["mean_predicted_score"]), (7.0, 7.0))
        self.assertEqual(metrics["error_over_05_pct"], 100)
        self.assertEqual(self.db.execute("SELECT COUNT(*) AS n FROM v_prediction_metrics").fetchone()["n"], 3)
        self.assertEqual(self.db.execute("SELECT * FROM v_country_error_ranking").fetchone()["error_rank"], 1)
        self.assertEqual(self.db.execute("SELECT * FROM v_prediction_activity_daily").fetchone()["predictions"], 2)
        quality = self.db.execute("SELECT * FROM v_event_quality_daily").fetchone()
        self.assertEqual((quality["received_events"], quality["predicted_events"]), (2, 2))

    def test_raw_bytes_cannot_be_modified(self):
        process_message(self.db, self.model, self.message(), self.info)
        with self.assertRaises(psycopg.errors.RaiseException):
            self.db.execute("UPDATE raw_happiness_events SET raw_value=%s", (b"changed",))
        self.assertEqual(self.db.execute("SELECT COUNT(*) AS n FROM fact_predictions").fetchone()["n"], 1)

    def test_concurrent_duplicate_has_only_one_fact(self):
        def process(offset):
            with psycopg.connect(self.url, autocommit=True, row_factory=dict_row) as db:
                return process_message(db, self.model, self.message(offset=offset), self.info)
        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses = list(pool.map(process, [0, 1]))
        self.assertEqual(sorted(statuses), ["INVALID_VALUES", "VALID"])
        self.assertEqual(self.db.execute("SELECT COUNT(*) AS n FROM fact_predictions").fetchone()["n"], 1)

    def test_failed_fact_transaction_preserves_raw_for_retry(self):
        self.db.execute("""CREATE FUNCTION fail_fact() RETURNS TRIGGER LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'simulated database failure'; END; $$;
            CREATE TRIGGER fail_fact BEFORE INSERT ON fact_predictions
            FOR EACH ROW EXECUTE FUNCTION fail_fact();""", prepare=False)
        try:
            with self.assertRaises(psycopg.errors.RaiseException):
                process_message(self.db, self.model, self.message(), self.info)
            raw = self.db.execute("SELECT * FROM raw_happiness_events").fetchone()
            self.assertEqual(raw["processing_status"], "RECEIVED")
            self.assertEqual(self.db.execute("SELECT COUNT(*) AS n FROM fact_predictions").fetchone()["n"], 0)
            self.assertEqual(self.db.execute("SELECT COUNT(*) AS n FROM dim_date").fetchone()["n"], 0)
        finally:
            self.db.execute("DROP TRIGGER fail_fact ON fact_predictions; DROP FUNCTION fail_fact();", prepare=False)
        self.assertEqual(process_message(self.db, self.model, self.message(), self.info), "VALID")

    def test_producer_sends_json_country_key_delay_and_topic_topology(self):
        path = Path(self.directory.name) / "sample.csv"
        pd.read_csv(DATA_PATH).head(2).to_csv(path, index=False)
        admin = Mock()
        admin.create_topics.return_value = {TOPIC: Mock()}
        topic = SimpleNamespace(error=None, partitions={i: SimpleNamespace(replicas=[1]) for i in range(3)})
        admin.list_topics.side_effect = [SimpleNamespace(topics={}), SimpleNamespace(topics={TOPIC: topic})]
        producer = Mock()
        producer.flush.return_value = 0
        with patch("kafka.producer.AdminClient", return_value=admin), patch("kafka.producer.Producer", return_value=producer), patch("kafka.producer.time.sleep") as sleep:
            self.assertEqual(publish_events(.25, path), 2)
        created = admin.create_topics.call_args.args[0][0]
        self.assertEqual((created.topic, created.num_partitions, created.replication_factor), (TOPIC, 3, 1))
        for call in producer.produce.call_args_list:
            event = json.loads(call.kwargs["value"])
            self.assertEqual(call.args[0], TOPIC)
            self.assertEqual(call.kwargs["key"], event["country"].encode())
        self.assertEqual(sleep.call_args_list[0].args, (.25,))

    def test_event_mapping_matches_saved_model(self):
        import pickle
        from kafka.producer import ROOT
        with (ROOT / "models/model.pkl").open("rb") as file:
            model = pickle.load(file)
        inputs = {column: self.event[field] for field, column in FEATURES.items()}
        expected = model.predict(pd.read_csv(DATA_PATH)[list(FEATURES.values())].head(1))[0]
        actual = model.predict(pd.DataFrame([inputs], columns=list(FEATURES.values())))[0]
        self.assertAlmostEqual(actual, expected, places=12)


if __name__ == "__main__":
    unittest.main()
