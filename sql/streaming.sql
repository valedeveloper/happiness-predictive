PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS raw_happiness_events (
    id INTEGER PRIMARY KEY,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id INTEGER NOT NULL,
    raw_key BLOB,
    raw_value BLOB,
    headers_json TEXT NOT NULL,
    kafka_timestamp_type INTEGER NOT NULL,
    kafka_timestamp_ms INTEGER,
    received_at TEXT NOT NULL,
    event_id TEXT,
    processing_status TEXT NOT NULL DEFAULT 'RECEIVED'
        CHECK (processing_status IN ('RECEIVED', 'VALID', 'INVALID_SCHEMA', 'INVALID_VALUES', 'PREDICTION_ERROR')),
    error_message TEXT,
    processed_at TEXT,
    UNIQUE(topic, partition_id, offset_id)
);

CREATE TRIGGER IF NOT EXISTS preserve_raw_message
BEFORE UPDATE OF topic, partition_id, offset_id, raw_key, raw_value, headers_json,
                 kafka_timestamp_type, kafka_timestamp_ms, received_at
ON raw_happiness_events
BEGIN
    SELECT RAISE(ABORT, 'El mensaje crudo es inmutable');
END;

CREATE TABLE IF NOT EXISTS event_claims (
    event_id TEXT PRIMARY KEY,
    raw_event_id INTEGER NOT NULL UNIQUE REFERENCES raw_happiness_events(id)
);

CREATE TABLE IF NOT EXISTS happiness_predictions (
    id INTEGER PRIMARY KEY,
    raw_event_id INTEGER NOT NULL UNIQUE REFERENCES raw_happiness_events(id),
    event_id TEXT NOT NULL UNIQUE,
    country TEXT NOT NULL,
    year INTEGER NOT NULL,
    event_time TEXT NOT NULL,
    input_features_json TEXT NOT NULL,
    actual_happiness_score REAL NOT NULL,
    predicted_happiness_score REAL NOT NULL,
    residual REAL NOT NULL,
    absolute_error REAL NOT NULL,
    model_name TEXT NOT NULL,
    model_sha256 TEXT NOT NULL,
    predicted_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS raw_status_idx ON raw_happiness_events(processing_status);
CREATE INDEX IF NOT EXISTS predictions_country_year_idx ON happiness_predictions(country, year);
