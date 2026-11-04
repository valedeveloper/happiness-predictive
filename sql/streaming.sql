-- PostgreSQL: staging y modelo estrella para predicciones.
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    raw_key BYTEA,
    raw_value BYTEA,
    headers_json TEXT NOT NULL DEFAULT '[]',
    kafka_timestamp_type SMALLINT NOT NULL,
    kafka_timestamp_ms BIGINT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    event_id TEXT,
    processing_status TEXT NOT NULL DEFAULT 'RECEIVED'
        CHECK (processing_status IN ('RECEIVED', 'VALID', 'INVALID_SCHEMA', 'INVALID_VALUES', 'PREDICTION_ERROR')),
    error_message TEXT,
    processed_at TIMESTAMPTZ,
    UNIQUE (topic, partition_id, offset_id)
);

-- Los rechazos pueden repetir event_id; un evento aceptado solo se reserva una vez.
CREATE UNIQUE INDEX IF NOT EXISTS accepted_event_id_unique
    ON raw_happiness_events(event_id)
    WHERE processing_status IN ('VALID', 'PREDICTION_ERROR');

CREATE OR REPLACE FUNCTION preserve_raw_message() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'El mensaje original es inmutable';
END;
$$;
DROP TRIGGER IF EXISTS preserve_raw_message ON raw_happiness_events;
CREATE TRIGGER preserve_raw_message
BEFORE UPDATE OF topic, partition_id, offset_id, raw_key, raw_value, headers_json,
                 kafka_timestamp_type, kafka_timestamp_ms, received_at
ON raw_happiness_events FOR EACH ROW EXECUTE FUNCTION preserve_raw_message();

CREATE TABLE IF NOT EXISTS dim_country (
    country_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE,
    region TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    day SMALLINT GENERATED ALWAYS AS (EXTRACT(DAY FROM calendar_date)::SMALLINT) STORED,
    month SMALLINT GENERATED ALWAYS AS (EXTRACT(MONTH FROM calendar_date)::SMALLINT) STORED,
    quarter SMALLINT GENERATED ALWAYS AS (EXTRACT(QUARTER FROM calendar_date)::SMALLINT) STORED,
    year SMALLINT GENERATED ALWAYS AS (EXTRACT(YEAR FROM calendar_date)::SMALLINT) STORED,
    CHECK (date_id = EXTRACT(YEAR FROM calendar_date)::INTEGER * 10000
                  + EXTRACT(MONTH FROM calendar_date)::INTEGER * 100
                  + EXTRACT(DAY FROM calendar_date)::INTEGER)
);

-- Cada fila en fact_predictions corresponde a una predicción generada para un
-- evento válido de un país y año histórico, con la versión del modelo utilizada.
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_event_id BIGINT NOT NULL UNIQUE REFERENCES raw_happiness_events(raw_event_id),
    country_id INTEGER NOT NULL REFERENCES dim_country(country_id),
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    observation_year SMALLINT NOT NULL CHECK (observation_year BETWEEN 2015 AND 2019),
    actual_score DOUBLE PRECISION NOT NULL CHECK (actual_score BETWEEN 0 AND 10),
    predicted_score DOUBLE PRECISION NOT NULL
        CHECK (predicted_score > '-Infinity'::DOUBLE PRECISION AND predicted_score < 'Infinity'::DOUBLE PRECISION),
    prediction_error DOUBLE PRECISION GENERATED ALWAYS AS (actual_score - predicted_score) STORED,
    event_time TIMESTAMPTZ NOT NULL,
    prediction_timestamp TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    model_name TEXT NOT NULL,
    model_sha256 TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS fact_country_year_idx ON fact_predictions(country_id, observation_year);
CREATE INDEX IF NOT EXISTS fact_date_idx ON fact_predictions(date_id);
CREATE INDEX IF NOT EXISTS raw_status_idx ON raw_happiness_events(processing_status);

-- Vista base: trazabilidad de los hechos hacia el original y sus dimensiones.
CREATE OR REPLACE VIEW v_prediction_detail AS
SELECT f.*, c.country_name, c.region, d.calendar_date, r.event_id
FROM fact_predictions f
JOIN dim_country c USING (country_id)
JOIN dim_date d USING (date_id)
JOIN raw_happiness_events r USING (raw_event_id);

-- R1: calidad del flujo, agrupada por día UTC de recepción.
CREATE OR REPLACE VIEW v_event_quality_daily AS
SELECT (received_at AT TIME ZONE 'UTC')::DATE AS received_date,
       COUNT(*) AS received_events,
       COUNT(*) FILTER (WHERE processing_status='RECEIVED') AS pending_events,
       COUNT(*) FILTER (WHERE processing_status='VALID') AS predicted_events,
       COUNT(*) FILTER (WHERE processing_status='INVALID_SCHEMA') AS invalid_schema_events,
       COUNT(*) FILTER (WHERE processing_status='INVALID_VALUES') AS invalid_value_events,
       COUNT(*) FILTER (WHERE processing_status='PREDICTION_ERROR') AS prediction_errors,
       100.0 * COUNT(*) FILTER (WHERE processing_status='VALID') / NULLIF(COUNT(*), 0) AS prediction_rate_pct,
       100.0 * COUNT(*) FILTER (WHERE processing_status IN ('INVALID_SCHEMA','INVALID_VALUES'))
         / NULLIF(COUNT(*) FILTER (WHERE processing_status<>'RECEIVED'), 0) AS rejection_rate_pct
FROM raw_happiness_events GROUP BY 1;

-- R2 y R3: niveles observados/predichos y precisión por país, región y año histórico.
-- No se promedian RMSE ni R² ya agregados para producir métricas de otro nivel.
CREATE OR REPLACE VIEW v_prediction_metrics AS
SELECT model_name, model_sha256, country_name, region, observation_year,
       COUNT(*) AS predictions,
       AVG(actual_score) AS mean_actual_score,
       AVG(predicted_score) AS mean_predicted_score,
       AVG(prediction_error) AS mean_error,
       AVG(ABS(prediction_error)) AS mae,
       SQRT(AVG(POWER(prediction_error, 2))) AS rmse,
       CASE WHEN COUNT(*) > 1 AND VAR_POP(actual_score) > 0
            THEN 1 - AVG(POWER(prediction_error, 2)) / VAR_POP(actual_score)
            ELSE NULL END AS r2,
       MAX(ABS(prediction_error)) AS max_absolute_error,
       100.0 * COUNT(*) FILTER (WHERE ABS(prediction_error)>0.5) / COUNT(*) AS error_over_05_pct
FROM v_prediction_detail
GROUP BY model_name, model_sha256, GROUPING SETS (
    (country_name, region, observation_year),
    (region, observation_year),
    (observation_year)
);

-- R4: actividad y demora emisión-predicción por día UTC de emisión.
CREATE OR REPLACE VIEW v_prediction_activity_daily AS
SELECT calendar_date, model_name, model_sha256, COUNT(*) AS predictions,
       AVG(EXTRACT(EPOCH FROM (prediction_timestamp-event_time))*1000) AS mean_latency_ms
FROM v_prediction_detail GROUP BY calendar_date, model_name, model_sha256;

-- R5: clasificación de países por MAE en cada año y versión del modelo.
CREATE OR REPLACE VIEW v_country_error_ranking AS
SELECT *, DENSE_RANK() OVER (
    PARTITION BY model_sha256, observation_year ORDER BY mae DESC
) AS error_rank
FROM v_prediction_metrics WHERE country_name IS NOT NULL;
