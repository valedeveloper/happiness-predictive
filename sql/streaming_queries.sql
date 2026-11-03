-- Ejecutar contra data/streaming/happiness.db con un cliente SQLite.
SELECT processing_status, COUNT(*) AS records
FROM raw_happiness_events GROUP BY processing_status;

-- La conversión a texto es solo una vista de consulta; el original sigue como BLOB.
SELECT topic, partition_id, offset_id, event_id, processing_status, error_message,
       CAST(raw_value AS TEXT) AS original_json
FROM raw_happiness_events ORDER BY id DESC LIMIT 10;

SELECT event_id, country, year, actual_happiness_score, predicted_happiness_score,
       residual, absolute_error, model_name
FROM happiness_predictions ORDER BY id DESC LIMIT 10;

SELECT COUNT(*) AS predictions, AVG(absolute_error) AS mae,
       AVG(residual * residual) AS mse
FROM happiness_predictions;

-- La repetición de un país en diferentes ediciones no es un evento duplicado.
SELECT country, COUNT(*) AS events FROM happiness_predictions GROUP BY country;
