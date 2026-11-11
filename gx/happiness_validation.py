"""Tres validaciones: ejecutar después de transformar y cargar el histórico."""

import os
from pathlib import Path

import great_expectations as gx
import pandas as pd
import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]


def main():
    load_dotenv(ROOT / ".env")
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas(name="happiness")

    def validate(name, frame, expectation):
        batch = source.add_dataframe_asset(name=name).add_batch_definition_whole_dataframe(
            name="datos"
        ).get_batch(batch_parameters={"dataframe": frame})
        success = batch.validate(expectation).success
        print(f"{'OK' if success else 'FALLO'}: {name}")
        return success

    data = pd.read_csv(ROOT / "data/processed/happiness_analysis.csv")
    # 1. La normalización de países debe conservar la granularidad país–año.
    results = [validate("Pais y año únicos", data,
        gx.expectations.ExpectCompoundColumnsToBeUnique(
            column_list=["country", "year"], ignore_row_if="never"))]

    # 2. Tras recuperar las regiones, el histórico actual no tiene desconocidos.
    regions = data["region"].astype("string").str.strip()
    resolved = (regions.notna() & ~regions.str.casefold().isin(["", "desconocido"])).fillna(False)
    results.append(validate("Regiones completas", pd.DataFrame({"resuelta": resolved}),
        gx.expectations.ExpectColumnValuesToBeInSet(column="resuelta", value_set=[True])))

    # 3. Consulta de solo lectura: cada VALID tiene un hecho; los demás, ninguno.
    # Una base sin predicciones no se considera una carga validada.
    with psycopg.connect(os.getenv("DATABASE_URL") or "") as db:
        consistent = db.execute("""
            SELECT EXISTS (SELECT 1 FROM fact_predictions) AND NOT EXISTS (
                SELECT r.raw_event_id
                FROM raw_happiness_events r FULL JOIN fact_predictions f USING (raw_event_id)
                GROUP BY r.raw_event_id, r.processing_status
                HAVING r.raw_event_id IS NULL OR COUNT(f.prediction_id) <>
                    CASE WHEN r.processing_status='VALID' THEN 1 ELSE 0 END
            )
        """).fetchone()[0]
    results.append(validate("Trazabilidad tras la carga", pd.DataFrame({"consistente": [consistent]}),
        gx.expectations.ExpectColumnValuesToBeInSet(column="consistente", value_set=[True])))
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
