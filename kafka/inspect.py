"""Consulta de resultados locales sin modificar la base SQLite."""

import argparse
import json
import sqlite3
from contextlib import closing
from pathlib import Path

from .config import DATABASE_PATH


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE_PATH)
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("limit debe ser positivo.")
    if not args.database.is_file():
        parser.error(f"No existe la base: {args.database}")
    uri = args.database.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        statuses = dict(connection.execute(
            "SELECT processing_status, COUNT(*) FROM raw_happiness_events GROUP BY processing_status"
        ).fetchall())
        count = connection.execute("SELECT COUNT(*) FROM happiness_predictions").fetchone()[0]
        last_predictions = [dict(row) for row in connection.execute(
            """SELECT event_id, country, year, actual_happiness_score, predicted_happiness_score,
               residual, model_name FROM happiness_predictions ORDER BY id DESC LIMIT ?""", (args.limit,)
        )]
    print(json.dumps({"database": str(args.database), "raw_statuses": statuses,
                      "predictions": count, "last_predictions": last_predictions}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
