"""Configuración compartida sin dependencia de un directorio de ejecución."""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOPIC = "happiness-events"
GROUP_ID = "happiness-ml-team"
PARTITIONS = 3
REPLICATION_FACTOR = 1
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
DATA_PATH = PROJECT_ROOT / "data/processed/happiness_modeling.csv"
MODEL_PATH = PROJECT_ROOT / "models/model.pkl"
METADATA_PATH = PROJECT_ROOT / "models/model_metadata.json"
DATABASE_PATH = Path(os.getenv("HAPPINESS_DATABASE", str(PROJECT_ROOT / "data/streaming/happiness.db")))

# Orden canónico de entrenamiento: el objetivo nunca aparece aquí.
EVENT_TO_MODEL = {
    "gdp": "gdp_per_capita",
    "family": "social_support",
    "health": "healthy_life_expectancy",
    "freedom": "freedom",
    "generosity": "generosity",
    "corruption": "corruption",
}
