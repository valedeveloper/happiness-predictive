"""Carga única del pipeline y reconstrucción explícita del esquema de entrenamiento."""

import hashlib
import importlib.metadata
import json
import math
import pickle

import pandas as pd

from .config import EVENT_TO_MODEL, METADATA_PATH, MODEL_PATH


class ModelPredictor:
    def __init__(self, model_path=MODEL_PATH, metadata_path=METADATA_PATH):
        with open(metadata_path, encoding="utf-8") as file:
            self.metadata = json.load(file)
        self.features = self.metadata["features"]
        if self.features != list(EVENT_TO_MODEL.values()):
            raise ValueError("El contrato del evento no coincide con el orden de variables de los metadatos.")
        # Pickle requiere un entorno compatible; se verifica antes de consumir mensajes.
        for package in ["scikit-learn", "numpy", "pandas", "scipy"]:
            expected = self.metadata["package_versions"][package]
            observed = importlib.metadata.version(package)
            if observed != expected:
                raise RuntimeError(f"{package}: se requiere {expected}, instalado {observed}. Instale kafka/requirements.txt.")
        with open(model_path, "rb") as file:
            model_bytes = file.read()
        self.sha256 = hashlib.sha256(model_bytes).hexdigest()
        # Este artefacto debe proceder del entrenamiento local y ser de confianza.
        self.model = pickle.loads(model_bytes)
        if self.model.feature_names_in_.tolist() != self.features:
            raise ValueError("Las columnas del pickle no coinciden con sus metadatos.")
        self.name = self.metadata["selected_model"]

    def feature_values(self, event):
        by_model_name = {name: float(event[source]) for source, name in EVENT_TO_MODEL.items()}
        return {name: by_model_name[name] for name in self.features}

    def predict(self, event):
        features = self.feature_values(event)
        frame = pd.DataFrame([features], columns=self.features)
        # El pipeline ya contiene su preprocesamiento; no se vuelve a ajustar.
        prediction = float(self.model.predict(frame)[0])
        if not math.isfinite(prediction):
            raise ValueError("El modelo devolvió una predicción no finita.")
        return prediction, features
