"""
helpers.py
----------
Shared utility functions used across the project.
"""

import logging
import json
import joblib
import numpy as np
from pathlib import Path
from datetime import datetime


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Returns a consistently formatted logger."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        handler.setFormatter(logging.Formatter(fmt, datefmt="%H:%M:%S"))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


# ---------------------------------------------------------------------------
# Model persistence
# ---------------------------------------------------------------------------

MODELS_DIR = Path(__file__).resolve().parents[2] / "models"


def save_model(model, name: str):
    """Saves a scikit-learn / XGBoost model with joblib."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    path = MODELS_DIR / f"{name}.pkl"
    joblib.dump(model, path)
    logging.getLogger(__name__).info(f"Model saved: {path}")
    return path


def load_model(name: str):
    """Loads a previously saved model."""
    path = MODELS_DIR / f"{name}.pkl"
    return joblib.load(path)


def save_keras_model(model, name: str):
    """Saves a Keras model to the models/ directory."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    path = MODELS_DIR / f"{name}.keras"
    model.save(path)
    logging.getLogger(__name__).info(f"Keras model saved: {path}")
    return path


# ---------------------------------------------------------------------------
# Results persistence
# ---------------------------------------------------------------------------

RESULTS_DIR = Path(__file__).resolve().parents[2] / "results"


def save_results(results: dict, filename: str = None):
    """Serialises a results dict to JSON in results/."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    filename = filename or f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path = RESULTS_DIR / filename

    def _convert(obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        raise TypeError(f"Type {type(obj)} not serialisable")

    with open(path, "w") as f:
        json.dump(results, f, indent=2, default=_convert)
    logging.getLogger(__name__).info(f"Results saved: {path}")
    return path


# ---------------------------------------------------------------------------
# Seed fixing
# ---------------------------------------------------------------------------

def set_seeds(seed: int = 42):
    """Fixes random seeds for reproducibility across numpy, random, and TF."""
    import random
    random.seed(seed)
    np.random.seed(seed)
    try:
        import tensorflow as tf
        tf.random.set_seed(seed)
    except ImportError:
        pass
