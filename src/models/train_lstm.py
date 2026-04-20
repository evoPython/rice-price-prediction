"""
train_lstm.py
-------------
Stacked LSTM network for rice price forecasting.

Architecture (per methodology):
  • Multiple stacked LSTM layers for enhanced feature extraction
  • Dropout regularisation to prevent overfitting
  • Adaptive learning rate via ReduceLROnPlateau callback
  • Adam optimiser, MSE loss

Input shape: (samples, timesteps, features)
"""

import numpy as np
from pathlib import Path

from src.utils.helpers import get_logger, save_keras_model

logger = get_logger(__name__)

MODELS_DIR = Path(__file__).resolve().parents[2] / "models"
RANDOM_STATE = 42

# Default architecture hyperparameters
DEFAULT_UNITS_1   = 64
DEFAULT_UNITS_2   = 32
DEFAULT_DROPOUT   = 0.2
DEFAULT_LR        = 0.001
DEFAULT_EPOCHS    = 100
DEFAULT_BATCH     = 16
DEFAULT_TIMESTEPS = 1


def build_lstm(
    n_features: int,
    timesteps: int = DEFAULT_TIMESTEPS,
    units_1: int = DEFAULT_UNITS_1,
    units_2: int = DEFAULT_UNITS_2,
    dropout: float = DEFAULT_DROPOUT,
    learning_rate: float = DEFAULT_LR,
) -> "tf.keras.Model":
    """
    Builds a stacked LSTM model.

    Parameters
    ----------
    n_features    : number of input features
    timesteps     : sequence length (1 = single-step)
    units_1       : hidden units in first LSTM layer
    units_2       : hidden units in second LSTM layer (set to 0 to skip)
    dropout       : dropout rate after each LSTM layer
    learning_rate : initial Adam learning rate

    Returns
    -------
    Compiled Keras model
    """
    try:
        import tensorflow as tf
        from tensorflow.keras.models import Sequential
        from tensorflow.keras.layers import LSTM, Dense, Dropout
        from tensorflow.keras.optimizers import Adam
    except ImportError as e:
        raise ImportError(
            "TensorFlow is required for LSTM training. "
            "Install it with: pip install tensorflow"
        ) from e

    tf.random.set_seed(RANDOM_STATE)

    model = Sequential(name="LSTM_Rice_Price")

    # First LSTM layer (return_sequences=True because a second layer follows)
    model.add(LSTM(
        units=units_1,
        return_sequences=(units_2 > 0),
        input_shape=(timesteps, n_features),
        name="lstm_1",
    ))
    model.add(Dropout(dropout, name="dropout_1"))

    # Second LSTM layer (optional)
    if units_2 > 0:
        model.add(LSTM(units=units_2, name="lstm_2"))
        model.add(Dropout(dropout, name="dropout_2"))

    # Output layer
    model.add(Dense(1, name="output"))

    model.compile(
        optimizer=Adam(learning_rate=learning_rate),
        loss="mse",
        metrics=["mae"],
    )

    return model


def train_lstm(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray = None,
    y_val: np.ndarray = None,
    timesteps: int = DEFAULT_TIMESTEPS,
    units_1: int = DEFAULT_UNITS_1,
    units_2: int = DEFAULT_UNITS_2,
    dropout: float = DEFAULT_DROPOUT,
    learning_rate: float = DEFAULT_LR,
    epochs: int = DEFAULT_EPOCHS,
    batch_size: int = DEFAULT_BATCH,
    patience: int = 15,
    save: bool = True,
) -> tuple:
    """
    Trains the stacked LSTM model.

    Parameters
    ----------
    X_train / y_train : training data (scaled, already outlier-cleaned)
    X_val   / y_val   : optional validation data; if None, 10% of train used
    timesteps         : LSTM sequence length
    units_1 / units_2 : LSTM layer sizes
    dropout           : dropout rate
    learning_rate     : initial Adam LR
    epochs            : max training epochs
    batch_size        : mini-batch size
    patience          : EarlyStopping patience
    save              : save model to models/lstm.keras

    Returns
    -------
    (model, history)   – fitted Keras model + training history object
    """
    try:
        from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau, ModelCheckpoint
    except ImportError as e:
        raise ImportError("TensorFlow is required for LSTM training.") from e

    logger.info("Training LSTM …")

    # Reshape to (samples, timesteps, features)
    n_features = X_train.shape[1]
    X_train_3d = X_train.reshape(-1, timesteps, n_features)

    if X_val is not None:
        X_val_3d = X_val.reshape(-1, timesteps, n_features)
        validation_data = (X_val_3d, y_val)
    else:
        validation_data = None  # Keras will use validation_split

    model = build_lstm(
        n_features=n_features,
        timesteps=timesteps,
        units_1=units_1,
        units_2=units_2,
        dropout=dropout,
        learning_rate=learning_rate,
    )

    model.summary(print_fn=logger.info)

    # Callbacks
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_path = str(MODELS_DIR / "lstm_best.keras")

    callbacks = [
        EarlyStopping(
            monitor="val_loss",
            patience=patience,
            restore_best_weights=True,
            verbose=1,
        ),
        ReduceLROnPlateau(
            monitor="val_loss",
            factor=0.5,
            patience=patience // 2,
            min_lr=1e-6,
            verbose=1,
        ),
        ModelCheckpoint(
            filepath=checkpoint_path,
            monitor="val_loss",
            save_best_only=True,
            verbose=0,
        ),
    ]

    fit_kwargs = dict(
        x=X_train_3d,
        y=y_train,
        epochs=epochs,
        batch_size=batch_size,
        callbacks=callbacks,
        verbose=1,
        shuffle=False,
    )

    if validation_data is not None:
        # Kept for compatibility, but the pipeline now passes None so the
        # validation slice comes from the training data only.
        fit_kwargs["validation_data"] = validation_data
    else:
        fit_kwargs["validation_split"] = 0.10

    history = model.fit(**fit_kwargs)

    logger.info(
        f"LSTM training complete. "
        f"Best val_loss: {min(history.history['val_loss']):.6f}"
    )

    if save:
        save_keras_model(model, "lstm")

    return model, history


def predict_lstm(
    model,
    X: np.ndarray,
    y_scaler=None,
    timesteps: int = DEFAULT_TIMESTEPS,
) -> np.ndarray:
    """
    Generates predictions from the LSTM model.
    If a y_scaler is provided, inverse-transforms the output.

    Parameters
    ----------
    model     : fitted Keras LSTM model
    X         : scaled feature array (2D)
    y_scaler  : fitted StandardScaler for the target (optional)
    timesteps : LSTM timestep parameter

    Returns
    -------
    1D numpy array of predictions in original price units
    """
    n_features = X.shape[1]
    X_3d = X.reshape(-1, timesteps, n_features)
    preds = model.predict(X_3d, verbose=0).ravel()

    if y_scaler is not None:
        preds = y_scaler.inverse_transform(preds.reshape(-1, 1)).ravel()

    return preds
