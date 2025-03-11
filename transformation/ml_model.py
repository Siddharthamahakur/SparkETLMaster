import time

import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

from utils.logger import setup_logger

logger = setup_logger("ml_model")


def train_model(X, y):
    """
    Trains a Linear Regression model and logs relevant metrics.

    :param X: Features (list or numpy array)
    :param y: Target variable (list or numpy array)
    :return: Tuple (trained model, MSE, R^2) or None if an error occurs
    """
    try:
        start_time = time.time()  # Track execution time

        # Convert inputs to NumPy arrays
        X = np.array(X, dtype=np.float64)
        y = np.array(y, dtype=np.float64)

        # Reshape X if it's 1D (single feature)
        if X.ndim == 1:
            X = X.reshape(-1, 1)

        # Validate shapes
        if X.shape[0] != y.shape[0]:
            logger.error(f"Shape mismatch: X has {X.shape[0]} samples, y has {y.shape[0]} samples")
            return None

        if X.size == 0 or y.size == 0:
            logger.error("Empty dataset provided for training")
            return None

        logger.info(f"Starting ML model training with {X.shape[0]} samples and {X.shape[1]} features...")

        # Train the model
        model = LinearRegression()
        model.fit(X, y)

        # Predict on training data
        y_pred = model.predict(X)

        # Compute performance metrics
        mse = mean_squared_error(y, y_pred)
        r2 = r2_score(y, y_pred)

        training_time = time.time() - start_time  # Compute elapsed time

        # Log performance
        logger.info(f"Model training completed in {training_time:.4f} seconds")
        logger.info(f"Performance: MSE = {mse:.4f}, R^2 = {r2:.4f}")

        return model, mse, r2  # Return model and metrics

    except ValueError as ve:
        logger.error(f"ValueError: {ve}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error during ML model training: {e}", exc_info=True)

    return None  # Return None on failure