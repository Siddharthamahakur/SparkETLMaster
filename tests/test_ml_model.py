import logging

import numpy as np
import pytest
from sklearn.metrics import mean_squared_error

from transformation.ml_model import train_model

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def sample_data():
    """
    Fixture to generate sample training data.
    """
    X = np.array([1, 2, 3, 4, 5]).reshape(-1, 1)  # Reshape for compatibility with ML models
    y = np.array([2, 4, 6, 8, 10])  # Corresponding target values
    return X, y


def test_train_model(sample_data):
    """
    Test if the train_model function successfully trains an ML model.
    Includes data validation, model verification, and performance checks.
    """
    X, y = sample_data
    logger.info("Starting ML model training test.")

    try:
        # Train the model
        model = train_model(X, y)

        # Ensure model is trained successfully
        assert model is not None, "Model training failed: No model returned."

        # Check if model has a prediction method
        assert hasattr(model, "predict"), "Trained model lacks a predict method."

        # Validate model predictions
        y_pred = model.predict(X)
        mse = mean_squared_error(y, y_pred)
        logger.info(f"Model training completed. MSE: {mse:.4f}")

        # Ensure model predictions are reasonably accurate
        assert mse < 1e-3, f"Model performance issue: MSE is too high ({mse:.4f})"

    except Exception as e:
        logger.error(f"Model training test failed: {e}")
        pytest.fail(f"train_model() raised an exception: {e}")
