import numpy as np
from sklearn.linear_model import LinearRegression

from utils.logger import setup_logger

logger = setup_logger("ml_model")


def train_model(X, y):
    """Trains a simple ML model"""
    try:
        logger.info("Training ML model...")
        model = LinearRegression()
        model.fit(np.array(X).reshape(-1, 1), y)
        logger.info("Model training completed!")
        return model
    except Exception as e:
        logger.error(f"Error in ML model training: {e}", exc_info=True)
        return None
