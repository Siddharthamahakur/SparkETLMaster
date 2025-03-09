import logging
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)  # Ensure log directory exists


def setup_logger(name):
    """Function to set up a logger for each module"""
    logger = logging.getLogger(name)
    if not logger.handlers:  # Prevent duplicate handlers
        logger.setLevel(logging.INFO)

        # File Handler
        file_handler = logging.FileHandler(f"{LOG_DIR}/{name}.log")
        file_handler.setLevel(logging.INFO)

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Log Format
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add Handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
