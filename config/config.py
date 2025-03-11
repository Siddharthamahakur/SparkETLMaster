import os

import yaml


def load_config(config_filename="config.yaml"):
    """Load YAML configuration file from the correct directory."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # Go one level up
    config_path = os.path.join(project_root, config_filename)  # Full path

    print(f"🔍 Checking for config file at: {config_path}")  # Debugging statement

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"❌ Config file not found: {config_path}")

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    return config
