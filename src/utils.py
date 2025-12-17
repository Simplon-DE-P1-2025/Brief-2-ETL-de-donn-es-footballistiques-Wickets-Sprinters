"""
Goal: This file serves to define main functions to load configuration parameters.
"""

import yaml
from pathlib import Path

def fct_load_config(config_filename: str ="config.yaml") -> dict:
    """
    Goal:
        Function to load configuration parameters from a YAML file.
    Parameters:
        config_path (str): The name of the YAML file.
    Returns:
        dict: A dictionary containing the configuration parameters.
    """
    config_path = Path(config_filename).resolve()

    project_root = Path(__file__).resolve().parents[1]
    config_path = project_root / config_filename
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


