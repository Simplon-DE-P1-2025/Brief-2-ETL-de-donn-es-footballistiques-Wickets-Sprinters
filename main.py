"""
Goal: 
    This module serves as the main entry point for the ETL process.
    It reads configuration parameters, extracts data from multiple sources,
    and consolidates them into a single DataFrame.
    It utilizes functions from the eda.config module to load configurations.
    It utilizes functions from the eda.extract module to perform these tasks.
"""

import yaml
from src.etl.extract import fct_extract_data
from src.etl.transform import normalize_datetime, test_country_column, trf_file_wcup_2014
from src.utils import fct_load_config
import pandas as pd
import os

# Load configuration parameters from config.yaml
config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = fct_load_config(config_path)

root_csv_2010 = config['root_csv_2010']
root_csv_2014 = config['root_csv_2014']
root_csv_2022 = config['root_csv_2022']
root_json_2018 = config['root_json_2018']       

# Extract and consolidate data from the specified files
fct_extract_data(root_csv_2010, root_csv_2014, root_csv_2022, root_json_2018)

# Display the first few rows of the consolidated DataFrame

def main() -> None:

    df = extract (chemin)
    df = extract (chemin)
    df = extract (chemin)
    df = extract (chemin)
    
    df_2014_clean = transform(df_2014)


if __name__ == "__main__":
    main()