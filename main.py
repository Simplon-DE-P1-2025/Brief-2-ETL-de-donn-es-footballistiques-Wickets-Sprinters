"""
Goal: 
    This module serves as the main entry point for the ETL process.
    It reads configuration parameters, extracts data from multiple sources,
    and consolidates them into a single DataFrame.
    It utilizes functions from the eda.config module to load configurations.
    It utilizes functions from the eda.extract module to perform these tasks.
"""


from src.etl.extract import fct_read_csv, fct_read_json_nested
from src.etl.transform import trf_file_wcup_2014, fct_transform_data_2018, transform_2022_data
from src.utils import fct_load_config
import pandas as pd  # pour la manipulation de DataFrames
import os             # pour gérer les chemins et interactions système
from pathlib import Path  # pour manipuler les chemins de fichiers de manière portable
from typing import Dict   # pour typer les dictionnaires dans les fonctions
import numpy as np    # pour les opérations numériques avancées

# Load configuration parameters from config.yaml
config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = fct_load_config(config_path)

root_csv_2010 = config['root_csv_2010']
root_csv_2014 = config['root_csv_2014']
root_csv_2022 = config['root_csv_2022']
root_json_2018 = config['root_json_2018']

# Display the first few rows of the consolidated DataFrame

def main() -> None:
    # extraction
    #df_2010 = fct_read_csv(root_csv_2010)
    df_2014 = fct_read_csv(root_csv_2014)
    dfs_2018 = fct_read_json_nested(root_json_2018)
    df_2022 = fct_read_csv (root_csv_2022)
    
    
    
    # transform
    df_2014_clean = trf_file_wcup_2014(df_2014, config)
    df_2018_clean = fct_transform_data_2018(dfs_2018, config)
    df_2022_clean = transform_2022_data(df_2022)

    

    # Merge (concaténation verticale)
    df_concat = pd.concat([df_2014_clean, df_2018_clean, df_2022_clean], ignore_index=True)
    # Vider la colonne match_id
    df_concat["match_id"] = None
    # Trier par date (ascendant)
    df_final = df_concat.sort_values("date").reset_index(drop=True)
    # Réincrémenter match_id
    df_final["match_id"] = range(1, len(df_final) + 1)
    print(df_final['match_id'].is_unique)
    
    # Load
    


if __name__ == "__main__":
    main()