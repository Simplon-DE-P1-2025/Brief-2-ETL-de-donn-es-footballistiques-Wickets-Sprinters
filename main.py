# -*- coding: utf-8 -*-
"""
Goal: 
    This module serves as the main entry point for the ETL process.
    It reads configuration parameters, extracts data from multiple sources,
    and consolidates them into a single DataFrame.
    It utilizes functions from the eda.config module to load configurations.
    It utilizes functions from the eda.extract module to perform these tasks.
"""
import pandas as pd  # pour la manipulation de DataFrames
import os             # pour gérer les chemins et interactions système
from pathlib import Path  # pour manipuler les chemins de fichiers de manière portable
from typing import Dict   # pour typer les dictionnaires dans les fonctions
import numpy as np    # pour les opérations numériques avancées

from sqlalchemy import (
    MetaData, Table, Column,
    Integer, String, Date
    )
from etl.extract import fct_read_csv, fct_read_json_nested
from etl.transform import (
    fct_transform_2010,
    trf_file_wcup_2014,
    fct_transform_data_2018,
    transform_2022_data
    )
from etl.load import create_postgres_engine
from etl.utils import fct_load_config


# chargement des paraètres de configuration à partir de ./config.yaml
config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = fct_load_config(config_path)

# Récupération des chemins vers les fichiers sources
root_csv_2010 = config['root_csv_2010']
root_csv_2014 = config['root_csv_2014']
root_csv_2022 = config['root_csv_2022']
root_json_2018 = config['root_json_2018']

# Charger les variables d'environnement à partir du fichier .env
host=os.getenv("HOST")
database=os.getenv("DATABASE")
user=os.getenv("DB_USER")
password=os.getenv("PASSWORD")

def main() -> None:
    """
    Run the complete ETL pipeline.

    This function performs the following steps:
    1. Extract data from CSV and JSON source files.
    2. Transform and normalize datasets for each World Cup edition.
    3. Merge all datasets into a single consolidated DataFrame.
    4. Generate a unique incremental match identifier.
    5. Load the final dataset into a PostgreSQL database.

    Environment variables required:
    - HOST: database host
    - DATABASE: database name
    - USER: database user
    - PASSWORD: database password

    Returns
    -------
    None
        This function does not return any value.
        Data is loaded into the database.
    """
    # --------------------
    # Extraction
    # --------------------
    df_2010 = fct_read_csv(root_csv_2010)
    df_2014 = fct_read_csv(root_csv_2014)
    dfs_2018 = fct_read_json_nested(root_json_2018)
    df_2022 = fct_read_csv (root_csv_2022)

    # --------------------
    # Transformation
    # --------------------
    df_2010_clean = fct_transform_2010(df_2010, config)
    df_2014_clean = trf_file_wcup_2014(df_2014, config)
    df_2018_clean = fct_transform_data_2018(dfs_2018, config)
    df_2022_clean = transform_2022_data(df_2022, config)

    # --------------------
    # Merge (concatenation verticale)
    # --------------------
    df_concat = pd.concat(
        [df_2010_clean,df_2014_clean, df_2018_clean, df_2022_clean],
        ignore_index=True)

    # Reset and regenerate match_id
    df_concat["match_id"] = None
    df_final = df_concat.sort_values("date").reset_index(drop=True)
    df_final["match_id"] = range(1, len(df_final) + 1)

    # Load
    engine = create_postgres_engine(
        host=os.getenv("HOST"),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD")
    )

    metadata = MetaData()

    matches = Table(
        "matches",
        metadata,
        Column("match_id", Integer, primary_key=True),
        Column("date", Date, nullable=False),
        Column("home_team", String(100)),
        Column("away_team", String(100)),
        Column("home_result", Integer),
        Column("away_result", Integer),
        Column("stage", String(50)),
        Column("edition", Integer),
        Column("city", String(100)),
        )

    metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    # Chargement des données dans la base
    try:
        df_final.to_sql(
            "matches",
            session.bind,
            if_exists="replace",
            index=False,
            method="multi"
        )
        session.commit()
        print("✅ Données chargées avec succès dans la table 'matches'")
    except Exception as e:
        session.rollback()
        print("❌ Erreur lors du chargement des données dans la base")
        print(f"Détails : {e}")
        raise
    finally:
        session.close()
        print("🔒 Connexion à la base de données fermée")


if __name__ == "__main__":
    main()
