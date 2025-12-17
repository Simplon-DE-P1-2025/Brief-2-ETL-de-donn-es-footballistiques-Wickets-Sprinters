from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Optional, List, Dict, Any
import pandas as pd
import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(r"C:\Users\DELL\Documents\vscode_simplon\Brief-2-ETL-de-donn-es-footballistiques-Wickets-Sprinters\.env")

def load_env(env_file: str = ".env") -> None:
    """
    Load environment variables from a .env file.
    Works in both script and notebook environments.
    """

    env_path = Path(env_file)

    if not env_path.is_absolute():
        # Script
        if "__file__" in globals():
            project_root = Path(__file__).resolve().parents[1]
        # Notebook
        else:
            project_root = Path.cwd()

        env_path = project_root / env_path

    if not env_path.exists():
        raise FileNotFoundError(f"❌ .env file not found: {env_path}")
    
    # load_dotenv(env_path)

def fct_load_config(config_filename: str = "config.yaml") -> dict:
    """
    Goal
    ----
    Load configuration parameters from a YAML file.

    This function works both in:
    - Python scripts
    - Jupyter notebooks (where __file__ is not defined)

    Parameters
    ----------
    config_filename : str
        Relative or absolute path to the YAML configuration file.

    Returns
    -------
    dict
        Dictionary containing configuration parameters.
    """

    config_path = Path(config_filename)

    # Cas 1 : chemin absolu → on l'utilise directement
    if config_path.is_absolute():
        final_path = config_path

    else:
        # Cas 2 : Script Python (__file__ existe)
        if "__file__" in globals():
            project_root = Path(__file__).resolve().parents[1]

        # Cas 3 : Notebook Jupyter (__file__ n'existe pas)
        else:
            project_root = Path(os.getcwd())

        final_path = project_root / config_path

    if not final_path.exists():
        raise FileNotFoundError(f"❌ Config file not found: {final_path}")

    with open(final_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


def create_postgres_engine(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int = 5432
) -> Optional[Engine]:
    """
    Crée un moteur SQLAlchemy pour PostgreSQL.

    Paramètres
    ----------
    host : str
        Adresse du serveur PostgreSQL
    database : str
        Nom de la base de données
    user : str
        Utilisateur PostgreSQL
    password : str
        Mot de passe
    port : int, optionnel
        Port PostgreSQL (par défaut 5432)

    Retour
    ------
    sqlalchemy.engine.Engine | None
        Moteur SQLAlchemy ou None en cas d'erreur
    """
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
            pool_pre_ping=True
        )
        return engine
    except Exception as e:
        print(f"❌ Erreur de création du moteur SQLAlchemy : {e}")
        return None


def execute_select(
    engine: Engine,
    query: str,
    params: dict | None = None
) -> List[Dict[str, Any]]:
    """
    Exécute une requête SELECT et retourne les résultats sous forme de dictionnaires.
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return [dict(row._mapping) for row in result]
    
def select_to_dataframe(
    engine: Engine,
    query: str,
    params: dict | None = None
) -> pd.DataFrame:
    """
    Exécute une requête SELECT et retourne un DataFrame pandas.
    """
    return pd.read_sql(text(query), engine, params=params)


def execute_query(
    engine: Engine,
    query: str,
    params: dict | None = None
) -> None:
    """
    Exécute une requête INSERT, UPDATE ou DELETE.
    """
    try:
        with engine.begin() as conn:  # commit automatique
            conn.execute(text(query), params or {})
    except Exception as e:
        print(f"❌ Erreur SQL : {e}")


def dataframe_to_table(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    schema: str = "public",
    if_exists: str = "append"
) -> None:
    """
    Insère un DataFrame pandas dans une table PostgreSQL.
    """
    df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        method="multi"
    )

