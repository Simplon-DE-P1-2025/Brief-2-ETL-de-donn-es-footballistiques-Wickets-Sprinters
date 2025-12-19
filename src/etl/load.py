from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Optional, List, Dict, Any
import pandas as pd
import os
import yaml
from pathlib import Path

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
        print(f"Erreur de création du moteur SQLAlchemy : {e}")
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
        print(f"Erreur SQL : {e}")


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

