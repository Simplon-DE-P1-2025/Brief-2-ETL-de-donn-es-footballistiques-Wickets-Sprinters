# -*- coding: utf-8 -*-
"""
Goal: This file serves to define main functions to load configuration parameters.
"""
import os
import re
import yaml
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Optional, Union, Dict, List


def fct_load_config(config_filename: str = "config.yaml") -> dict:
    """
    Charge le fichier YAML de config.

    Fonction compatible script et notebook, chemins relatifs ou absolus.
    """
    from pathlib import Path
    import os, yaml

    config_path = Path(config_filename)

    # Si chemin absolu, on l'utilise directement
    if config_path.is_absolute():
        final_path = config_path
    else:
        # Utiliser le répertoire courant pour les chemins relatifs
        if "__file__" in globals():
            base_dir = Path(__file__).parent.parent  # src/
        else:
            base_dir = Path(os.getcwd())
        final_path = base_dir / config_path

    if not final_path.exists():
        raise FileNotFoundError(f"❌ Config file not found: {final_path}")

    with open(final_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def normalize_datetime(x: Union[str, pd.Timestamp]) -> Optional[str]:
    """
    Cette fonction est relative au traitement du fichier WorldCupMatches2014.csv
    Convertit une date/heure en chaîne de caractères au format YYYYMMDDhhmmss.

    Paramètres
    ----------
    x : str | pandas.Timestamp
        Date/heure à convertir (ex: "12 Jun 2014 - 17:00").

    Retour
    ------
    str | None
        Date/heure normalisée au format YYYYMMDDhhmmss,
        ou None si la conversion échoue ou si la valeur est manquante.
    """
    try:
        # Conversion en datetime pandas (gestion automatique de plusieurs formats)
        dt = pd.to_datetime(x, dayfirst=False, errors="coerce")
        if pd.isna(dt):
            dt = pd.to_datetime(x, dayfirst=True, errors="coerce")

        # Formatage final en YYYYMMDDhhmmss
        return dt.strftime("%Y%m%d%H%M%S")

    except Exception:
        # Retourne None si la conversion échoue
        return None

def test_country_column(df: pd.DataFrame, column: str) -> Dict[str, List[str]]:
    """
    Cette fonction est relative au traitement du fichier WorldCupMatches2014.csv
    Teste la colonne pour :
    - majuscule initiale
    - espaces superflus (début, fin, multiples)
    - caractères spéciaux ou accentués
    - guillemets indésirables dans la chaîne
    """
    issues = {
        "not_capitalized": [],
        "special_chars": [],
        "extra_spaces": []
    }

    for val in df[column].dropna().unique():
        val_str = str(val)
        val_strip = val_str.strip()

        # Majuscule initiale
        if not val_strip[0].isupper():
            issues["not_capitalized"].append(val_str)

        # Espaces en début ou fin
        if val_str != val_strip:
            issues["extra_spaces"].append(val_str)

        # Espaces multiples à l'intérieur
        if "  " in val_str:
            if val_str not in issues["extra_spaces"]:
                issues["extra_spaces"].append(val_str)

        # Caractères spéciaux ou accents incorrects (garde lettres accentuées)
        if re.search(r'[^A-Za-zÀ-ÖØ-öø-ÿ\s\-]', val_strip):
            if val_str not in issues["special_chars"]:
                issues["special_chars"].append(val_str)

        # Détection spécifique des guillemets " ou '
        if '"' in val_str or "'" in val_str:
            if val_str not in issues["special_chars"]:
                issues["special_chars"].append(val_str)

    return issues

def fct_capitalize_string_columns(df: pd.DataFrame, cols: list = None) -> pd.DataFrame:
    """
    Objectif :
        Supprimer les espaces superflus et mettre la première lettre en majuscule,
        le reste en minuscules, uniquement pour les valeurs de type str.
    
    Paramètres :
        df (pd.DataFrame) : DataFrame d'entrée.
        cols (list, optionnel) : Liste des colonnes à nettoyer.
    
    Retour :
        pd.DataFrame : DataFrame avec les colonnes de chaînes nettoyées.
    """
    # Vérifier que des colonnes ont été fournies
    if not cols:
        print(
            "[INFO] Aucune colonne spécifiée pour capitalize_string_columns. "
            "Le DataFrame reste inchangé.")
        return df

    # Traiter chaque colonne spécifiée
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype("string")
                .str.strip()
                .str.capitalize()
            )
    return df



def fct_upper_string_columns(df: pd.DataFrame, cols: list = None) -> pd.DataFrame:
    """
    Objectif :
        Supprimer les espaces superflus et mettre toutes les lettres en majuscules,
        uniquement pour les valeurs de type str.
    
    Paramètres :
        df (pd.DataFrame) : DataFrame d'entrée.
        cols (list, optionnel) : Liste des colonnes à nettoyer. 
    Retour :
        pd.DataFrame : DataFrame avec les colonnes de chaînes nettoyées.
    """
    # Vérifier que des colonnes ont été fournies
    if not cols:
        print(
            "[INFO] Aucune colonne spécifiée pour clean_string_columns_upper. "
            "Le DataFrame reste inchangé.")
        return df

    # Traiter chaque colonne spécifiée
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype("string")
                .str.strip()
                .str.upper()
            )
    return df


def fct_lower_string_columns(df: pd.DataFrame, cols: list = None) -> pd.DataFrame:
    """
    Objectif :
        Supprimer les espaces superflus et mettre toutes les lettres en minuscules,
        uniquement pour les valeurs de type str.
    
    Paramètres :
        df (pd.DataFrame) : DataFrame d'entrée.
        cols (list, optionnel) : Liste des colonnes à nettoyer. 
    Retour :
        pd.DataFrame : DataFrame avec les colonnes de chaînes nettoyées.
    """
    # Vérifier que des colonnes ont été fournies
    if not cols:
        print(
            "[INFO] Aucune colonne spécifiée pour clean_string_columns_lower. "
            "Le DataFrame reste inchangé."
            )
        return df

    # Traiter chaque colonne spécifiée
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype("string")
                .str.strip()
                .str.lower()
            )
    return df

def fct_iso_to_yyyymmddhhmmss(df: pd.DataFrame, col: str, new_col: str = None) -> pd.DataFrame:
    """
    Convertir une colonne ISO 8601 en une seule colonne YYYYMMDDhhmmss.
    Les valeurs manquantes ou invalides sont remplacées par :
        année=9999, mois=99, jour=99, heure=99, minute=99, seconde=99
    """
    if new_col is None:
        new_col = col

    def safe_convert(x):
        try:
            dt = pd.to_datetime(x, utc=True)
            year = f"{dt.year:04d}"
            month = f"{dt.month:02d}"
            day = f"{dt.day:02d}"
            hour = f"{dt.hour:02d}"
            minute = f"{dt.minute:02d}"
            second = f"{dt.second:02d}"
        except Exception:
            year = "9999"
            month = day = hour = minute = second = "99"

        return f"{year}{month}{day}{hour}{minute}{second}"

    df[new_col] = df[col].apply(safe_convert)
    return df

def fct_extract_edition(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Extraire l'année d'une colonne de date ISO 8601.
    
    Paramètres :
        df (pd.DataFrame) : DataFrame d'entrée
        col (str) : colonne ISO à extraire l'année
    
    Retour :
        pd.DataFrame : DataFrame avec nouvelle colonne année
    """
    df['edition'] = df[col].str[:4].astype(int)
    return df

def fct_generate_unique_stage(
    df: pd.DataFrame,
    col_stage:str ,
    col_round:str,
    col_group : str
) -> pd.DataFrame:
    """
    Générer une colonne unique 'stage' en combinant les colonnes 'round_id' et 'group_id'.
    
    Paramètres :
        df (pd.DataFrame) : DataFrame d'entrée
        col_stage (str) : nom de la colonne stage à créer
        col_round (str) : nom de la colonne round_id
        col_group (str) : nom de la colonne group_id
    
    Retour :
        pd.DataFrame : DataFrame avec nouvelle colonne stage
    """
    # générer une colonne stage_name
    conditions = [
        df['stage'].str.lower().str.strip() == 'group',
        df['stage'].str.lower().str.strip() == 'knockout'
    ]
    choices = [
    df['group_id'],
    df['round_id']
    ]
    df['stage_name'] = np.select(conditions, choices,  default='notdefined')
    return df

def fct_final_columns_to_keep (
    df : pd.DataFrame,
    columns_to_keep_original_list : list,
    columns_to_keep_final_list : list
) -> pd.DataFrame:
    """
    Garder uniquement les colonnes spécifiées dans le DataFrame.
    
    Paramètres :
        df (pd.DataFrame) : DataFrame d'entrée
        columns_to_keep_original_list (list) : liste des noms de colonnes originales à garder
        columns_to_keep_final_list (list) : liste des nouveaux noms de colonnes à garder
    
    Retour :
        pd.DataFrame : DataFrame avec uniquement les colonnes spécifiées
    """
    # Créer un dictionnaire de mappage des anciennes aux nouvelles colonnes
    column_mapping = dict(zip(columns_to_keep_original_list, columns_to_keep_final_list))

    # Filtrer les colonnes à garder
    df_filtered = df[columns_to_keep_original_list].copy()

    # Renommer les colonnes selon le mapping
    df_filtered.rename(columns=column_mapping, inplace=True)

    return df_filtered

def fct_harmonize_column_values(
    df:pd.DataFrame,
    col:str ,
    mapping_dict:Dict[str, str]
) -> pd.DataFrame:
    """
    Harmoniser les valeurs d'une colonne en utilisant un dictionnaire de mappage.
    Paramètres :
        df (pd.DataFrame) : DataFrame d'entrée
        col (str) : nom de la colonne à harmoniser
        mapping_dict (Dict[str, str]) : dictionnaire de mappage des valeurs
    Retour :
        pd.DataFrame : DataFrame avec la colonne harmonisée
    """
    if col not in df.columns:
        print(f"La colonne '{col}' n'existe pas dans le DataFrame.")
        return df

    df[col] = df[col].apply(lambda x: mapping_dict.get(x, x) if pd.notnull(x) else x)

    return df
