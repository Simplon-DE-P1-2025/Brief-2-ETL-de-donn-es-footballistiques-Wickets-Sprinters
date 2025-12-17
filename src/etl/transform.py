import pandas as pd 
from typing import Optional, Union, Dict, List, Any
import re

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
        dt = pd.to_datetime(x, dayfirst=True)

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

def trf_file_wcup_2014(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Transforme et normalise les données des matchs de la Coupe du Monde 2014.

    Cette fonction applique une série de traitements sur un DataFrame brut
    contenant les matchs de la Coupe du Monde 2014 :
    - sélection des colonnes pertinentes,
    - normalisation et renommage des colonnes,
    - création d'une colonne de date normalisée,
    - normalisation des valeurs de la colonne `stage`,
    - détection et correction des anomalies dans les colonnes `home_team`
      et `away_team`,
    - tri chronologique des matchs,
    - création d'un identifiant unique de match (`match_id`),
    - réorganisation finale des colonnes.

    Les règles de transformation (colonnes retenues, mappings de renommage
    et de normalisation) sont définies dans le dictionnaire de configuration.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame contenant les données brutes des matchs de la Coupe du Monde 2014.
    config : dict
        Dictionnaire de configuration contenant la clé
        ``'trf_file_wcup_2014'`` avec notamment :
        - ``colonnes_retenues`` : liste des colonnes à conserver,
        - ``news_columns`` : dictionnaire de renommage des colonnes,
        - ``stage_mapping`` : dictionnaire de normalisation des phases de compétition,
        - ``correction_team_mapping`` : dictionnaire de correction des noms d'équipes.

    Returns
    -------
    pandas.DataFrame
        DataFrame transformé et normalisé, contenant les colonnes suivantes :
        ``match_id``, ``date``, ``home_team``, ``away_team``,
        ``home_result``, ``away_result``, ``stage``, ``edition``, ``city``.

    Notes
    -----
    - La fonction utilise les fonctions externes ``normalize_datetime`` et
      ``test_country_column``.
    - Les anomalies détectées sur les colonnes d'équipes sont affichées
      via des impressions (`print`).
    - Le DataFrame retourné est trié par date croissante.
    """
    
    
    # Sélection des colonnes pertinentes
    df_2014_news = df[config['trf_file_wcup_2014']['colonnes_retenues']]

    # Normalisation des noms de colonnes
    df_2014_news.columns = df_2014_news.columns.str.lower().str.replace(" ", "_")

    # Renommage des colonnes pour uniformisation
    df_2014_news = df_2014_news.rename(
        columns=config['trf_file_wcup_2014']['news_columns']
    )

    # Création d’une colonne date normalisée
    df_2014_news["date"] = df_2014_news["datetime"].apply(normalize_datetime)

    # Récupérer le valeurs distinctes de la colonne stage
    df_2014_news["stage"].unique()

    # normalisation des noms de la colonne stage
    # à partir d'un dictionnaire ``stage_mapping`` dans la config
    df_2014_news["stage"] = df_2014_news["stage"].map(config['trf_file_wcup_2014']['stage_mapping']).fillna(df_2014_news["stage"])

    # Détection des anomalies dans la colonne home_team
    home_team_results = test_country_column(df_2014_news, "home_team")
    print(home_team_results)

    # normalisation des noms de la colonne home_team
    df_2014_news["home_team"] = df_2014_news["home_team"].map(config['trf_file_wcup_2014']['correction_team_mapping']).fillna(df_2014_news["home_team"])


    # Détection des anomalies dans la colonne away_team
    away_team_results = test_country_column(df_2014_news, "away_team")
    print(away_team_results)

    # normalisation des noms de la colonne away_team
    df_2014_news["away_team"] = df_2014_news["away_team"].map(config['trf_file_wcup_2014']['correction_team_mapping']).fillna(df_2014_news["away_team"])
    df_2014_news["away_team"].unique()


    # Trier par date (ascendant : plus ancien en premier)
    df_2014_news.sort_values("date", inplace=True)
    # Réinitialiser l'index incrémental pour match_id
    df_2014_news["match_id"] = range(1, len(df_2014_news) + 1)

    # Réorganisation des colonnes du DataFrame
    df_2014_news = df_2014_news[["match_id", "date", "home_team", "away_team", "home_result", "away_result", "stage", "edition", "city"]]
    
    return df_2014_news