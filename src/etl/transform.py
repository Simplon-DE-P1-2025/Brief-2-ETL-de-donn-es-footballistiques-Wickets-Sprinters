import pandas as pd 
from typing import Optional, Union, Dict, List, Any
import re
import numpy as np

from pyparsing import col



##########   2010   ##################################################################

def fct_transform_2010(df : pd.DataFrame , config : Dict) -> pd.DataFrame:
    """
    Goal:
        Fonction qui transforme le DataFrame du dataset 2010 selon les étapes définies.
    Parameters:
        df (pd.DataFrame): The input DataFrame for the 2010 dataset.
        config (str): Path to the configuration YAML file.
    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    
    # supprimer les doublons
    df = df.drop_duplicates()
    
    #creer colonnes 'home_result' et 'away_result'
    df['score'] = df['score'].str.slice(0, 3)
    df[['home_result','away_result']] = df['score'].str.split('-', expand=True)

    #convertir les colonnes 'home_result' et 'away_result' en type Int64
    df['home_result'] = (
        df['home_result']
        .astype("string")
        .str.extract(r"(\d+)", expand=False)
        .astype("Int64").fillna(0)
    )
    df['away_result'] = (
        df['away_result']
        .astype("string")
        .str.extract(r"(\d+)", expand=False)
        .astype("Int64").fillna(0)
    )
    
    #rename columns pour etre homogène avec les autres datasets
    dict_columns_2010 = config['dict_columns_2010']
    df = df.rename(columns=dict_columns_2010)
    
    #home_team et away_team, garder que le nom de pays en Anglais
    df[['home_team','home_team_lanorig']] = df['home_team'].str.split('(', expand=True)
    df[['away_team','away_team_lanorig']] = df['away_team'].str.split('(', expand=True)
    
    # Convertir la colonne 'date' en format YYYY en string 
    df["date"] = df["date"].astype("string")

    #garder que l'année pour 'edition'
    df['edition'] =df['date'].astype(int)
    
    #supprimer '.' dans colonne 'city'
    df['city'] = df['city'].str.replace('.', '', regex=False)
    
    # Harminser la colonne 'stage' avec les valeurs définies dans le fichier config.yaml
    stage_net = config['stage_mapping_2010']
    df["stage"] = df["stage"].replace(stage_net)
    
    #garder que des colonnes nescessaires
    columns_to_keep = config['columns_to_keep_2010']
    df = df[columns_to_keep]
    
    return df
    


##########   2014   ##################################################################

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

##########   2018   ##################################################################


def fct_load_config(config_filename: str = "config.yaml") -> dict:
    """
    Objectif :
        Charger les paramètres de configuration depuis un fichier YAML.
    Paramètres :
        config_filename (str) : Chemin relatif ou absolu du fichier YAML.
    Retour :
        dict : Paramètres de configuration.
    """
    config_path = Path(config_filename)

    # Résoudre le chemin relatif depuis la racine du projet
    if not config_path.is_absolute():
        project_root = Path(__file__).resolve().parents[1]
        config_path = project_root / config_path

    if not config_path.exists():
        raise FileNotFoundError(f"Fichier de configuration introuvable : {config_path}")

    # Charger le fichier YAML
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


def fct_add_prefix_to_df(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    Objectif :
        Ajouter un préfixe à toutes les colonnes d'un DataFrame.
    Paramètres :
        df (pd.DataFrame) : DataFrame d'entrée.
        prefix (str) : Préfixe à ajouter.
    Retour :
        pd.DataFrame : DataFrame avec les noms de colonnes modifiés.
    """
    for col in df.columns:
        df.rename(columns={col: f"{prefix}_{col}"}, inplace=True)
    return df


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
        print("[INFO] Aucune colonne spécifiée pour capitalize_string_columns. Le DataFrame reste inchangé.")
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
        print("[INFO] Aucune colonne spécifiée pour clean_string_columns_upper. Le DataFrame reste inchangé.")
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
        print("[INFO] Aucune colonne spécifiée pour clean_string_columns_lower. Le DataFrame reste inchangé.")
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
    df['edition'] = df[col].str[:4]
    return df

def fct_generate_unique_stage(df: pd.DataFrame, col_stage:str , col_round:str, col_group : str) -> pd.DataFrame:
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


def fct_final_columns_to_keep (df : pd.DataFrame, columns_to_keep_original_list : list, columns_to_keep_final_list : list) -> pd.DataFrame:
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

def fct_harmonize_column_values(df:pd.DataFrame, col:str , mapping_dict:Dict[str, str]) -> pd.DataFrame:
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

def fct_transform_data_2018(dfs_2018 : Dict[str, pd.DataFrame] , config: Dict) -> pd.DataFrame:
    """
    Transformer le DataFrame final des matches 2018 en gardant uniquement les colonnes spécifiées dans la configuration.
    
    Paramètres :
        dfs_2018 (List) : Liste des DataFrames transformés pour l'année 2018.
        config (Dict) : Dictionnaire de configuration contenant les paramètres nécessaires.
    
    Retour :
        pd.DataFrame : DataFrame final transformé pour l'année 2018.
    """
    
    df_stadiums = dfs_2018['stadiums'].copy()
    df_teams = dfs_2018['teams'].copy()
    df_groups = dfs_2018['groups'].copy()
    df_rounds = dfs_2018['rounds'].copy()
    df_matches = dfs_2018['matches'].copy()
    #-------------------------------------------------------------------------
    #------------------------stadiums transformations #------------------------
    #-------------------------------------------------------------------------
    # Filtrer les colonnes nécessaires
    columns_to_keep = ['id', 'name', 'city']
    df_stadiums_transformed = df_stadiums[columns_to_keep].copy()

    # Mettre en le premier caractère en Majuscule pour les colonnes name et city & convertir en type string
    df_stadiums_transformed = fct_capitalize_string_columns(df_stadiums_transformed, ['name', 'city'])
    
    #-------------------------------------------------------------------------
    #------------------------teams transformations #------------------------
    #-------------------------------------------------------------------------
    # Filtrer les colonnes nécessaires
    columns_to_keep = ['id', 'name']
    df_teams_transformed = df_teams[columns_to_keep].copy()

    # Mettre en le premier caractère en Majuscule pour la colonne name & convertir en type string
    df_teams_transformed = fct_capitalize_string_columns(df_teams_transformed, ['name'])
    
    #-------------------------------------------------------------------------
    #------------------------groups transformations #------------------------
    #-------------------------------------------------------------------------
    # Mettre en le premier caractère en Majuscule pour la colonne group_name & convertir en type string
    df_groups_transformed = fct_upper_string_columns(df_groups, ['group_name'])

    # Mettre en le premier caractère en Muniscule pour la colonne group_id & convertir en type string
    df_groups_transformed = fct_lower_string_columns(df_groups_transformed, ['group_id'])
    
    #-------------------------------------------------------------------------
    #------------------------rounds transformations #------------------------
    #-------------------------------------------------------------------------
    # Mettre en le premier caractère en Minuscule pour la colonne round_id & convertir en type string
    df_rounds_transformed = fct_lower_string_columns(df_rounds, ['round_id'])

    # Mettre en le premier caractère en Majuscule pour la colonne round_name & convertir en type string
    df_rounds_transformed = fct_capitalize_string_columns(df_rounds_transformed, ['round_name'])
    
    #-------------------------------------------------------------------------
    #------------------------matches transformations #------------------------
    #-------------------------------------------------------------------------
    # Mettre en le premier caractère en Minuscule pour la colonne group_id et round_id & convertir en type string
    df_matches_transformed = fct_lower_string_columns(df_matches, ['group_id','round_id'])

    # Mettre en le premier caractère en Majuscule pour la colonne stage et type & convertir en type string
    df_matches_transformed = fct_capitalize_string_columns(df_matches_transformed, ['stage','type'])

    # Remplir les valeurs manquantes dans 'round_id' et 'group_id' avec 'notdefined'
    df_matches_transformed['round_id'] = df_matches_transformed['round_id'].fillna('notdefined')
    df_matches_transformed['group_id'] = df_matches_transformed['group_id'].fillna('notdefined')

    # Générer une colonne unique 'stage' en combinant les colonnes 'round_id' et 'group_id'.
    df_matches_transformed = fct_generate_unique_stage(df_matches_transformed, 'stage', 'round_id', 'group_id')

    # Extraire l'année de la colonne date pour créer la colonne edition
    df_matches_transformed['edition'] = fct_extract_edition(df_matches_transformed, 'date')['edition']

    # Convertir la colonne date ISO en format YYYYMMDDhhmmss
    df_matches_transformed = fct_iso_to_yyyymmddhhmmss(df_matches_transformed, 'date', 'formatted_date')

    # Eclater les listes dans la colonne 'channels' en plusieurs lignes
    # df_matches_transformed = df_matches_transformed.explode('channels').reset_index(drop=True)

    #Génerer une colonne unique 'stage' en combinant les colonnes 'round_id' et 'group_id'.
    df_matches_transformed = fct_generate_unique_stage(df_matches_transformed, 'stage', 'round_id', 'group_id')
    
    # Harmoniser la colonne stage_name en fonction des valeurs de stage
    stage_mapping = config['stage_mapping_2018']
    df_matches_transformed = fct_harmonize_column_values(df_matches_transformed, 'stage_name', stage_mapping)
    #-------------------------------------------------------------------------
    #-----------------------Merger en dataframe finale------------------------
    #-------------------------------------------------------------------------
    #ajouter des préfixes aux colonnes des dataframes transformés:
    df_teams_home= df_teams_transformed.add_prefix('team_home')
    df_teams_away= df_teams_transformed.add_prefix('team_away')
    df_groups_transformed = df_groups_transformed.add_prefix('group_')
    df_rounds_transformed = df_rounds_transformed.add_prefix('round_')
    df_stadiums_transformed = df_stadiums_transformed.add_prefix('stadium_')
    # df_tvchannels_transformed = df_tvchannels_transformed.add_prefix('channel_')
    df_matches_transformed = df_matches_transformed.add_prefix('match_')

    #faire le merge entre les dataframes transformés pour obtenir un dataframe final des matches complet
    df_2018_final = (df_matches_transformed
                    .merge(df_teams_home, left_on='match_home_team_id', right_on='team_homeid', how='left')
                    .merge(df_teams_away, left_on='match_away_team_id', right_on='team_awayid', how='left')
                    .merge(df_groups_transformed, left_on='match_group_id', right_on='group_group_id', how='left')
                    .merge(df_rounds_transformed, left_on='match_round_id', right_on='round_round_id', how='left')
                    .merge(df_stadiums_transformed, left_on='match_stadium_id', right_on='stadium_id', how='left')
    )
    #-------------------------------------------------------------------------
    #--------------------Garder que les colonnes demandées--------------------
    #-------------------------------------------------------------------------
    list_columns_original = config['list_columns_original_2018']
    list_columns_final = config['list_wanted_columns']
    df_2018_final = fct_final_columns_to_keep(df_2018_final, list_columns_original, list_columns_final).sort_values(by='match_id').reset_index(drop=True)
    
    return df_2018_final
        
                            


######################## 2022  #################################################

def transform_2022_data(df: pd.DataFrame , config: dict) -> pd.DataFrame:
    """
    Transforme les données brutes des matchs 2022 en un format nettoyé et standardisé.

    Les opérations réalisées sont :
    - Sélection et renommage des colonnes utiles
    - Nettoyage du format de l'heure
    - Fusion de la date et de l'heure en un timestamp unique (YYYYMMDDHHMMSS)
    - Normalisation des noms d'équipes (Title Case)
    - Conversion des scores en entiers (nullable Int64)

    Parameters
    ----------
    raw_df : pandas.DataFrame
        DataFrame contenant les données brutes des matchs avec au minimum
        les colonnes suivantes :
        - 'team1'
        - 'team2'
        - 'number of goals team1'
        - 'number of goals team2'
        - 'date'
        - 'hour'
        - 'category'

    Returns
    -------
    pandas.DataFrame
        DataFrame transformé avec les colonnes :
        - home_team (str)
        - away_team (str)
        - home_result (Int64)
        - away_result (Int64)
        - date (str, format YYYYMMDDHHMMSS)
        - stage (str)

    Notes
    -----
    - Les dates invalides ou mal formées sont converties en NaT puis en NaN.
    - Les scores non numériques sont convertis en valeurs manquantes (pd.NA).
    """
    
    list_wanted_columns = [
    'team1', 
    'team2',
    'number of goals team1', 
    'number of goals team2', 
    'date',
    'hour',
    'category'
    ]

    # Filtrage des colonnes
    df_filtered = df[list_wanted_columns].copy()

    df_filtered = df_filtered.rename(columns={
    "team1": "home_team",
    "team2": "away_team",
    "number of goals team1": "home_result",
    "number of goals team2": "away_result",
    "category": "stage",
    })

    # nettoyer l'heure "17 : 00" -> "17:00"
    df_filtered["hour"] = df_filtered["hour"].astype("string").str.replace(" ", "", regex=False)

    # parse date + hour (mois en anglais)
    # dt = pd.to_datetime(
    # df_filtered["date"].astype("string").str.strip() + " " + df_filtered["hour"].astype("string"),
    # errors="coerce"
    # )
    dt = pd.to_datetime(
        df_filtered["date"].astype(str).str.strip() + " " + df_filtered["hour"].astype(str).str.strip(),
        format="%d %b %Y %H:%M",  # exemple : '01 Jan 2022 15:30'
        errors="coerce",
        dayfirst=True  # si ton format est jour/mois/année
    )

    df_filtered["date"] = dt.dt.strftime("%Y%m%d%H%M%S")
    df_filtered = df_filtered.drop(columns=["hour"])

    #Noms des équipes avec la première lettre en majuscule
    df_filtered["home_team"] = df_filtered["home_team"].astype("string").str.strip().str.lower().str.title()
    df_filtered["away_team"] = df_filtered["away_team"].astype("string").str.strip().str.lower().str.title()

    # Résultats en int 
    df_filtered["home_result"] = pd.to_numeric(df_filtered["home_result"], errors="coerce").astype("Int64")
    df_filtered["away_result"] = pd.to_numeric(df_filtered["away_result"], errors="coerce").astype("Int64")
    
    # Harminser la colonne 'stage' avec les valeurs définies dans le fichier config.yaml
    mapping_dict = config['stage_mapping_2022']
    df_filtered["stage"] = df_filtered["stage"].replace(mapping_dict)
    
    # Trier par date (ascendant : plus ancien en premier)
    df_filtered.sort_values("date", inplace=True)
    # Réinitialiser l'index incrémental pour match_id
    df_filtered["match_id"] = range(1, len(df_filtered) + 1)
    df_filtered['edition'] = df_filtered['date'].str[:4].astype(int)
    df_filtered["city"] = None
    
    # Réorganisation des colonnes du DataFrame
    df_filtered = df_filtered[["match_id", "date", "home_team", "away_team", "home_result", "away_result", "stage", "edition", "city"]]


    
    return df_filtered