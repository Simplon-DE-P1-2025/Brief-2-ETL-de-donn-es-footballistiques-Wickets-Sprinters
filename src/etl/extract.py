import pandas as pd 
import json
from typing import Dict


def fct_read_csv(root_file: str) -> pd.DataFrame:
    """
    Goal:
        Function to read a CSV file and return a pandas DataFrame.
    Parameters:
        root_file (str): The path to the CSV file.
    Returns:
        pd.DataFrame: The DataFrame containing the data from the CSV file.
    """
    seps = [',', ';', '|', '\t']

    try:
        for sep in seps:
            try:
                df = pd.read_csv(
                    root_file,
                    sep=sep,
                    encoding="utf-8",
                    skipinitialspace=True
                )

                # Si plus d'une colonne → bon séparateur
                if df.shape[1] > 1:
                    return df

            except Exception:
                continue

        print(f"Aucun séparateur valide trouvé pour {root_file}")
        return pd.DataFrame()

    except FileNotFoundError:
        print(f"Erreur : fichier {root_file} introuvable")
        return pd.DataFrame()


def fct_read_json_nested(root_file: str) -> dict:
    """
    Goal
        Function to read a JSON file and return a pandas DataFrame.
    Parameters:
        root_file (str): The path to the JSON file.
    Returns:
        pd.DataFrame: The DataFrame containing the data from the JSON file.
    """
    with open(root_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    dfs = {}

    # --------------------
    # DIMENSIONS
    # --------------------
    dfs['stadiums'] = pd.DataFrame(data.get('stadiums', []))
    dfs['tvchannels'] = pd.DataFrame(data.get('tvchannels', []))
    dfs['teams'] = pd.DataFrame(data.get('teams', []))

    # --------------------
    # MATCHES - GROUP STAGE
    # --------------------
    matches = []

    for group_key, group_data in data.get('groups', {}).items():
        for match in group_data.get('matches', []):
            match_flat = match.copy()
            match_flat['group'] = group_key
            match_flat['stage'] = 'group'
            matches.append(match_flat)

    # --------------------
    # MATCHES - KNOCKOUT
    # --------------------
    for round_key, round_data in data.get('knockout', {}).items():
        for match in round_data.get('matches', []):
            match_flat = match.copy()
            match_flat['group'] = None
            match_flat['stage'] = round_key
            matches.append(match_flat)

    dfs['matches'] = pd.DataFrame(matches)

    return dfs



def fct_add_prefix_to_df(df:pd.DataFrame, prefix:str) -> pd.DataFrame:
    """
    Goal:
        Function to add a prefix to all column names in a DataFrame.
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        prefix (str): The prefix to add to each column name.
    Returns:
        pd.DataFrame: The DataFrame with updated column names.
    """
    for col in df.columns:
        df.rename(columns={col: f"{prefix}_{col}"}, inplace=True)
    return df



def fct_extract_data(
    root_csv_2010: str,
    root_csv_2014: str,
    root_csv_2022: str,
    root_json_2018: str
) -> None:
    """
    Goal:
        Function to extract data from a CSV files and JSON file to a consolidated DataFrame df.
    Parameters:
        root_csv_2010 (str): The path to the first CSV file.
        root_csv_2014 (str): The path to the second CSV file.
        root_csv_2022 (str): The path to the third CSV file.
        root_json_2018 (str): The path to the JSON file.
    Returns:
        pd.DataFrame: The consolidated DataFrame containing data from all files.
    """
    df_2010 = fct_read_csv(root_csv_2010)
    df_2014 = fct_read_csv(root_csv_2014)
    df_2022 = fct_read_csv(root_csv_2022)
    df_2018 = fct_read_json_nested(root_json_2018)
    
    print(df_2010.head())
    print(df_2014.head())
    print(df_2022.head())
    print(df_2018['matches'].head())
    # df = pd.concat([df_2010, df_2014, df_2022, df_2018], ignore_index=True)
    return None


def fct_read_json_nested(root_file: str) -> Dict[str, pd.DataFrame]:
    """
    Objectif :
        Charger un JSON imbriqué (teams, stadiums, tvchannels, groups, rounds, matches)
        dans des DataFrames séparés.
    Paramètres :
        root_file (str) : Chemin du fichier JSON.
    Retour :
        dict : Dictionnaire de DataFrames pandas :
            - 'teams', 'stadiums', 'tvchannels', 'groups', 'rounds', 'matches', 'bridge_match_channels'
    """
    with open(root_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    dfs = {}
    # Entités simples
    dfs['teams'] = pd.DataFrame(data.get('teams', []))
    dfs['stadiums'] = pd.DataFrame(data.get('stadiums', []))
    dfs['tvchannels'] = pd.DataFrame(data.get('tvchannels', []))

    # Groupes
    groups_rows = []
    for group_id, group_data in data.get('groups', {}).items():
        groups_rows.append({
            'group_id': group_id,
            'group_name': group_data.get('name'),
            'winner_team_id': group_data.get('winner'),
            'runnerup_team_id': group_data.get('runnerup')
        })
    dfs['groups'] = pd.DataFrame(groups_rows)

    # Matchs et liens match-channel
    matches_rows = []
    match_channels_rows = []

    # Phase de groupes
    for group_id, group_data in data.get('groups', {}).items():
        for match in group_data.get('matches', []):
            match_id = match.get('name')
            matches_rows.append({
                'match_id': match_id,
                'type': match.get('type'),
                'stage': 'group',
                'group_id': group_id,
                'round_id': None,
                'date': match.get('date'),
                'stadium_id': match.get('stadium'),
                'home_team_id': match.get('home_team'),
                'away_team_id': match.get('away_team'),
                'home_result': match.get('home_result'),
                'away_result': match.get('away_result'),
                'home_penalty': None,
                'away_penalty': None,
                'winner': match.get('winner'),
                'finished': match.get('finished'),
                'matchday': match.get('matchday'),
                'channels': match.get('channels', [])
            })
            for channel_id in match.get('channels', []):
                match_channels_rows.append({'match_id': match_id, 'channel_id': channel_id})

    # Phase à élimination directe
    rounds_rows = []
    for round_key, round_data in data.get('knockout', {}).items():
        rounds_rows.append({'round_id': round_key, 'round_name': round_data.get('name')})
        for match in round_data.get('matches', []):
            match_id = match.get('name')
            matches_rows.append({
                'match_id': match_id,
                'type': match.get('type'),
                'stage': 'knockout',
                'group_id': None,
                'round_id': round_key,
                'date': match.get('date'),
                'stadium_id': match.get('stadium'),
                'home_team_id': match.get('home_team'),
                'away_team_id': match.get('away_team'),
                'home_result': match.get('home_result'),
                'away_result': match.get('away_result'),
                'home_penalty': match.get('home_penalty'),
                'away_penalty': match.get('away_penalty'),
                'winner': match.get('winner'),
                'finished': match.get('finished'),
                'matchday': match.get('matchday'),
                'channels': match.get('channels', [])
            })
            for channel_id in match.get('channels', []):
                match_channels_rows.append({'match_id': match_id, 'channel_id': channel_id})

    # Convertir en DataFrames
    dfs['matches'] = pd.DataFrame(matches_rows)
    dfs['bridge_match_channels'] = pd.DataFrame(match_channels_rows)
    dfs['rounds'] = pd.DataFrame(rounds_rows)

    return dfs

