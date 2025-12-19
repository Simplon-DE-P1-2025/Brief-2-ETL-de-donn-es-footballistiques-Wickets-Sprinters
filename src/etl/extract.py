# -*- coding: utf-8 -*-
"""
Extraction utilities for football World Cup datasets.

This module contains helper functions used in the ETL pipeline to:
- Read CSV files with automatic delimiter detection
- Load and normalize nested JSON football data
- Apply basic DataFrame transformations for downstream processing

The extracted data is converted into pandas DataFrames and prepared
for transformation and loading into a database.
"""

import pandas as pd
import json
from typing import Dict
from pathlib import Path


def fct_read_csv(root_file: str) -> pd.DataFrame:
    """
    Read a CSV file and return its content as a pandas DataFrame.

    The function tries multiple separators (`,`, `;`, `|`, `\\t`) to automatically
    detect the correct delimiter. If the file does not exist or no valid separator
    is found, an empty DataFrame is returned and an informative message is printed.

    Parameters
    ----------
    root_file : str
        Path to the CSV file to read.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the CSV data if successful,
        otherwise an empty DataFrame.
    """
    seps = [',', ';', '|', '\t']

    if not Path(root_file).exists():
        print(f"Erreur : fichier {root_file} introuvable")
        return pd.DataFrame()

    for sep in seps:
        try:
            df = pd.read_csv(
                root_file,
                sep=sep,
                encoding="utf-8",
                skipinitialspace=True
            )
            if df.shape[1] > 1:
                return df
        except Exception:
            continue

    print(f"Aucun séparateur valide trouvé pour {root_file}")
    return pd.DataFrame()

def fct_read_json_nested(root_file: str) -> Dict[str, pd.DataFrame]:
    """
    Objectif :
        Charger un JSON imbriqué (teams, stadiums, tvchannels, groups, rounds, matches)
        dans des DataFrames séparés.
    Paramètres :
        root_file (str) : Chemin du fichier JSON.
    Retour :
        dict : Dictionnaire de DataFrames pandas :
            - 'teams', 'stadiums', 'tvchannels', 'groups', 'rounds',
            'matches', 'bridge_match_channels'
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
