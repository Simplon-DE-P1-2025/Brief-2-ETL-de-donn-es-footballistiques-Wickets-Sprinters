import pandas as pd 
import json

"""
Goal:
    Function to read a CSV file and return a pandas DataFrame.
Parameters:
    root_file (str): The path to the CSV file.
Returns:
    pd.DataFrame: The DataFrame containing the data from the CSV file.
"""
import pandas as pd

def fct_read_csv(root_file: str) -> pd.DataFrame:
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




"""
Goal
    Function to read a JSON file and return a pandas DataFrame.
Parameters:
    root_file (str): The path to the JSON file.
Returns:
    pd.DataFrame: The DataFrame containing the data from the JSON file.
"""
import pandas as pd
import json

import json
import pandas as pd

def fct_read_json_nested(root_file: str) -> dict:
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


"""
Goal:
    Function to add a prefix to all column names in a DataFrame.
Parameters:
    df (pd.DataFrame): The input DataFrame.
    prefix (str): The prefix to add to each column name.
Returns:
    pd.DataFrame: The DataFrame with updated column names.
"""
def fct_add_prefix_to_df(df:pd.DataFrame, prefix:str) -> pd.DataFrame:
    for col in df.columns:
        df.rename(columns={col: f"{prefix}_{col}"}, inplace=True)
    return df


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
def fct_extract_data(root_csv_2010: str, root_csv_2014: str, root_csv_2022: str, root_json_2018: str) -> None:
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
