# -*- coding: utf-8 -*-
"""
Tests for the fct_transform_2010 function.
"""
import pytest
import sys
from pathlib import Path
import pandas as pd


# Ajouter src au chemin Python
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
from etl.transform import fct_transform_2010

##########   test-2010   ##################################################################

@pytest.fixture
def sample_df_2010():
    """Sample input DataFrame mimicking the 2010 dataset."""
    return pd.DataFrame({
        "date": ["2010", "2010"],
        "home_team": ["France (FR)", "Spain (ES)"],
        "away_team": ["Brazil (BR)", "Netherlands (NL)"],
        "score": ["1-0", "0-1"],
        "stage": ["Final", "Semi-final"],
        "city": ["Johannesburg.", "Cape Town."]
    })


@pytest.fixture
def sample_config():
    """Minimal configuration dictionary for 2010 transformation."""
    return {
        "dict_columns_2010": {
            "date": "date",
            "home_team": "home_team",
            "away_team": "away_team",
            "stage": "stage",
            "city": "city"
        },
        "stage_mapping_2010": {
            "Final": "final",
            "Semi-final": "semi-final"
        },
        "columns_to_keep_2010": [
            "date",
            "home_team",
            "away_team",
            "home_result",
            "away_result",
            "stage",
            "edition",
            "city"
        ]
    }


def test_fct_transform_2010_basic(sample_df_2010, sample_config):
    """Test full transformation logic for 2010 dataset."""
    df_result = fct_transform_2010(sample_df_2010, sample_config)

    # --------------------
    # Structure
    # --------------------
    expected_columns = [
        "match_id",
        "date",
        "home_team",
        "away_team",
        "home_result",
        "away_result",
        "stage",
        "edition",
        "city"
    ]
    assert list(df_result.columns) == expected_columns

    # --------------------
    # Types and values
    # --------------------
    assert df_result["home_result"].dtype.name == "Int64"
    assert df_result["away_result"].dtype.name == "Int64"

    # Score split
    assert df_result.loc[0, "home_result"] == 1
    assert df_result.loc[0, "away_result"] == 0

    # Stage mapping
    assert df_result.loc[0, "stage"] == "final"

    # City cleaning
    assert "." not in df_result.loc[0, "city"]

    # Edition extraction
    assert df_result.loc[0, "edition"] == 2010

    # Match ID increment
    assert list(df_result["match_id"]) == [1, 2]

##########   test-2014   ##################################################################


##########   test-2018   ##################################################################
# tests/test_transform_2018.py

import pytest
import pandas as pd
from typing import Dict

# Importer la fonction à tester
from etl.transform import fct_transform_data_2018

# # ---------------------------------------------------
# # Fixtures pour les DataFrames simulés
# # ---------------------------------------------------
# @pytest.fixture
# def dfs_2018():
#     return {
#         'stadiums': pd.DataFrame({
#             'id': [1],
#             'name': ['stadium one'],
#             'city': ['cityone'],
#             'extra_col': ['to_drop']
#         }),
#         'teams': pd.DataFrame({
#             'id': [10],
#             'name': ['teamone'],
#             'extra_col': ['to_drop']
#         }),
#         'groups': pd.DataFrame({
#             'group_id': ['a'],
#             'group_name': ['group_a'],
#         }),
#         'rounds': pd.DataFrame({
#             'round_id': ['round_16'],
#             'round_name': ['round_of_16'],
#         }),
#         'matches': pd.DataFrame({
#             'match_id': [100],
#             'home_team_id': [10],
#             'away_team_id': [10],
#             'group_id': ['a'],
#             'round_id': ['round_16'],
#             'stadium_id': [1],
#             'stage': ['round_16'],
#             'type': ['type1'],
#             'date': ['2018-06-14T15:00:00'],
#             'home_result': [2],         # <- ajouté
#             'away_result': [1],         # <- ajouté
#         })
#     }


# @pytest.fixture
# def config():
#     return {
#         'stage_mapping_2018': {
#             'a': 'group_a',
#             'b': 'group_b',
#             'c': 'group_c',
#             'd': 'group_d',
#             'e': 'group_e',
#             'f': 'group_f',
#             'g': 'group_g',
#             'h': 'group_h',
#             'round_16': 'round_of_16',
#             'round_8': 'quarter_finals',
#             'round_4': 'semi_finals',
#             'round_2_loser': 'third_place',
#             'round_2': 'final'
#         },
#         'list_columns_original_2018': [
#             'match_match_id',
#             'match_formatted_date',
#             'team_homename',
#             'team_awayname',
#             'match_home_result',
#             'match_away_result',
#             'match_stage_name',
#             'match_edition',
#             'stadium_city'
#         ],
#         'list_wanted_columns': [
#             'match_id',
#             'date',
#             'home_team',
#             'away_team',
#             'home_result',
#             'away_result',
#             'stage',
#             'edition',
#             'city'
#         ]
#     }

# # ---------------------------------------------------
# # Test principal
# # ---------------------------------------------------
# def test_fct_transform_data_2018(dfs_2018, config):
#     # Appel de la fonction
#     df_final = fct_transform_data_2018(dfs_2018, config)
    
#     # Vérifications simples
#     assert isinstance(df_final, pd.DataFrame)
#     assert len(df_final) == 1  # On avait une seule ligne simulée
#     # Vérifie que toutes les colonnes voulues sont présentes
#     for col in config['list_wanted_columns']:
#         assert col in df_final.columns
#     # Vérifie quelques valeurs clés
#     assert df_final['match_id'].iloc[0] == 100


# ##########   test-2022   ##################################################################

import pytest
import pandas as pd

# --------------------
# Fixtures
# --------------------
@pytest.fixture
def sample_dfs_2018():
    """Sample input DataFrames mimicking the 2018 dataset."""
    return {
        "stadiums": pd.DataFrame({
            "id": [1, 2],
            "name": ["stadium one", "stadium two"],
            "city": ["Moscow.", "Saint Petersburg."]
        }),
        "teams": pd.DataFrame({
            "id": [1, 2],
            "name": ["France", "Croatia"]
        }),
        "groups": pd.DataFrame({
            "group_id": ["A", "B"],
            "group_name": ["Group A", "Group B"]
        }),
        "rounds": pd.DataFrame({
            "round_id": ["round_16", "round_8"],
            "round_name": ["Round of 16", "Quarter Finals"]
        }),
        "matches": pd.DataFrame({
            "match_id": [1, 2],
            "date": ["2018-07-15", "2018-07-14"],
            "home_team_id": [1, 2],
            "away_team_id": [2, 1],
            "home_result": [4, 2],
            "away_result": [2, 1],
            "stage": ["knockout", "knockout"],
            "type": ["knockout", "knockout"],
            "group_id": ["A", "B"],
            "round_id": ["round_16", "round_8"],
            "stadium_id": [1, 2],
            "channels": [[], []]
        })
    }

@pytest.fixture
def sample_config_2018():
    """Minimal configuration dictionary for 2018 transformation."""
    return {
        "stage_mapping_2018": {
            "a": "group_a",
            "b": "group_b",
            "round_16": "round_of_16",
            "round_8": "quarter_finals"
        },
        "list_columns_original_2018": [
            "match_match_id",
            "match_formatted_date",
            "team_homename",
            "team_awayname",
            "match_home_result",
            "match_away_result",
            "match_stage_name",
            "match_edition",
            "stadium_city"
        ],
        "list_wanted_columns": [
            "match_id",
            "date",
            "home_team",
            "away_team",
            "home_result",
            "away_result",
            "stage",
            "edition",
            "city"
        ]
    }

# --------------------
# Test fonction
# --------------------
def test_fct_transform_data_2018_basic(sample_dfs_2018, sample_config_2018):
    """Test full transformation logic for 2018 dataset."""
    df_result = fct_transform_data_2018(sample_dfs_2018, sample_config_2018)

    # --------------------
    # Structure
    # --------------------
    expected_columns = [
        "match_id",
        "date",
        "home_team",
        "away_team",
        "home_result",
        "away_result",
        "stage",
        "edition",
        "city"
    ]
    assert list(df_result.columns) == expected_columns

    # --------------------
    # Types et valeurs
    # --------------------
    assert df_result["match_id"].dtype.name in ["int64", "Int64"]
    assert df_result["date"].dtype.name in ["string","String", "object"]
    assert df_result["home_team"].dtype.name in ["string","String", "object"]
    assert df_result["away_team"].dtype.name in ["string","String", "object"]
    assert df_result["home_result"].dtype.name in ["int64", "Int64"]
    assert df_result["away_result"].dtype.name in ["int64", "Int64"]
    assert df_result["stage"].dtype.name in ["string","String", "object"]
    assert df_result["edition"].dtype.name in ["int64", "Int64"]
    assert df_result["city"].dtype.name in ["string","String", "object"]

    # Vérification des résultats des scores
    assert df_result.loc[0, "home_result"] == 4
    assert df_result.loc[0, "away_result"] == 2

    # Vérification que toutes les valeurs du stage sont dans le mapping
    valid_stages = ["group_a", "group_b", "group_c", "group_d",
                    "group_e", "group_f", "group_g", "group_h",
                    "round_of_16", "quarter_finals", "semi_finals",
                    "third_place", "final"]

    assert df_result["stage"].isin(valid_stages).all()


    # Vérification du nettoyage de la ville
    assert "." not in df_result.loc[0, "city"]

    # Vérification de l'édition
    assert df_result.loc[0, "edition"] == 2018

    # Vérification des match_id
    assert list(df_result["match_id"]) == [1, 2]
