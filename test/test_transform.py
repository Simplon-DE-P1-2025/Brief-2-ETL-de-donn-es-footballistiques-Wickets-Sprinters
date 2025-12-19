# -*- coding: utf-8 -*-
"""
Tests for the fct_transform_2010 function.
"""

import sys
from pathlib import Path
import pytest
import pandas as pd
from unittest.mock import patch

# Ajouter src au chemin Python
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
from etl.transform import fct_transform_2010, trf_file_wcup_2014, fct_transform_data_2018

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

<<<<<<< HEAD


##########   test-2018   ##################################################################
# tests/test_transform_2018.py

=======
##########   test-2018   ##################################################################
# tests/test_transform_2018.py

import pytest
import pandas as pd
from typing import Dict

# Importer la fonction à tester
from etl.transform import fct_transform_data_2018

>>>>>>> e35c46bc234325cc0cbb3c8596dd11891e30d161
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
            "group_name": ["Group A", "Group B"],
            "winner_team_id": [1, 2],
            "runnerup_team_id": [2, 1]
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
            "away_result": [0, 1],
            "stage": ["knockout", "knockout"],
            "type": ["knockout", "knockout"],
            "group_id": ["A", "B"],
            "round_id": ["round_16", "round_8"],
            "stadium_id": [1, 2],
            "channels": [[], []]
        })
    }

@pytest.fixture
def sample_dfs_2018_with_nulls(sample_dfs_2018):
    """Fixture qui simule des valeurs manquantes pour tester le remplissage des NaN."""
    dfs = sample_dfs_2018.copy()
    dfs["matches"].loc[1, "round_id"] = None
    dfs["matches"].loc[1, "group_id"] = None
    dfs["matches"].loc[1, "stadium_id"] = None
    return dfs

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

def test_fct_transform_data_2018_null_values(sample_dfs_2018_with_nulls, sample_config_2018):
    """Test la gestion des valeurs null / NaN dans les matches."""
    df_result = fct_transform_data_2018(sample_dfs_2018_with_nulls, sample_config_2018)

    # Vérifier que 'round_id' et 'group_id' manquants sont remplacés par 'notdefined' dans le stage
    assert "notdefined" in df_result.loc[1, "stage"]

    # Vérifier que les colonnes finales ne contiennent plus de NaN
    assert not df_result.isna().any().any()

def test_fct_transform_data_2018_basic(sample_dfs_2018, sample_config_2018):
    """Test complet de la transformation pour le dataset 2018."""
    df_result = fct_transform_data_2018(sample_dfs_2018, sample_config_2018)

    # Vérifications simples
    assert isinstance(df_result, pd.DataFrame)
    
    # --------------------
    # Structure
    # --------------------
    assert list(df_result.columns) == sample_config_2018["list_wanted_columns"]

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
    assert df_result["edition"].dtype.name in ["int64", "Int64"]
    assert df_result["city"].dtype.name in ["string","String", "object"]

    # Vérification des résultats des scores
    assert df_result.loc[0, "home_result"] == 4
    assert df_result.loc[0, "away_result"] == 0
    assert df_result.loc[1, "home_result"] == 2
    assert df_result.loc[1, "away_result"] == 1

    # Vérification que toutes les valeurs du stage sont dans le mapping
    valid_stages = ["group_a", "group_b", "group_c", "group_d",
                    "group_e", "group_f", "group_g", "group_h",
                    "round_of_16", "quarter_finals", "semi_finals",
                    "third_place", "final"]
    assert df_result["stage"].isin(valid_stages).all()

    # Vérification du nettoyage de la ville
    assert "." not in df_result.loc[0, "city"]

    # Vérifier les noms des équipes (capitalisation)
    for team in df_result["home_team"].tolist() + df_result["away_team"].tolist():
        assert team[0].isupper()
        
    # Vérification de l'édition
    assert df_result.loc[0, "edition"] == 2018

    # Vérifier la présence unique de chaque match_id
    assert df_result["match_id"].is_unique
    
    # Vérification des match_id
    assert list(df_result["match_id"]) == [1, 2]
    
    # Vérifier que les colonnes liées aux merges ne sont pas présentes
    for col in ["team_homeid", "team_awayid", "group_group_id", "round_round_id", "stadium_id"]:
        assert col not in df_result.columns

    # --------------------
    # Vérification de l'ordre des lignes
    # --------------------
    assert df_result["match_id"].tolist() == sorted(df_result["match_id"].tolist())


# ##########   test-2022   ##################################################################
