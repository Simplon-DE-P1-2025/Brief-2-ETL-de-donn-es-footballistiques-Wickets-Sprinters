# -*- coding: utf-8 -*-
"""
Tests for the fct_transform_2010 function.
"""

import sys
from pathlib import Path
import pytest
import pandas as pd
from unittest.mock import patch
import pytest
import pandas as pd


# Ajouter src au chemin Python
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
from etl.transform import fct_transform_2010, trf_file_wcup_2014, fct_transform_data_2018

# Importer la fonction à tester
from etl.transform import fct_transform_data_2018

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

# ============================================================================
# Fixtures
# ============================================================================

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
    """Fixture simulant des valeurs manquantes."""
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

# ============================================================================
# Tests
# ============================================================================

def test_transform_2018_structure(sample_dfs_2018, sample_config_2018):
    df = fct_transform_data_2018(sample_dfs_2018, sample_config_2018)

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == sample_config_2018["list_wanted_columns"]


def test_transform_2018_column_types(sample_dfs_2018, sample_config_2018):
    df = fct_transform_data_2018(sample_dfs_2018, sample_config_2018)

    assert df["match_id"].dtype.name in ["int64", "Int64"]
    assert df["date"].dtype.name in ["string", "String", "object"]
    assert df["home_team"].dtype.name in ["string", "String", "object"]
    assert df["away_team"].dtype.name in ["string", "String", "object"]
    assert df["home_result"].dtype.name in ["int64", "Int64"]
    assert df["away_result"].dtype.name in ["int64", "Int64"]
    assert df["stage"].dtype.name in ["string", "String", "object"]
    assert df["edition"].dtype.name in ["int64", "Int64"]
    assert df["city"].dtype.name in ["string", "String", "object"]


def test_transform_2018_scores(sample_dfs_2018, sample_config_2018):
    df = fct_transform_data_2018(sample_dfs_2018, sample_config_2018)

    assert df.loc[0, "home_result"] == 4
    assert df.loc[0, "away_result"] == 0
    assert df.loc[1, "home_result"] == 2
    assert df.loc[1, "away_result"] == 1


def test_transform_2018_stage_values(sample_dfs_2018, sample_config_2018):
    df = fct_transform_data_2018(sample_dfs_2018, sample_config_2018)

    valid_stages = [
        "group_a", "group_b", "group_c", "group_d",
        "group_e", "group_f", "group_g", "group_h",
        "round_of_16", "quarter_finals",
        "semi_finals", "third_place", "final"
    ]

    assert df["stage"].isin(valid_stages).all()


def test_transform_2018_city_cleaning(sample_dfs_2018, sample_config_2018):
    df = fct_transform_data_2018(sample_dfs_2018, sample_config_2018)

    assert "." not in df.loc[0, "city"]


def test_transform_2018_team_names_capitalized(sample_dfs_2018, sample_config_2018):
    df = fct_transform_data_2018(sample_dfs_2018, sample_config_2018)

    for team in df["home_team"].tolist() + df["away_team"].tolist():
        assert team[0].isupper()


def test_transform_2018_edition(sample_dfs_2018, sample_config_2018):
    df = fct_transform_data_2018(sample_dfs_2018, sample_config_2018)

    assert (df["edition"] == 2018).all()


def test_transform_2018_match_id_uniqueness(sample_dfs_2018, sample_config_2018):
    df = fct_transform_data_2018(sample_dfs_2018, sample_config_2018)

    assert df["match_id"].is_unique
    assert df["match_id"].tolist() == sorted(df["match_id"].tolist())


def test_transform_2018_no_technical_columns(sample_dfs_2018, sample_config_2018):
    df = fct_transform_data_2018(sample_dfs_2018, sample_config_2018)

    forbidden_columns = [
        "team_homeid",
        "team_awayid",
        "group_group_id",
        "round_round_id",
        "stadium_id"
    ]

    for col in forbidden_columns:
        assert col not in df.columns


def test_transform_2018_null_values(sample_dfs_2018_with_nulls, sample_config_2018):
    df = fct_transform_data_2018(sample_dfs_2018_with_nulls, sample_config_2018)

    assert "notdefined" in df.loc[1, "stage"]
    assert not df.isna().any().any()
