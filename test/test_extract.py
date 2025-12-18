import pytest
import pandas as pd
import json
from io import StringIO
from pathlib import Path
import sys

# Ajouter src au chemin Python
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
from etl.extract import fct_read_csv, fct_read_json_nested, fct_add_prefix_to_df, fct_extract_data

# --------------------
# Fixtures pour fichiers temporaires
# --------------------
@pytest.fixture
def csv_content():
    return "col1,col2,col3\n1,2,3\n4,5,6"

@pytest.fixture
def json_content():
    return {
        "teams": [{"id": 1, "name": "Team A"}, {"id": 2, "name": "Team B"}],
        "stadiums": [{"id": 1, "name": "Stadium A"}],
        "tvchannels": [{"id": 1, "name": "Channel A"}],
        "groups": {
            "A": {
                "name": "Group A",
                "winner": 1,
                "runnerup": 2,
                "matches": [
                    {"name": "M1", "type": "group", "home_team": 1, "away_team": 2, "home_result": 1, "away_result": 2, "date": "2022-01-01", "stadium": 1, "finished": True, "matchday": 1, "channels": [1], "winner": 2}
                ]
            }
        },
        "knockout": {}
    }

# --------------------
# Tests
# --------------------
def test_fct_read_csv(tmp_path, csv_content):
    file = tmp_path / "test.csv"
    file.write_text(csv_content)
    
    df = fct_read_csv(str(file))
    
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 3)
    assert list(df.columns) == ["col1", "col2", "col3"]

def test_fct_read_csv_file_not_found():
    df = fct_read_csv("non_existent.csv")
    assert df.empty

def test_fct_add_prefix_to_df():
    df = pd.DataFrame({"a": [1], "b": [2]})
    df_prefixed = fct_add_prefix_to_df(df.copy(), "test")
    assert list(df_prefixed.columns) == ["test_a", "test_b"]

def test_fct_read_json_nested(tmp_path, json_content):
    file = tmp_path / "test.json"
    file.write_text(json.dumps(json_content))
    
    dfs = fct_read_json_nested(str(file))
    
    assert isinstance(dfs, dict)
    assert set(dfs.keys()) >= {"teams", "stadiums", "tvchannels", "matches", "groups", "rounds", "bridge_match_channels"}
    
    assert not dfs["teams"].empty
    assert not dfs["matches"].empty
    assert dfs["matches"].iloc[0]["match_id"] == "M1"

def test_fct_extract_data(tmp_path, csv_content, json_content):
    # Créer fichiers CSV temporaires
    csv_files = []
    for year in ["2010", "2014", "2022"]:
        f = tmp_path / f"{year}.csv"
        f.write_text(csv_content)
        csv_files.append(str(f))
    
    # Créer fichier JSON temporaire
    json_file = tmp_path / "2018.json"
    json_file.write_text(json.dumps(json_content))
    
    result = fct_extract_data(*csv_files, str(json_file))
    
    # Actuellement la fonction retourne None
    assert result is None
