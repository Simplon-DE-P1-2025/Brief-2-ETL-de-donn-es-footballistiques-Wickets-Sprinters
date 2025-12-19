import pytest
import pandas as pd
import json
from pathlib import Path
import sys

# Ajouter src au chemin Python
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))
from etl.extract import fct_read_csv, fct_read_json_nested

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


def test_fct_read_csv_file_not_found(capsys):
    fake_path = "fichier_qui_n_existe_pas.csv"

    df = fct_read_csv(fake_path)

    # 1️⃣ Vérifier que le DataFrame est vide
    assert isinstance(df, pd.DataFrame)
    assert df.empty

    # 2️⃣ Vérifier le message affiché
    captured = capsys.readouterr()
    assert f"Erreur : fichier {fake_path} introuvable" in captured.out


def test_fct_read_csv_no_valid_separator(tmp_path, capsys):
    # Créer un CSV avec une seule colonne
    file = tmp_path / "one_column.csv"
    file.write_text("col1\nvalue1\nvalue2")

    df = fct_read_csv(str(file))

    # Le DataFrame retourné doit être vide
    assert isinstance(df, pd.DataFrame)
    assert df.empty

    # Vérifier le message affiché
    captured = capsys.readouterr()
    assert f"Aucun séparateur valide trouvé pour {file}" in captured.out


def test_fct_read_csv_exception_continue(tmp_path, capsys, monkeypatch):
    # Créer un fichier CSV valide (mais on va casser read_csv)
    file = tmp_path / "test.csv"
    file.write_text("a,b\n1,2")

    # Fake read_csv qui lève une Exception
    def fake_read_csv(*args, **kwargs):
        raise Exception("Erreur forcée")

    # Monkeypatch de pandas.read_csv
    monkeypatch.setattr(pd, "read_csv", fake_read_csv)

    df = fct_read_csv(str(file))

    # La fonction doit continuer sans lever d'exception
    assert isinstance(df, pd.DataFrame)
    assert df.empty

    # Vérifier que le message final est affiché
    captured = capsys.readouterr()
    assert f"Aucun séparateur valide trouvé pour {file}" in captured.out

def test_fct_read_json_nested(tmp_path, json_content):
    file = tmp_path / "test.json"
    file.write_text(json.dumps(json_content))
    
    dfs = fct_read_json_nested(str(file))
    
    assert isinstance(dfs, dict)
    assert set(dfs.keys()) >= {"teams", "stadiums", "tvchannels", "matches", "groups", "rounds", "bridge_match_channels"}
    
    assert not dfs["teams"].empty
    assert not dfs["matches"].empty
    assert dfs["matches"].iloc[0]["match_id"] == "M1"
    

def test_fct_read_json_nested_knockout_part(tmp_path):
    data = {
        "teams": [],
        "stadiums": [],
        "tvchannels": [],
        "groups": {},
        "knockout": {
            "final": {
                "name": "Final",
                "matches": [
                    {
                        "name": "FRA-CRO",
                        "type": "knockout",
                        "date": "2018-07-15",
                        "stadium": "stadium_1",
                        "home_team": "FRA",
                        "away_team": "CRO",
                        "home_result": 4,
                        "away_result": 2,
                        "home_penalty": None,
                        "away_penalty": None,
                        "winner": "FRA",
                        "finished": True,
                        "matchday": 7,
                        "channels": ["TF1", "beIN"]
                    }
                ]
            }
        }
    }

    json_file = tmp_path / "knockout.json"
    json_file.write_text(json.dumps(data))

    dfs = fct_read_json_nested(str(json_file))

    # ---------- rounds ----------
    assert not dfs["rounds"].empty
    assert dfs["rounds"].iloc[0]["round_id"] == "final"
    assert dfs["rounds"].iloc[0]["round_name"] == "Final"

    # ---------- matches ----------
    assert not dfs["matches"].empty
    match = dfs["matches"].iloc[0]

    assert match["stage"] == "knockout"
    assert match["round_id"] == "final"
    assert match["group_id"] is None
    assert match["match_id"] == "FRA-CRO"

    # ---------- bridge_match_channels ----------
    bridge = dfs["bridge_match_channels"]
    assert len(bridge) == 2
    assert set(bridge["channel_id"]) == {"TF1", "beIN"}
    assert all(bridge["match_id"] == "FRA-CRO")
