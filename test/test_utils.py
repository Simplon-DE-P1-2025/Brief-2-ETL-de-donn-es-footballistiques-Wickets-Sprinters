# -*- coding: utf-8 -*-
"""
Tests unitaires pour la fonction fct_load_config.

Ces tests couvrent :
- le chargement d'un fichier de configuration via un chemin absolu
- le chargement via un chemin relatif en contexte script Python (__file__)
- le chargement via un chemin relatif en contexte Jupyter (__file__ absent)
- la levée d'une exception si le fichier n'existe pas
"""

import os
import yaml
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, mock_open
import builtins

from etl.utils import fct_load_config, normalize_datetime


# ============================================================================
# TEST 1 — Chemin ABSOLU
# ============================================================================
def test_fct_load_config_absolute_path(tmp_path):
    """
    Vérifie que le fichier YAML est correctement chargé
    lorsque le chemin fourni est absolu.
    """
    config_content = {
        "db": {
            "host": "localhost",
            "port": 5432
        }
    }

    config_file = tmp_path / "config.yaml"
    with open(config_file, "w", encoding="utf-8") as f:
        yaml.dump(config_content, f)

    result = fct_load_config(str(config_file))

    assert result == config_content

# ====================================================================
# TEST CHEMIN RELATIF AVEC __file__ (script Python)
# ====================================================================
def test_fct_load_config_relative_with_file_mock(monkeypatch):
    config_content = {"env": "production"}
    m = mock_open(read_data=yaml.dump(config_content))

    # simuler __file__ dans utils.py
    fake_file = "/fake/project/src/etl/utils.py"
    monkeypatch.setitem(globals(), "__file__", fake_file)

    with patch("builtins.open", m), patch("pathlib.Path.exists", lambda self: True):
        result = fct_load_config("config.yaml")

    assert result == config_content


# ====================================================================
# TEST CHEMIN RELATIF SANS __file__ (Notebook)
# ====================================================================
def test_fct_load_config_relative_without_file_mock(monkeypatch):
    config_content = {"notebook": True}
    m = mock_open(read_data=yaml.dump(config_content))

    monkeypatch.delenv("__file__", raising=False)
    monkeypatch.setattr("os.getcwd", lambda: "/fake/notebook")

    with patch("builtins.open", m), patch("pathlib.Path.exists", lambda self: True):
        result = fct_load_config("config.yaml")

    assert result == config_content


# ============================================================================
# TEST 4 — Fichier INEXISTANT
# ============================================================================
def test_fct_load_config_file_not_found(tmp_path):
    """
    Vérifie que la fonction lève une FileNotFoundError
    si le fichier de configuration n'existe pas.
    """
    missing_file = tmp_path / "missing_config.yaml"

    with pytest.raises(FileNotFoundError) as exc_info:
        fct_load_config(str(missing_file))

    assert "Config file not found" in str(exc_info.value)


def test_normalize_datetime_valid_str():
    # Chaîne au format "12 Jun 2014 - 17:00"
    date_str = "12 Jun 2014 - 17:00"
    result = normalize_datetime(date_str)
    assert result == "20140612170000"

def test_normalize_datetime_valid_timestamp():
    # Objet pandas.Timestamp
    ts = pd.Timestamp("2014-06-12 17:00")
    result = normalize_datetime(ts)
    assert result == "20140612170000"

def test_normalize_datetime_invalid_str():
    # Chaîne non date → doit retourner None
    bad_str = "invalid date"
    result = normalize_datetime(bad_str)
    assert result is None

def test_normalize_datetime_none():
    # Valeur None → doit retourner None
    result = normalize_datetime(None)
    assert result is None

def test_normalize_datetime_empty_string():
    # Chaîne vide → doit retourner None
    result = normalize_datetime("")
    assert result is None

def test_normalize_datetime_different_format():
    # Format différent ex: "2014/06/12 17:00"
    date_str = "2014/06/12 17:00"
    result = normalize_datetime(date_str)
    assert result == "20140612170000"

def test_normalize_datetime_dayfirst_false():
    # Format américain avec month/day/year, dayfirst=True doit être géré correctement
    date_str = "06/12/2014 17:00"
    result = normalize_datetime(date_str)
    assert result == "20140612170000"