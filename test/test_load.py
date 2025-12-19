# -*- coding: utf-8 -*-
"""
Tests for create_postgres_engine function.
"""

from unittest.mock import patch, MagicMock
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.load import create_postgres_engine, execute_query, execute_select


# ----------------------------
# SUCCESS CASE
# ----------------------------

@patch("etl.load.create_engine")
def test_create_postgres_engine_success(mock_create_engine):
    """Test engine creation success."""
    mock_engine = MagicMock(spec=Engine)
    mock_create_engine.return_value = mock_engine

    engine = create_postgres_engine(
        host="localhost",
        database="test_db",
        user="test_user",
        password="test_pwd",
        port=5432
    )

    # Returned object
    assert engine is mock_engine

    # create_engine called once
    mock_create_engine.assert_called_once()

    # Verify connection URL
    called_url = mock_create_engine.call_args[0][0]
    assert "postgresql+psycopg2://" in called_url
    assert "test_user:test_pwd@localhost:5432/test_db" in called_url


# ----------------------------
# FAILURE CASE
# ----------------------------

@patch("etl.load.create_engine")
def test_create_postgres_engine_failure(mock_create_engine, capsys):
    """Test engine creation failure."""
    mock_create_engine.side_effect = Exception("Connection error")

    engine = create_postgres_engine(
        host="localhost",
        database="test_db",
        user="test_user",
        password="wrong_pwd"
    )

    # Returned object
    assert engine is None

    # Error message printed
    captured = capsys.readouterr()
    assert "Erreur de création du moteur SQLAlchemy" in captured.out


# ----------------------------
# SUCCESS CASE
# ----------------------------

@patch("etl.load.text")
def test_execute_query_success(mock_text):
    mock_engine = MagicMock()
    mock_connection = MagicMock()

    mock_engine.begin.return_value.__enter__.return_value = mock_connection

    execute_query(
        mock_engine,
        "INSERT INTO test VALUES (:id)",
        {"id": 1}
    )

    mock_text.assert_called_once_with("INSERT INTO test VALUES (:id)")
    mock_connection.execute.assert_called_once_with(
        mock_text.return_value,
        {"id": 1}
    )


# ----------------------------
# FAILURE CASE
# ----------------------------

@patch("etl.load.text")
def test_execute_query_failure(mock_text, capsys):
    """Test SQL execution failure handling."""

    mock_engine = MagicMock(spec=Engine)
    mock_engine.begin.side_effect = Exception("SQL failure")

    execute_query(mock_engine, "DELETE FROM table")

    # Capture printed error
    captured = capsys.readouterr()
    assert "Erreur SQL" in captured.out
   

@patch("etl.load.text")
def test_execute_select_success(mock_text):
    # --------------------
    # Arrange (préparation)
    # --------------------
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()

    # engine.connect() comme context manager
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    # Simuler une ligne SQLAlchemy
    mock_row_1 = MagicMock()
    mock_row_1._mapping = {"id": 1, "name": "France"}

    mock_row_2 = MagicMock()
    mock_row_2._mapping = {"id": 2, "name": "Brazil"}

    # Le résultat doit être itérable
    mock_result.__iter__.return_value = [mock_row_1, mock_row_2]

    # conn.execute retourne le résultat mocké
    mock_conn.execute.return_value = mock_result

    # --------------------
    # Act (appel)
    # --------------------
    result = execute_select(
        engine=mock_engine,
        query="SELECT * FROM teams WHERE id > :id",
        params={"id": 0}
    )

    # --------------------
    # Assert (vérifications)
    # --------------------
    mock_engine.connect.assert_called_once()

    mock_text.assert_called_once_with(
        "SELECT * FROM teams WHERE id > :id"
    )

    mock_conn.execute.assert_called_once_with(
        mock_text.return_value,
        {"id": 0}
    )

    assert result == [
        {"id": 1, "name": "France"},
        {"id": 2, "name": "Brazil"},
    ]
    
    
@patch("etl.load.text")
def test_execute_select_without_params(mock_text):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    mock_row = MagicMock()
    mock_row._mapping = {"count": 64}

    mock_conn.execute.return_value = [mock_row]

    result = execute_select(
        engine=mock_engine,
        query="SELECT COUNT(*) AS count FROM matches",
        params=None
    )

    mock_conn.execute.assert_called_once_with(
        mock_text.return_value,
        {}
    )

    assert result == [{"count": 64}]