import pandas as pd
import pytest

from etl.transform import trf_file_wcup_2014

# test def trf_file_wcup_2014
@pytest.fixture
def sample_config():
    return {
        'colonnes_retenues': ['date', 'home_team', 'away_team', 'home_result', 'away_result', 'stage', 'edition', 'city'],
        'news_columns': {
            'home_team': 'home_team',
            'away_team': 'away_team',
            'home_result': 'home_result',
            'away_result': 'away_result',
            'stage': 'stage',
            'edition': 'edition',
            'city': 'city',
            'date': 'date'
        },
        'stage_mapping': {
            'Group': 'Group Stage',
            'Final': 'Final'
        },
        'correction_team_mapping': {
            'Brasil': 'Brazil',
            'Deutschland': 'Germany'
        }
    }

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'date': ['2014-06-12', '2014-07-13'],
        'home_team': ['Brasil', 'Deutschland'],
        'away_team': ['Croatia', 'Argentina'],
        'home_result': [3, 1],
        'away_result': [1, 0],
        'stage': ['Group', 'Final'],
        'edition': [2014, 2014],
        'city': ['Sao Paulo', 'Rio de Janeiro']
    })

def test_trf_file_wcup_2014(sample_df, sample_config):
    # Appelle fonction
    result = trf_file_wcup_2014(sample_df, sample_config)
    
    # Verifier des colonnes
    expected_columns = ['match_id', 'date', 'home_team', 'away_team', 'home_result', 'away_result', 'stage', 'edition', 'city']
    assert list(result.columns) == expected_columns

    # Verifier length
    assert len(result) == 2

    # Verifier normalisations
    assert result.iloc[0]['home_team'] == 'Brazil'
    assert result.iloc[1]['home_team'] == 'Germany'
    assert result.iloc[0]['stage'] == 'Group Stage'
    assert result.iloc[1]['stage'] == 'Final'
    assert result.iloc[0]['city'] == 'Sao Paulo'
    assert result.iloc[1]['city'] == 'Rio de Janeiro'

    # Verifier que 'match_id' est unique
    assert result['match_id'].is_unique
    assert not result['match_id'].isnull().any()

    # Verifier organisation par date
    assert result.iloc[0]['date'] < result.iloc[1]['date']

