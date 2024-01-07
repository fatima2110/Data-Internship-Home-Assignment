import json
from unittest.mock import MagicMock, patch
from unittest.mock import patch, mock_open
import pytest

from datetime import timedelta, datetime

from airflow.decorators import dag
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

import pandas as pd
import os

from dags.transform import transform_json
from dags.extract import extract
from dags.load import load

# Replace 'dags' with the actual name of the module where your functions/tasks are defined

@pytest.fixture
def sample_json_data():
    return {
        "job": {
            "title": "Software Engineer (.NET)",
            "industry": "Insurance",
            "description": "description description description",
            "employment_type": "FULL_TIME",
            "date_posted": "2021-07-14T17:04:21.000Z"
        },
        "company": {
            "name": "Travelers",
            "link": "https://www.linkedin.com/company/travelers"
        },
        "education": {
            "required_credential": "bachelor degree"
        },
        "experience": {
            "months_of_experience": 48,
            "seniority_level": "Lead"
        },
        "salary": {
            "currency": "USD",
            "min_value": 69000,
            "max_value": 112000,
            "unit": "YEAR"
        },
        "location": {
            "country": "US",
            "locality": "Hartford",
            "region": "CT",
            "postal_code": "06156",
            "street_address": null,
            "latitude": 41.765774,
            "longitude": -72.673355
        }
        }

@patch('dags.SqliteHook')
def test_transform_json(mock_sqlite_hook, sample_json_data):
    transformed_data = transform_json(sample_json_data)

    assert transformed_data['job']['title'] == sample_json_data['title']
    assert transformed_data['job']['industry'] == sample_json_data['industry']
    # others assert

@patch('dags.os.makedirs')
@patch('dags.pd.read_csv')
def test_extract(mock_read_csv, mock_makedirs):
    # Mock the necessary functions
    mock_read_csv.return_value = pd.DataFrame({'context': ['{"key": "value"}']})
    mock_makedirs.return_value = None

    # Call the extract task
    extract()

    # Assertions
    # Add assertions based on the expected behavior of the extract task

@patch('dags.SqliteHook')
@patch('dags.open', new_callable=mock_open)
def test_load(mock_open, mock_sqlite_hook, sample_json_data):
    # Mock open function to return a mock file
    mock_open.return_value.read.return_value = json.dumps(sample_json_data)

    # Create a mock instance of SqliteHook
    sqlite_hook_instance = MagicMock()
    mock_sqlite_hook.return_value = sqlite_hook_instance

    # Call the load task
    load()

    # Assertions
    sqlite_hook_instance.insert_rows.assert_called_once()
    # ... (add more assertions as needed)
    sqlite_hook_instance.get_conn().close.assert_called_once()

