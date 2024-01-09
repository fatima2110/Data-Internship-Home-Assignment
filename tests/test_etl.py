import pytest
from unittest.mock import patch
from airflow.models import DagBag, TaskInstance
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pandas as pd

import sqlite3

from dags.create_table import create_tables, TABLES_CREATION_QUERY

# fuction runs before the tests
@pytest.fixture
def df():
    source_file_path = 'source/jobs.csv'
    df = pd.read_csv(source_file_path)
    return df

# check if column exists
def test_col_exists(df):
    name = "context"
    assert name in df.columns

