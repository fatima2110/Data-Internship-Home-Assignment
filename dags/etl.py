from datetime import timedelta, datetime

from airflow.decorators import dag
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

import pandas as pd
import os

from load import load
from create_table import create_tables
from extract import extract
from transform import transform

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 7),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

def etl_dag():
    """ETL pipeline"""
    # Définir les dépendances entre les tâches
    create_tables_task = create_tables()
    extract_task = extract()
    transform_task = transform()
    load_task = load()

    # Spécifier les dépendances
    create_tables_task >> extract_task
    extract_task >> transform_task
    transform_task >> load_task


etl_dag()