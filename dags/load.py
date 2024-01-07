from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

import json
import os

from datetime import timedelta, datetime

from airflow.decorators import dag
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

import pandas as pd
import os


@task()
def load():
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    # Path to the folder containing JSON files
    json_folder_path = 'staging/transformed'

    # Loop through JSON files in the folder
    for filename in os.listdir(json_folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(json_folder_path, filename)

            try:
                # Read JSON file
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    json_data = json.load(json_file)

                # Extract relevant data from 'job' section
                job_data = json_data.get('job', {})
                title = job_data.get('title', None)
                industry = job_data.get('industry', None)
                description = job_data.get('description', None)
                employment_type = job_data.get('employment_type', None)
                date_posted = job_data.get('date_posted', None)

                # Insert data into 'job' table
                # Insert data into 'job' table
                # Insert data into 'job' table
                sqlite_hook.insert_rows(
                    table='job',
                    rows=[(title, industry, description, employment_type, date_posted)],
                    target_fields=['title', 'industry', 'description', 'employment_type', 'date_posted']
                )

                # Récupérer l'ID de la dernière ligne insérée en exécutant une requête SELECT
                result = sqlite_hook.get_conn().execute("SELECT last_insert_rowid()").fetchone()
                job_id = result[0] if result else None
                # Extract and insert data into 'company' table
                company_data = json_data.get('company', {})
                name = company_data.get('name', None)
                link = company_data.get('link', None)
                sqlite_hook.insert_rows(
                    table='company',
                    rows=[(job_id, name, link)],
                    target_fields=['job_id', 'name', 'link']
                )

                # Extract and insert data into 'education' table
                education_data = json_data.get('education', {})
                required_credential = education_data.get('required_credential', None)
                sqlite_hook.insert_rows(
                    table='education',
                    rows=[(job_id, required_credential)],
                    target_fields=['job_id', 'required_credential']
                )

                # Extract and insert data into 'experience' table
                experience_data = json_data.get('experience', {})
                months_of_experience = experience_data.get('months_of_experience', None)
                seniority_level = experience_data.get('seniority_level', None)
                sqlite_hook.insert_rows(
                    table='experience',
                    rows=[(job_id, months_of_experience, seniority_level)],
                    target_fields=['job_id', 'months_of_experience', 'seniority_level']
                )

                # Extract and insert data into 'salary' table
                salary_data = json_data.get('salary', {})
                currency = salary_data.get('currency', None)
                min_value = salary_data.get('min_value', None)
                max_value = salary_data.get('max_value', None)
                unit = salary_data.get('unit', None)
                sqlite_hook.insert_rows(
                    table='salary',
                    rows=[(job_id, currency, min_value, max_value, unit)],
                    target_fields=['job_id', 'currency', 'min_value', 'max_value', 'unit']
                )

                # Extract and insert data into 'location' table
                location_data = json_data.get('location', {})
                country = location_data.get('country', None)
                locality = location_data.get('locality', None)
                region = location_data.get('region', None)
                postal_code = location_data.get('postal_code', None)
                street_address = location_data.get('street_address', None)
                latitude = location_data.get('latitude', None)
                longitude = location_data.get('longitude', None)
                sqlite_hook.insert_rows(
                    table='location',
                    rows=[(job_id, country, locality, region, postal_code, street_address, latitude, longitude)],
                    target_fields=['job_id', 'country', 'locality', 'region', 'postal_code', 'street_address', 'latitude', 'longitude']
                )
            except Exception as e:
                print(f"Error processing file {filename}: {str(e)}")
                sqlite_hook.rollback()

            finally:
                sqlite_hook.get_conn().close()
