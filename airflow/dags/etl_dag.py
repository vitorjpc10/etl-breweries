import logging
import os
# import pytest
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.data_extraction import Extract
from scripts.data_transformation import Transform
from scripts.data_loading import Loading
from scripts.file_io_utilities import FileIO

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initiating file read/write utility class
file_io = FileIO()
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
extracted_path = os.path.join(base_path, 'data', 'extracted')
transformed_path = os.path.join(base_path, 'data', 'transformed')


def extract_data(ti):
    extract = Extract()

    logging.info("Extracting breweries data...")
    brewery_data = extract.get_breweries()
    logging.info("Breweries data extracted successfully.")

    file_io.save_to_json_file(brewery_data, os.path.join(extracted_path, 'extracted_breweries_data.json'))

    # Running extraction tests
    # pytest.main(["-q", "--disable-warnings", "-s", os.path.join("test_extraction.py")])

    # Push data to XCom
    ti.xcom_push(key='breweries_data', value=brewery_data)


def transform_data(ti):
    # Pull data from XCom
    breweries_data = ti.xcom_pull(key='breweries_data', task_ids='extract_data')

    transform = Transform()

    logging.info("Transforming breweries data...")
    brewery_data_formatted = transform.format_brewery_data(breweries_data)
    logging.info("Breweries data transformed successfully.")

    file_io.save_to_json_file(brewery_data_formatted, os.path.join(transformed_path, 'raw', 'json', 'formatted_breweries_data.json'))
    file_io.save_to_parquet_folder(brewery_data_formatted, os.path.join(transformed_path, 'raw', 'parquet'), ['state', 'city'])

    # Running transformation tests
    # pytest.main(["-q", "--disable-warnings", "-s", os.path.join('tests', "test_transformation.py")])

    brewery_type_aggregated_view = transform.create_aggregated_view_brewery_type(breweries_data, ['city', 'state'])

    file_io.save_to_json_file(brewery_type_aggregated_view, os.path.join(transformed_path, 'aggregated_data', 'json', 'brewery_type_aggregated_view.json'))
    file_io.save_to_parquet_folder(brewery_type_aggregated_view, os.path.join(transformed_path, 'aggregated_data', 'parquet'))

    # Push transformed data to XCom
    ti.xcom_push(key='breweries_data_formatted', value=brewery_data_formatted)


def load_data(ti):
    # Pull transformed data from XCom
    breweries_data_formatted = ti.xcom_pull(key='breweries_data_formatted', task_ids='transform_data')

    load = Loading()

    # Load breweries data
    logging.info("Loading breweries data...")
    load.load_data(breweries_data_formatted, "breweries")
    logging.info("Breweries data loaded successfully.")

    load.close_spark()

    logging.info("ETL PROCESSED SUCCESSFULLY")


# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
        default_args=default_args,
        dag_id='breweries_etl_dag',
        description='A DAG for extracting, transforming, and loading  brewery data',
        schedule_interval='@daily'
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task
