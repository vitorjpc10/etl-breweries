import logging
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.data_extraction import Extract
from scripts.data_transformation import Transform
from scripts.data_loading import Loading
from scripts.file_io_utilities import FileIO
from test.extraction_data_tests import ExtractionDataTests
from test.transformation_data_tests import TransformationDataTests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initiating file read/write utility class
file_io = FileIO()
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
extracted_path = os.path.join(base_path, 'data', 'extracted')
transformed_path = os.path.join(base_path, 'data', 'transformed')
timestamp = datetime.now().strftime("%Y-%m-%d")
timestamp_partition = f"timestamp={timestamp}"


def extract_data(ti):
    """
    Extracts data using the Extract class and pushes the extracted data to XCom.

    Args:
        ti (airflow.models.TaskInstance): Task instance object

    Returns:
        None
    """
    extract = Extract()

    logging.info("Extracting breweries data...")
    brewery_data = extract.get_breweries()
    logging.info("Breweries data extracted successfully.")

    # Push data to XCom
    ti.xcom_push(key='breweries_data', value=brewery_data)


def extract_data_test(ti):
    """
    Tests the extracted data.

    Args:
        ti (airflow.models.TaskInstance): Task instance object

    Returns:
        None
    """
    breweries_data = ti.xcom_pull(key='breweries_data', task_ids='extract_data')

    transformation_test = ExtractionDataTests(breweries_data)
    transformation_test.run_tests()

def write_extract_data(ti):
    """
    Writes the extracted data to local and S3 storage.

    Args:
        ti (airflow.models.TaskInstance): Task instance object

    Returns:
        None
    """
    breweries_data = ti.xcom_pull(key='breweries_data', task_ids='extract_data')

    file_io.save_to_json_file(breweries_data, os.path.join(extracted_path, 'extracted_breweries_data.json'))
    file_io.save_to_json_file_s3(breweries_data, f"{os.getenv('S3_OUTPUT_PATH')}/extracted/{timestamp_partition}/extracted_breweries_data.json")

def transform_data(ti):
    """
    Transforms the extracted data and saves the transformed data to local and S3 storage.

    Args:
        ti (airflow.models.TaskInstance): Task instance object

    Returns:
        None
    """
    # Pull data from XCom
    breweries_data = ti.xcom_pull(key='breweries_data', task_ids='extract_data')

    transform = Transform()

    logging.info("Transforming breweries data...")
    brewery_data_formatted = transform.format_brewery_data(breweries_data)
    logging.info("Breweries data transformed successfully.")

    brewery_type_aggregated_view = transform.create_aggregated_view_brewery_type(breweries_data, ['city', 'state'])

    # Push transformed data to XCom
    ti.xcom_push(key='breweries_data_formatted', value=brewery_data_formatted)
    ti.xcom_push(key='brewery_type_aggregated_view', value=brewery_type_aggregated_view)

def transform_data_test(ti):
    """
    Tests the transformed data.

    Args:
        ti (airflow.models.TaskInstance): Task instance object

    Returns:
        None
    """
    # Pull transformed data from XCom
    breweries_data_formatted = ti.xcom_pull(key='breweries_data_formatted', task_ids='transform_data')

    # Running transformation tests
    transformation_test = TransformationDataTests(breweries_data_formatted)
    transformation_test.run_tests()

def write_transform_data(ti):
    """
    Writes the transformed data to local and S3 storage.

    Args:
        ti (airflow.models.TaskInstance): Task instance object

    Returns:
        None
    """
    # Pull transformed data from XCom
    breweries_data_formatted = ti.xcom_pull(key='breweries_data_formatted', task_ids='transform_data')
    brewery_type_aggregated_view = ti.xcom_pull(key='brewery_type_aggregated_view', task_ids='transform_data')

    # Save formatted data to JSON and Parquet files
    file_io.save_to_json_file(breweries_data_formatted, os.path.join(transformed_path, 'raw', 'json', f'{timestamp_partition}/formatted_breweries_data.json'))
    file_io.save_to_json_file_s3(breweries_data_formatted, f"{os.getenv('S3_OUTPUT_PATH')}/transformed/raw/json/{timestamp_partition}/formatted_breweries_data.json")
    file_io.save_to_parquet_folder(breweries_data_formatted, os.path.join(transformed_path, 'raw', 'parquet', timestamp_partition), ['state', 'city'])
    file_io.save_to_parquet_folder_s3(breweries_data_formatted, f"{os.getenv('S3_OUTPUT_PATH')}/transformed/raw/parquet/{timestamp_partition}/", ['state', 'city'])

    # Save aggregated view to JSON and Parquet files
    file_io.save_to_json_file(brewery_type_aggregated_view, os.path.join(transformed_path, 'aggregated_data', 'json', timestamp_partition, 'brewery_type_aggregated_view.json'))
    file_io.save_to_json_file_s3(brewery_type_aggregated_view, f"{os.getenv('S3_OUTPUT_PATH')}/transformed/aggregated_data/json/{timestamp_partition}/brewery_type_aggregated_view.json")
    file_io.save_to_parquet_folder(brewery_type_aggregated_view, os.path.join(transformed_path, 'aggregated_data', 'parquet', timestamp_partition))
    file_io.save_to_parquet_folder_s3(brewery_type_aggregated_view, f"{os.getenv('S3_OUTPUT_PATH')}/transformed/aggregated_data/parquet/{timestamp_partition}/")


def load_data(ti):
    """
    Loads the transformed data into a database.

    Args:
        ti (airflow.models.TaskInstance): Task instance object

    Returns:
        None
    """
    # Pull transformed data from XCom
    breweries_data_formatted = ti.xcom_pull(key='breweries_data_formatted', task_ids='transform_data')

    load = Loading()

    # Load breweries data
    logging.info("Loading breweries data...")
    load.load_data(breweries_data_formatted, "breweries")
    logging.info("Breweries data loaded successfully.")

    load.close_spark()

    logging.info("ETL PROCESSED SUCCESSFUL")



# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 10),
    'email': ['your_email@example.com'],
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

    extract_data_test_task = PythonOperator(
        task_id='extract_data_test_validation',
        python_callable=extract_data_test
    )

    write_extract_data_task = PythonOperator(
        task_id='write_extract_data',
        python_callable=write_extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    transform_data_test_task = PythonOperator(
        task_id='transform_data_test_validation',
        python_callable=transform_data_test
    )

    write_transform_data_task = PythonOperator(
        task_id='write_transform_data',
        python_callable=write_transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract_task >> extract_data_test_task >> write_extract_data_task >> transform_task >> transform_data_test_task >> write_transform_data_task >> load_task
