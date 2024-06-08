import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_extraction import Extract
from data_transformation import Transform
from data_loading import Loading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data(ti):
    # Initialize Extractor
    extract = Extract()

    # Extract weather data
    logging.info("Extracting weather data...")
    weather_data = extract.get_weather(longitude=-74.0060, latitude=40.7128, exclude='hourly,daily', units='metric', lang='en')
    logging.info("Weather data extracted successfully.")

    # Extract traffic data
    logging.info("Extracting traffic data...")
    traffic_data = extract.get_traffic(start_coords=(-74.0060, 40.7128), end_coords=(-122.4194, 37.7749))
    logging.info("Traffic data extracted successfully.")

    # Push data to XCom
    ti.xcom_push(key='weather_data', value=weather_data)
    ti.xcom_push(key='traffic_data', value=traffic_data)


def transform_data(ti):
    # Initialize Transformer
    transform = Transform()

    # Pull data from XCom
    weather_data = ti.xcom_pull(key='weather_data', task_ids='extract_data')
    traffic_data = ti.xcom_pull(key='traffic_data', task_ids='extract_data')

    # Transform weather data
    logging.info("Transforming weather data...")
    weather_data_formatted = transform.clean_weather_data(weather_data)
    logging.info("Weather data transformed successfully.")

    # Transform traffic data
    logging.info("Transforming traffic data...")
    traffic_data_formatted = transform.clean_traffic_data(traffic_data)
    logging.info("Traffic data transformed successfully.")

    # Push transformed data to XCom
    ti.xcom_push(key='weather_data_formatted', value=weather_data_formatted)
    ti.xcom_push(key='traffic_data_formatted', value=traffic_data_formatted)


def load_data(ti):
    # Initialize Loader
    load = Loading()

    load.create_table_if_not_exists("traffic")
    load.create_table_if_not_exists("weather")

    # Pull transformed data from XCom
    weather_data_formatted = ti.xcom_pull(key='weather_data_formatted', task_ids='transform_data')
    traffic_data_formatted = ti.xcom_pull(key='traffic_data_formatted', task_ids='transform_data')

    # Load weather data
    logging.info("Loading weather data...")
    load.load_data(weather_data_formatted, "weather")
    logging.info("Weather data loaded successfully.")

    # Load traffic data
    logging.info("Loading traffic data...")
    load.load_data(traffic_data_formatted, "traffic")
    logging.info("Traffic data loaded successfully.")

    load.close_spark()

    logging.info("ETL PROCESSED SUCCESSFULLY")


# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        default_args=default_args,
        dag_id='data_etl_dag',
        description='A DAG for extracting, transforming, and loading  traffic and weather data',
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
