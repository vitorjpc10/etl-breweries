import logging
import os
from datetime import datetime

# Importing test modules
from tests.extraction_data_tests import ExtractionDataTests
from tests.transformation_data_tests import TransformationDataTests

# Importing ETL components
from data_extraction import Extract
from data_transformation import Transform
from data_loading import Loading
from file_io_utilities import FileIO


def main():
    """
    Main function to orchestrate the ETL process.

    Steps:
    1. Extract data from the Open Brewery DB API.
    2. Transform the extracted data, format it, and create aggregated views.
    3. Load the transformed data into a PostgreSQL database.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Initiating file read/write utility class
    file_io = FileIO()

    # Get the current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d")
    timestamp_partition = f"timestamp={timestamp}"

    # Define paths for extracted and transformed data
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    extracted_path = os.path.join(base_path, 'data', 'extracted')
    transformed_path = os.path.join(base_path, 'data', 'transformed')

    # Initialize Extractor
    extract = Extract()

    # Extract data from the API
    logging.info("Extracting breweries data...")
    brewery_data = extract.get_breweries()
    logging.info("Breweries data extracted successfully.")

    # Save extracted data to a JSON file (locally and AWS S3)
    file_io.save_to_json_file(brewery_data, os.path.join(extracted_path, f'{timestamp_partition}/extracted_breweries_data.json'))
    file_io.save_to_json_file_s3(brewery_data, f"{os.getenv('S3_OUTPUT_PATH')}/extracted/{timestamp_partition}/extracted_breweries_data.json")

    # Run extraction data tests
    extraction_test = ExtractionDataTests(brewery_data)
    extraction_test.run_tests()

    # Initialize Transformer
    transform = Transform()

    # Transform the data
    logging.info("Transforming breweries data...")
    brewery_data_formatted = transform.format_brewery_data(brewery_data)
    logging.info("Breweries data transformed successfully.")

    # Save formatted data to JSON and Parquet files
    file_io.save_to_json_file(brewery_data_formatted, os.path.join(transformed_path, 'raw', 'json', f'{timestamp_partition}/formatted_breweries_data.json'))
    file_io.save_to_json_file_s3(brewery_data_formatted, f"{os.getenv('S3_OUTPUT_PATH')}/transformed/raw/json/{timestamp_partition}/formatted_breweries_data.json")
    file_io.save_to_parquet_folder(brewery_data_formatted, os.path.join(transformed_path, 'raw', 'parquet', timestamp_partition), ['state', 'city'])
    file_io.save_to_parquet_folder_s3(brewery_data_formatted, f"{os.getenv('S3_OUTPUT_PATH')}/transformed/raw/parquet/{timestamp_partition}/", ['state', 'city'])

    # Run transformation tests
    transformation_test = TransformationDataTests(brewery_data_formatted)
    transformation_test.run_tests()

    # Create aggregated view of brewery types
    brewery_type_aggregated_view = transform.create_aggregated_view_brewery_type(brewery_data, ['city', 'state'])

    # Save aggregated view to JSON and Parquet files
    file_io.save_to_json_file(brewery_type_aggregated_view, os.path.join(transformed_path, 'aggregated_data', 'json', timestamp_partition, 'brewery_type_aggregated_view.json'))
    file_io.save_to_json_file_s3(brewery_type_aggregated_view, f"{os.getenv('S3_OUTPUT_PATH')}/transformed/aggregated_data/json/{timestamp_partition}/brewery_type_aggregated_view.json")
    file_io.save_to_parquet_folder(brewery_type_aggregated_view, os.path.join(transformed_path, 'aggregated_data', 'parquet', timestamp_partition))
    file_io.save_to_parquet_folder_s3(brewery_type_aggregated_view, f"{os.getenv('S3_OUTPUT_PATH')}/transformed/aggregated_data/parquet/{timestamp_partition}/")

    # Initialize Loader
    load = Loading()

    # Load breweries data into PostgreSQL database
    logging.info("Loading breweries data...")
    load.load_data(brewery_data_formatted, "breweries")
    logging.info("Breweries data loaded successfully.")

    # Close Spark session
    load.close_spark()

    logging.info("ETL PROCESSED SUCCESSFULLY")


if __name__ == "__main__":
    main()
