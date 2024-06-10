import logging
import os

from tests.extraction_data_tests import ExtractionDataTests
from data_extraction import Extract
from data_transformation import Transform
from data_loading import Loading
from file_io_utilities import FileIO
from tests.transformation_data_tests import TransformationDataTests


def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Initiating file read/write utility class
    file_io = FileIO()

    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    extracted_path = os.path.join(base_path, 'data', 'extracted')
    transformed_path = os.path.join(base_path, 'data', 'transformed')

    #! Initialize Extractor
    extract = Extract()

    logging.info("Extracting breweries data...")
    brewery_data = extract.get_breweries()
    logging.info("Breweries data extracted successfully.")

    file_io.save_to_json_file(brewery_data, os.path.join(extracted_path, 'extracted_breweries_data.json'))

    transformation_test = ExtractionDataTests(brewery_data)
    transformation_test.run_tests()

    # Running extraction tests
    # pytest.main(["-q", "--disable-warnings", "-s", os.path.join(base_path, 'tests', "test_extraction.py")])

    #! Initialize Transformer
    transform = Transform()

    logging.info("Transforming breweries data...")
    brewery_data_formatted = transform.format_brewery_data(brewery_data)
    logging.info("Breweries data transformed successfully.")

    file_io.save_to_json_file(brewery_data_formatted, os.path.join(transformed_path, 'raw', 'json', 'formatted_breweries_data.json'))
    file_io.save_to_parquet_folder(brewery_data_formatted, os.path.join(transformed_path, 'raw', 'parquet'), ['state', 'city'])

    # Running transformation tests
    transformation_test = TransformationDataTests(brewery_data)
    transformation_test.run_tests()

    brewery_type_aggregated_view = transform.create_aggregated_view_brewery_type(brewery_data, ['city', 'state'])

    file_io.save_to_json_file(brewery_type_aggregated_view, os.path.join(transformed_path, 'aggregated_data', 'json', 'brewery_type_aggregated_view.json'))
    file_io.save_to_parquet_folder(brewery_type_aggregated_view, os.path.join(transformed_path, 'aggregated_data', 'parquet'))


    #! Initialize Loader
    load = Loading()

    # Load breweries data
    logging.info("Loading breweries data...")
    load.load_data(brewery_data_formatted, "breweries")
    logging.info("Breweries data loaded successfully.")

    load.close_spark()

    logging.info("ETL PROCESSED SUCCESSFULLY")


if __name__ == "__main__":
    main()
