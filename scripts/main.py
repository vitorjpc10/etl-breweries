import logging
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq

from data_extraction import Extract
from data_transformation import Transform
from data_loading import Loading


def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    extracted_path = os.path.join(base_path, 'data', 'extracted')
    transformed_path = os.path.join(base_path, 'data', 'transformed')

    # Initialize Extractor
    extract = Extract()

    logging.info("Extracting breweries data...")
    brewery_data = extract.get_breweries()
    logging.info("Breweries data extracted successfully.")

    save_to_json_file(brewery_data, os.path.join(extracted_path, 'extracted_weather_data.json'))

    # Initialize Transformer
    transform = Transform()

    logging.info("Transforming breweries data...")
    brewery_data_formatted = transform.format_brewery_data(brewery_data)
    logging.info("Breweries data transformed successfully.")

    print(brewery_data_formatted)

    save_to_json_file(brewery_data_formatted, os.path.join(transformed_path, 'raw', 'json', 'formatted_breweries_data.json'))
    save_to_parquet_folder(brewery_data_formatted, os.path.join(transformed_path, 'raw', 'parquet'), ['state', 'city'])

    brewery_type_aggregated_view = transform.create_aggregated_view_brewery_type(brewery_data, ['city', 'state'])

    print(brewery_type_aggregated_view)

    save_to_json_file(brewery_type_aggregated_view, os.path.join(transformed_path, 'aggregated_data', 'json', 'brewery_type_aggregated_view.json'))
    save_to_parquet_folder(brewery_type_aggregated_view, os.path.join(transformed_path, 'aggregated_data', 'parquet'))


    # Initialize Loader
    load = Loading()

    #! Adjust this method for this project if needed to use...
    # load.create_table_if_not_exists("traffic")
    # load.create_table_if_not_exists("weather")

    # Load weather data
    logging.info("Loading breweries data...")
    load.load_data(brewery_data_formatted, "breweries")
    logging.info("Breweries data loaded successfully.")

    load.close_spark()

    logging.info("ETL PROCESSED SUCCESSFULLY")

def save_to_json_file(data_dict, file_path):
    """
    Write a dictionary to a file in JSON format. Creates the directory if it is missing.

    :param data_dict: Dictionary to write to file
    :param file_path: Path to the file where the dictionary should be written
    """
    try:
        # Extract directory path
        directory = os.path.dirname(file_path)

        # Create directory if it doesn't exist
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        # Write dictionary to JSON file
        with open(file_path, 'w') as file:
            json.dump(data_dict, file, indent=4)

        logging.info(f"Dictionary written to {file_path} successfully.")
    except Exception as e:
        raise Exception(f"Failed to write dictionary to {file_path}: {e}")

def save_to_parquet_folder(data, output_path, partition_cols=None):
    """
    Saves the data to Parquet files.

    :param data: List of dictionaries to save
    :param output_path: Directory to save Parquet files
    :param partition_cols: Optional string list of column keys to partition by
    """
    # Ensure the output directory exists
    os.makedirs(output_path, exist_ok=True)

    # Convert list of dictionaries to dictionary of lists
    columns = data[0].keys()  # Assuming all dictionaries have the same keys
    column_values = {}
    for col in columns:
        column_values[col] = [d[col] for d in data]

    # Convert dictionary data to Arrow Table
    table = pa.Table.from_pydict(column_values)

    # Save to Parquet files
    try:
        if partition_cols:
            pq.write_to_dataset(table, root_path=output_path, partition_cols=partition_cols)
        else:
            pq.write_table(table, os.path.join(output_path, 'data.parquet'))
        logging.info(f"Data written to '{output_path}' successfully.")

    except Exception as e:
        error_msg = f"Failed to write data to '{output_path}': {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)


if __name__ == "__main__":
    main()
