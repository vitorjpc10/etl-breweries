import logging
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq


class FileIO:

    def save_to_json_file(self, data_dict, file_path):
        """
        Write a dictionary to a file in JSON format. Creates the directory if it is missing.

        Args:
        data_dict (dict): Dictionary to write to file.
        file_path (str): Path to the file where the dictionary should be written.

        Raises:
        Exception: If writing to file fails.
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


    def save_to_parquet_folder(self, data, output_path, partition_cols=None):
        """
        Saves the data to Parquet files.

        Args:
        data (list of dict): List of dictionaries to save.
        output_path (str): Directory to save Parquet files.
        partition_cols (list of str, optional): String list of column keys to partition by.

        Raises:
        Exception: If writing to Parquet files fails.
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
