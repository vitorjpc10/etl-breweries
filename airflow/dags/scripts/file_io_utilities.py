import logging
import json
import os

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs


class FileIO:
    def __init__(self, s3_bucket_name=None):
        """
        Initialize the FileIO class.

        Args:
        s3_bucket_name (str, optional): Name of the S3 bucket. Default is None.
        """
        # Fetch AWS credentials from environment variables
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        # Initialize the S3 client with the provided credentials
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        # Check if the credentials are defined
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            logging.warning("AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY environment variables are not defined."
                            " Will be unable to write files to S3 buckets")

        self.s3_bucket_name = s3_bucket_name

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
        column_values = {col: [d[col] for d in data] for col in columns}

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

    def save_to_json_file_s3(self, data, s3_path):
        """
        Write a dictionary to a JSON file on S3.

        Args:
        data (dict): Dictionary to write to file.
        s3_path (str): S3 path where the file should be written.

        Raises:
        Exception: If writing to S3 fails.
        """
        # Check if AWS credentials are defined
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            logging.warning("Unable to write to S3 because AWS_ACCESS_KEY_ID or "
                            "AWS_SECRET_ACCESS_KEY environment variables are not defined.")
            return

        try:
            # Parse the S3 path into bucket and key
            s3_bucket, s3_key = self._parse_s3_path(s3_path)
            # Write the JSON data to the specified S3 path
            self.s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=json.dumps(data))
            logging.info(f"Dictionary written to S3 path {s3_path} successfully.")
        except Exception as e:
            raise Exception(f"Failed to write dictionary to S3 path {s3_path}: {e}")

    def save_to_parquet_folder_s3(self, data, s3_folder_path, partition_cols=None):
        """
        Save data to Parquet files on S3.

        Args:
        data (list of dict): List of dictionaries to save.
        s3_folder_path (str): S3 folder path where Parquet files should be written.
        partition_cols (list of str, optional): List of column keys to partition by.

        Raises:
        Exception: If writing to S3 fails.
        """
        # Check if AWS credentials are defined
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            logging.warning("Unable to write to S3 because AWS_ACCESS_KEY_ID or "
                            "AWS_SECRET_ACCESS_KEY environment variables are not defined.")
            return

        try:
            # Convert the data to a Pandas DataFrame
            df = pd.DataFrame(data)
            # Convert the DataFrame to an Arrow Table
            table = pa.Table.from_pandas(df)
            # Parse the S3 path into bucket and key prefix
            s3_bucket, s3_key_prefix = self._parse_s3_path(s3_folder_path)
            # Construct the S3 destination path
            s3_dest = f's3://{s3_bucket}/{s3_key_prefix}'
            # Write the data to the specified S3 path in Parquet format
            pq.write_to_dataset(table, root_path=s3_dest, filesystem=s3fs.S3FileSystem(), partition_cols=partition_cols)
            logging.info(f"Data written to S3 path '{s3_folder_path}' successfully.")
        except Exception as e:
            raise Exception(f"Failed to write data to S3 path '{s3_folder_path}': {e}")

    def _parse_s3_path(self, s3_path):
        """
        Helper method to parse the S3 path into bucket and key.

        Args:
        s3_path (str): The full S3 path.

        Returns:
        tuple: The S3 bucket and key.
        """
        s3_path = s3_path.replace("s3://", "")
        s3_bucket, s3_key = s3_path.split('/', 1)
        return s3_bucket, s3_key
