
from pyspark.sql import SparkSession
import os
import psycopg2
from pyspark.sql.functions import monotonically_increasing_id


class Loading:

    def __init__(self):
        # Load environment variables
        self.postgres_host = os.getenv('POSTGRES_HOST')
        self.postgres_port = os.getenv('POSTGRES_PORT')
        self.postgres_db = os.getenv('POSTGRES_DB')
        self.postgres_user = os.getenv('POSTGRES_USER')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD')

        # Check if all necessary environment variables are set
        if not self.postgres_host:
            raise EnvironmentError("POSTGRES_HOST environment variable is not set.")
        if not self.postgres_port:
            raise EnvironmentError("POSTGRES_PORT environment variable is not set.")
        if not self.postgres_db:
            raise EnvironmentError("POSTGRES_DB environment variable is not set.")
        if not self.postgres_user:
            raise EnvironmentError("POSTGRES_USER environment variable is not set.")
        if not self.postgres_password:
            raise EnvironmentError("POSTGRES_PASSWORD environment variable is not set.")

        # Construct the PostgreSQL URL
        self.postgres_url = f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

        # Initialize SparkSession
        self.spark = SparkSession.builder \
            .appName("PostgreSQL Connection") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
            .getOrCreate()

    def create_table_if_not_exists(self, table_name: str):
        # Connect to PostgreSQL to create the table if it doesn't exist
        connection = psycopg2.connect(
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password
        )
        cursor = connection.cursor()

        # Define SQL to create tables if they don't exist
        create_table_sql = {
            "weather": """
                CREATE TABLE IF NOT EXISTS weather (
                    id SERIAL PRIMARY KEY,
                    country TEXT,
                    description TEXT,
                    feels_like FLOAT,
                    humidity FLOAT,
                    latitude FLOAT,
                    longitude FLOAT,
                    name TEXT,
                    pressure FLOAT,
                    sunrise TEXT,
                    sunset TEXT,
                    temp_max FLOAT,
                    temp_min FLOAT,
                    temperature FLOAT,
                    timestamp TEXT,
                    visibility INT,
                    wind_degree_direction INT,
                    wind_speed FLOAT
                );
            """,
            "traffic": """
                CREATE TABLE IF NOT EXISTS traffic (
                    id SERIAL PRIMARY KEY,
                    destination_latitude FLOAT,
                    destination_longitude FLOAT,
                    destination_name TEXT,
                    distance FLOAT,
                    duration INT,
                    geometry TEXT,
                    origin_latitude FLOAT,
                    origin_longitude FLOAT,
                    origin_name TEXT,
                    route_coordinates TEXT,
                    start_time TEXT
                );
            """
        }

        # Execute SQL to create the table
        if table_name in create_table_sql:
            cursor.execute(create_table_sql[table_name])
            connection.commit()
        else:
            raise ValueError(f"Unknown table name: {table_name}. Table create query undefined. "
                             f" Defined table queries are: {create_table_sql.keys()}")

        # # Close the cursor and connection
        cursor.close()
        connection.close()

    def load_data(self, data: dict, table_name: str):

        # Create DataFrame from the provided data
        df = self.spark.createDataFrame(data)

        # Add a primary key column
        df = df.withColumn("id_primary", monotonically_increasing_id())

        # Define PostgreSQL connection properties
        properties = {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver"
        }

        # Write DataFrame to PostgreSQL
        df.write.jdbc(url=self.postgres_url, table=table_name, mode="append", properties=properties)


    def close_spark(self):
        # Stop SparkSession
        self.spark.stop()