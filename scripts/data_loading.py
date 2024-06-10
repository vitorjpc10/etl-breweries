from pyspark.sql import SparkSession
import os
import psycopg2


class Loading:

    def __init__(self):
        """
        Initializes Loading object and sets up PostgreSQL connection and SparkSession.
        """

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
        """
       Creates a table in PostgreSQL if it doesn't exist.

       Args:
       table_name (str): The name of the table to create.

       Raises:
       ValueError: If the table name is not recognized.
       """

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
            "breweries": """
                CREATE TABLE IF NOT EXISTS breweries (
                    id SERIAL PRIMARY KEY,
                    address_1 TEXT,
                    address_2 TEXT,
                    address_3 TEXT,
                    brewery_type TEXT,
                    city TEXT,
                    country TEXT,
                    brewery_id TEXT,
                    latitude FLOAT,
                    longitude FLOAT,
                    name TEXT,
                    phone TEXT,
                    postal_code TEXT,
                    state TEXT,
                    state_province TEXT,
                    street TEXT,
                    website_url TEXT
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

        cursor.close()
        connection.close()

    def load_data(self, data: dict, table_name: str):
        """
        Loads data into PostgreSQL.

        Args:
        data (dict): The data to load into the table.
        table_name (str): The name of the table to load data into.
        """

        # Create DataFrame from the provided data
        df = self.spark.createDataFrame(data)

        # Define PostgreSQL connection properties
        properties = {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver"
        }

        # Write DataFrame to PostgreSQL
        df.write.jdbc(url=self.postgres_url, table=table_name, mode="append", properties=properties)

    def close_spark(self):
        """
        Stops the SparkSession.
        """

        self.spark.stop()
