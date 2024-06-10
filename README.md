# ETL Data Pipeline for Weather and Traffic Data

TODO: move file write ultility functions to its own class from main
adjust create table function in loading script .py for this project
implement pytest classes and see how to call from main
create dag for project
bonus: visualizer of data SPIKE/ upload to data folder to s3


### Readme em português esta aqui: [README Português](README-PT.md)

## Description
This project extracts weather data from the OpenWeatherMap API and traffic data from the OSRM API, transforms and cleans the data, and loads it into a PostgreSQL database. Queries are then executed to generate reports based on the extracted data.

## Setup

### Prerequisites
- Git
- Docker
- Docker Compose

### Environment Variables
Set the environment variables for API keys in the `docker-compose` files. Obtain your API keys from:
- [OpenWeatherMap API](https://home.openweathermap.org/api_keys)
- [OSRM API](https://project-osrm.org/)

### Steps to Run

1. Set environment variables for api keys in docker-compose files (https://home.openweathermap.org/api_keys)

2. Clone the repository:
    ```bash
    git clone https://github.com/vitorjpc10/etl-weather_traffic_data.git
    ```
3. Move to the newly cloned repository:
    ```bash
    cd etl-weather_traffic_data
    ```

### ETL without Orchestrator (Python Docker)

4. Build and run the Docker containers:
    ```bash
    docker-compose up --build
    ```

5. The data will be extracted, transformed, and loaded into the PostgreSQL database based on the logic in `scripts/main.py`.

6. Once built, run the following command to execute queries on both weather and traffic tables from PostgreSQL database container:
    ```bash
    docker exec -it etl-breweries-db-1 psql -U postgres -c "\i queries/queries.sql"
    ```

   Do `\q` in terminal to quit query, there are 2 queries in total.

7. To generate the pivoted report, access the PostgreSQL database and execute the `query.sql` SQL file:
    ```bash
    docker exec -it etl-gdp-of-south-american-countries-using-the-world-bank-api-db-1 psql -U postgres -c "\i query.sql"
    ```

### ETL with Orchestrator (Apache Airflow)

4. Move to the Airflow directory:
    ```bash
    cd airflow
    ```

5. Build and run the Docker containers:
    ```bash
    docker-compose up aiflow-init --build
    ```
   ```bash
    docker-compose up
    ```

6. Once all containers are built access local (http://localhost:8080/) and trigger etl_dag DAG (username and password are admin by default)

7. Once DAG compiles successfully, run the following command to execute queries on both weather and traffic tables:
    ```bash
    docker exec -it airflow-postgres-1 psql -U airflow -c "\i queries/queries.sql"
    ```
   Do `\q` in terminal to quit query, there are 2 queries in total.


## Assumptions and Design Decisions
- The project uses Docker and Docker Compose for containerization and orchestration to ensure consistent development and deployment environments.
- Docker volumes are utilized to persist PostgreSQL data, ensuring that the data remains intact even if the containers are stopped or removed.
- The PostgreSQL database is selected for data storage due to its reliability, scalability, and support for SQL queries.
- Pure Python, SQL, and PySpark are used for data manipulation to ensure lightweight and efficient data processing.
- The SQL queries for generating reports are stored in separate files (e.g., `queries.sql`). This allows for easy modification of the queries and provides a convenient way to preview the results.
- To generate the reports, the SQL queries are executed within the PostgreSQL database container. This approach simplifies the process and ensures that the queries can be easily run and modified as needed.

## Airflow Sample DAG
![img.png](img.png)
