# LinkDB

LinkDB is a Flask-based API that interfaces with ScyllaDB, allowing users to dynamically create keyspaces, tables, and manage data.

## Features

- Generate an API key for authentication.
- Create keyspaces automatically during API key generation with specified replication factors.
- List tables in a given keyspace.
- Insert, update, query, and delete data from a specific table.
- Basic API key authentication for enhanced security.

## Prerequisites

- Python 3.x
- ScyllaDB
- Flask, Flask-RESTful, Flask-Limiter
- Cassandra driver for Python

## Setup

1. Clone the repository:

    ```bash
    git clone <repository_url>
    cd LinkDB
    ```

2. Install the required packages:

    ```bash
    pip install -r requirements.txt
    ```

3. Set up ScyllaDB using Docker or any preferred method.

4. Update the ScyllaDB connection details in `app.py` if necessary.

5. Run the Flask API:

    ```bash
    python app.py
    ```

## Testing

To test the API endpoints, you can use the provided test script:

    ```bash
    python test_api.py
    ```

## API Endpoints

- **GET** `/`: Welcome message.
- **POST** `/generate_api_key`: Generate a new API key for authentication.
- **POST** `/create_table/<keyspace_name>`: Create a new table within a keyspace. The body should contain 'table_name' and 'columns' (a dictionary of column names and their types).
- **GET** `/list_tables/<keyspace_name>`: List all tables in a keyspace.
- **POST** `/insert_data/<keyspace_name>/<table_name>`: Insert data into a table.
- **PUT** `/update_data/<keyspace_name>/<table_name>`: Update data in a table. The body should contain the data to update and the 'id' of the row to be updated.
- **GET** `/query_data/<keyspace_name>/<table_name>`: Fetch data from a table. The results can be limited using the 'limit' query parameter.
- **DELETE** `/delete_data/<keyspace_name>/<table_name>`: Delete data from a table based on an ID provided in the request body.