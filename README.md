# LinkDB

LinkDB is a Flask-based API that interfaces with ScyllaDB to allow users to dynamically create keyspaces, tables, and manage data.

## Features

- Create keyspaces with specified replication factors.
- List tables in a given keyspace.
- Insert data into a specified table.
- Query data from a specific table.
- Basic API key authentication for enhanced security.

## Prerequisites

- Python 3.x
- ScyllaDB
- Flask and Flask-RESTful
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

- **POST** `/create_keyspace`: Create a new keyspace.
- **POST** `/create_table/<keyspace_name>`: Create a new table within a keyspace.
- **GET** `/list_tables/<keyspace_name>`: List all tables in a keyspace.
- **POST** `/insert_data/<keyspace_name>/<table_name>`: Insert data into a table.
- **GET** `/query_data/<keyspace_name>/<table_name>`: Fetch all rows from a table.
