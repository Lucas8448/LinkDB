# LinkDB User Manual

## Introduction

Welcome to LinkDB, a Flask-based API designed to interface seamlessly with ScyllaDB. This manual provides comprehensive guidance on setting up and using LinkDB, ensuring you can leverage its full potential for dynamic database management.

### Version: 1.0

## Features

LinkDB offers a range of functionalities:

- **API Key Generation**: Secure access with generated API keys.
- **Dynamic Keyspace Creation**: Automate keyspace creation with API key generation, including replication factors customization.
- **Table Management**: List, insert, update, query, and delete data in tables within keyspaces.
- **Enhanced Security**: Basic API key authentication for secure data management.

## Prerequisites

Before installing LinkDB, ensure the following prerequisites are met:

- **Python 3.x**: Required for running the Flask application.
- **ScyllaDB**: The NoSQL database used by LinkDB.
- **Flask Packages**: Flask, Flask-RESTful, Flask-Limiter for RESTful API construction.
- **Cassandra Driver**: Python driver for ScyllaDB.

## Installation and Setup

Follow these steps to get LinkDB up and running:

Clone repository:

    git clone <repository_url>
    cd LinkDB

Install Required Packages:

    pip install -r requirements.txt

Set Up ScyllaDB: Use Docker or your preferred method for database setup.

Configure ScyllaDB Connection: Modify connection details in app.py as needed.

Launch Flask API:

    python app.py

Testing

Test the API endpoints using the provided script:

    python test_api.py

API Endpoints

    GET /: Welcome and service status.
    POST /generate_api_key: API key generation.
    POST /create_table/<keyspace_name>: Table creation within a keyspace.
    GET /list_tables/<keyspace_name>: Listing tables in a keyspace.
    POST /insert_data/<keyspace_name>/<table_name>: Data insertion.
    PUT /update_data/<keyspace_name>/<table_name>: Data update.
    GET /query_data/<keyspace_name>/<table_name>: Data querying.
    DELETE /delete_data/<keyspace_name>/<table_name>: Data deletion.

Troubleshooting and Support

Encountering issues? Check these common problems and solutions:

API Key Issues: Ensure keys are correctly generated and stored.
Connection Errors: Verify ScyllaDB connection details in app.py.
Dependency Problems: Re-install packages using requirements.txt.

End of User Manual for LinkDB.
Version: 1.0
