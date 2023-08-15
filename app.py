# Patch Gevent before importing anything else
from gevent import monkey
monkey.patch_all()

# Imports
import uuid
import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from gevent.pywsgi import WSGIServer
from flask_restful import Api, Resource
from flask import Flask, request

# Configuration
SCYLLA_HOST = 'localhost'
SCYLLA_PORT = 9042
KEYSPACE_FOR_API_KEYS = "api_keys"

# Create Flask app and API
app = Flask(__name__)
api = Api(app)

# Cassandra connection setup
auth_provider = PlainTextAuthProvider(
    username='cassandra', password='cassandra')
cluster = Cluster([SCYLLA_HOST], port=SCYLLA_PORT, auth_provider=auth_provider)
session = cluster.connect()

# Utility functions
def generate_api_key():
    return str(uuid.uuid4())


def get_keyspace_from_api_key(api_key):
    return "ks_" + api_key.replace("-", "_")


def create_keyspace_if_not_exists(keyspace_name):
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} "
        f"WITH replication = {{'class':'SimpleStrategy', 'replication_factor':1}}"
    )


def create_table_if_not_exists(table_query):
    session.execute(table_query)


# Initialize Cassandra tables and keyspaces
create_keyspace_if_not_exists(KEYSPACE_FOR_API_KEYS)
session.set_keyspace(KEYSPACE_FOR_API_KEYS)

CREATE_API_KEYS_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS api_keys (
    api_key TEXT PRIMARY KEY,
    client_keyspace TEXT
)
"""
create_table_if_not_exists(CREATE_API_KEYS_TABLE_QUERY)

USAGE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS api_key_usage (
    api_key TEXT,
    timestamp TIMESTAMP,
    endpoint TEXT,
    PRIMARY KEY (api_key, timestamp)
)
"""
create_table_if_not_exists(USAGE_TABLE_QUERY)

# Middleware and auxiliary functions
@app.before_request
def log_request():
    if '/generate_api_key' in request.path:
        return

    api_key = request.headers.get('API-Key')
    if not api_key:
        return {'status': 'error', 'message': 'API-Key header is missing'}, 401

    timestamp = datetime.datetime.utcnow().strftime(
        '%Y-%m-%d %H:%M:%S.%f')[:-3]
    endpoint = request.endpoint

    insert_usage_query = f"INSERT INTO api_key_usage (api_key, timestamp, endpoint) VALUES ('{api_key}', '{timestamp}', '{endpoint}')"
    session.execute(insert_usage_query)


def authenticate(api_key):
    keyspace_name = get_keyspace_from_api_key(api_key)
    query = f"SELECT api_key, client_keyspace FROM api_keys WHERE api_key = ?"
    prepared_statement = session.prepare(query)
    bound_statement = prepared_statement.bind((api_key,))
    rows = session.execute(bound_statement)
    result = rows.one()
    return result and result.client_keyspace == keyspace_name


def calculate_costs(api_key):
    count_query = f"SELECT COUNT(*) FROM api_key_usage WHERE api_key = '{api_key}'"
    rows = session.execute(count_query)
    count = rows.one()[0]
    return count * 0.001


def validate_create_table_data(data):
    if 'table_name' not in data or 'columns' not in data:
        return False, "Both 'table_name' and 'columns' fields are required."
    if not isinstance(data['columns'], dict):
        return False, "'columns' should be a dictionary with column name and type as key-value pairs."
    return True, ""

# Flask-RESTful Resources
class GenerateAPIKey(Resource):
    def post(self):
        new_key = generate_api_key()
        new_keyspace = get_keyspace_from_api_key(new_key)
        insert_query = f"INSERT INTO {KEYSPACE_FOR_API_KEYS}.api_keys (api_key, client_keyspace) VALUES ('{new_key}', '{new_keyspace}')"
        session.execute(insert_query)
        # Create the keyspace as well
        create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {new_keyspace} 
        WITH replication = {{'class':'SimpleStrategy', 'replication_factor':1}}
        """
        session.execute(create_keyspace_query)
        return {'api_key': new_key}


class GetUsageCosts(Resource):
    def get(self):
        api_key = request.headers.get('API-Key')
        if not api_key:
            return {'status': 'error', 'message': 'API-Key header is missing'}, 401

        if not authenticate(api_key):
            return {'status': 'error', 'message': 'Unauthorized'}, 401

        cost = calculate_costs(api_key)
        return {'status': 'success', 'api_key': api_key, 'cost': cost}


def validate_create_table_data(data):
  if 'table_name' not in data or 'columns' not in data:
    return False, "Both 'table_name' and 'columns' fields are required."

  if not isinstance(data['columns'], dict):
    return False, "'columns' should be a dictionary with column name and type as key-value pairs."

  return True, ""


class Home(Resource):
  def get(self):
    return {'message': 'Welcome to LinkDB.'}


class CreateTable(Resource):
  def post(self):
    api_key = request.headers.get('API-Key')
    if not authenticate(api_key):
        return {'status': 'error', 'message': 'Unauthorized'}, 401
    keyspace_name = get_keyspace_from_api_key(api_key)

    data = request.get_json()
    is_valid, validation_message = validate_create_table_data(data)
    if not is_valid:
      return {'status': 'error', 'message': validation_message}, 400

    table_name = data['table_name']
    columns = ", ".join([f"{k} {v}" for k, v in data['columns'].items()])

    create_table_query = f"CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} ({columns})"
    session.execute(create_table_query)
    return {'status': 'success', 'message': f'Table {table_name} created successfully in keyspace {keyspace_name}.'}


class ListTables(Resource):
  def get(self):
    api_key = request.headers.get('API-Key')
    keyspace_name = get_keyspace_from_api_key(api_key)
    if not authenticate(api_key):
      return {'message': 'Unauthorized'}, 401

    list_tables_query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace_name}'"
    rows = session.execute(list_tables_query)
    return {'tables': [row.table_name for row in rows]}


class InsertData(Resource):
  def post(self, table_name):
    api_key = request.headers.get('API-Key')
    keyspace_name = get_keyspace_from_api_key(api_key)
    if not authenticate(api_key):
      return {'message': 'Unauthorized'}, 401

    data = request.get_json()
    columns = ", ".join(data.keys())

    # Differentiate between string and non-string values
    values = ", ".join([f"'{v}'" if isinstance(
        v, str) else str(v) for v in data.values()])

    insert_data_query = f"INSERT INTO {keyspace_name}.{table_name} ({columns}) VALUES ({values})"
    session.execute(insert_data_query)
    return {'message': 'Data inserted successfully.'}


class QueryData(Resource):
  def get(self, table_name):
    api_key = request.headers.get('API-Key')
    keyspace_name = get_keyspace_from_api_key(api_key)
    if not authenticate(api_key):
      return {'status': 'error', 'message': 'Unauthorized'}, 401

    limit = int(request.args.get('limit', 50))

    select_data_query = f"SELECT * FROM {keyspace_name}.{table_name} LIMIT {limit}"
    rows = session.execute(select_data_query)

    # Convert rows to dictionaries using _asdict()
    return {'status': 'success', 'data': [row._asdict() for row in rows]}


class DeleteData(Resource):
    def delete(self, table_name):
        api_key = request.headers.get('API-Key')
        keyspace_name = get_keyspace_from_api_key(api_key)
        if not authenticate(api_key):
            return {'message': 'Unauthorized'}, 401

        data = request.get_json()
        if "id" not in data:
            return {'message': 'ID is required for deletion.'}, 400

        delete_data_query = f"DELETE FROM {keyspace_name}.{table_name} WHERE id={data['id']}"
        session.execute(delete_data_query)
        return {'message': 'Data deleted successfully.'}


class UpdateData(Resource):
    def put(self, table_name):
        api_key = request.headers.get('API-Key')
        keyspace_name = get_keyspace_from_api_key(api_key)
        if not authenticate(api_key):
            return {'message': 'Unauthorized'}, 401

        data = request.get_json()
        if "id" not in data:
            return {'message': 'ID is required for update.'}, 400

        set_clause = ", ".join([f"{k}='{v}'" if isinstance(
            v, str) else f"{k}={v}" for k, v in data.items() if k != "id"])

        update_data_query = f"UPDATE {keyspace_name}.{table_name} SET {set_clause} WHERE id={data['id']}"
        session.execute(update_data_query)
        return {'message': 'Data updated successfully.'}

# Routes
api.add_resource(Home, '/')
api.add_resource(GenerateAPIKey, '/generate_api_key')
api.add_resource(GetUsageCosts, '/get_usage_costs')
api.add_resource(CreateTable, '/create_table')
api.add_resource(ListTables, '/list_tables')
api.add_resource(InsertData, '/insert_data/<string:table_name>')
api.add_resource(QueryData, '/query_data/<string:table_name>')
api.add_resource(DeleteData, '/delete_data/<string:table_name>')
api.add_resource(UpdateData, '/update_data/<string:table_name>')

# Main function to start the server
def main():
    http = WSGIServer(('', 5000), app.wsgi_app)
    http.serve_forever()

# Run continuously
if __name__ == '__main__':
    main()
