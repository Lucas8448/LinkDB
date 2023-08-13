from flask import Flask, request, jsonify
from flask_restful import Api, Resource
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_cors import CORS
import uuid


def generate_api_key():
    return str(uuid.uuid4())


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:5500"}})
api = Api(app)

# Connection details
SCYLLA_HOST = 'localhost'
SCYLLA_PORT = 9042

auth_provider = PlainTextAuthProvider(
    username='cassandra', password='cassandra')
cluster = Cluster([SCYLLA_HOST], port=SCYLLA_PORT, auth_provider=auth_provider)
session = cluster.connect()

KEYSPACE_FOR_API_KEYS = "api_keys"
session.execute(
    f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE_FOR_API_KEYS} WITH replication = {{'class':'SimpleStrategy', 'replication_factor':1}}")
session.set_keyspace(KEYSPACE_FOR_API_KEYS)
CREATE_API_KEYS_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS api_keys (
    api_key TEXT PRIMARY KEY,
    client_keyspace TEXT
)
"""
session.execute(CREATE_API_KEYS_TABLE_QUERY)


def authenticate(api_key, keyspace_name):
    query = f"SELECT api_key, client_keyspace FROM {KEYSPACE_FOR_API_KEYS}.api_keys WHERE api_key = ?"
    prepared_statement = session.prepare(query)
    bound_statement = prepared_statement.bind((api_key,))
    rows = session.execute(bound_statement)
    result = rows.one()
    if result and result.client_keyspace == keyspace_name:
        return True
    return False


class GenerateAPIKey(Resource):
    def post(self):
        new_key = generate_api_key()
        # Create a unique keyspace name for the user
        new_keyspace = "ks_" + new_key.split('-')[0]
        insert_query = f"INSERT INTO {KEYSPACE_FOR_API_KEYS}.api_keys (api_key, client_keyspace) VALUES ('{new_key}', '{new_keyspace}')"
        session.execute(insert_query)
        # Create the keyspace as well
        create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {new_keyspace} 
        WITH replication = {{'class':'SimpleStrategy', 'replication_factor':1}}
        """
        session.execute(create_keyspace_query)
        return {'api_key': new_key, 'keyspace': new_keyspace}

limiter = Limiter(
  app,
  key_func=get_remote_address,
  default_limits=["400 per day", "50 per hour"]
)


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
  def post(self, keyspace_name):
    api_key = request.headers.get('API-Key')
    if not authenticate(api_key, keyspace_name):
      return {'status': 'error', 'message': 'Unauthorized'}, 401

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
  def get(self, keyspace_name):
    api_key = request.headers.get('API-Key')
    if not authenticate(api_key, keyspace_name):
      return {'message': 'Unauthorized'}, 401

    list_tables_query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace_name}'"
    rows = session.execute(list_tables_query)
    return {'tables': [row.table_name for row in rows]}


class InsertData(Resource):
  def post(self, keyspace_name, table_name):
    api_key = request.headers.get('API-Key')
    if not authenticate(api_key, keyspace_name):
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
  def get(self, keyspace_name, table_name):
    api_key = request.headers.get('API-Key')
    if not authenticate(api_key, keyspace_name):
      return {'status': 'error', 'message': 'Unauthorized'}, 401

    limit = int(request.args.get('limit', 50))

    select_data_query = f"SELECT * FROM {keyspace_name}.{table_name} LIMIT {limit}"
    rows = session.execute(select_data_query)


    # Convert rows to dictionaries using _asdict()
    return {'status': 'success', 'data': [row._asdict() for row in rows]}


class DeleteData(Resource):
    def delete(self, keyspace_name, table_name):
        api_key = request.headers.get('API-Key')
        if not authenticate(api_key, keyspace_name):
            return {'message': 'Unauthorized'}, 401

        data = request.get_json()
        if "id" not in data:
            return {'message': 'ID is required for deletion.'}, 400

        delete_data_query = f"DELETE FROM {keyspace_name}.{table_name} WHERE id={data['id']}"
        session.execute(delete_data_query)
        return {'message': 'Data deleted successfully.'}


class UpdateData(Resource):
    def put(self, keyspace_name, table_name):
        api_key = request.headers.get('API-Key')
        if not authenticate(api_key, keyspace_name):
            return {'message': 'Unauthorized'}, 401

        data = request.get_json()
        if "id" not in data:
            return {'message': 'ID is required for update.'}, 400

        set_clause = ", ".join([f"{k}='{v}'" if isinstance(
            v, str) else f"{k}={v}" for k, v in data.items() if k != "id"])

        update_data_query = f"UPDATE {keyspace_name}.{table_name} SET {set_clause} WHERE id={data['id']}"
        session.execute(update_data_query)
        return {'message': 'Data updated successfully.'}


api.add_resource(Home, '/')
api.add_resource(GenerateAPIKey, '/generate_api_key')
api.add_resource(CreateTable, '/create_table/<string:keyspace_name>')
api.add_resource(ListTables, '/list_tables/<string:keyspace_name>')
api.add_resource(
  InsertData, '/insert_data/<string:keyspace_name>/<string:table_name>')
api.add_resource(
  QueryData, '/query_data/<string:keyspace_name>/<string:table_name>')
api.add_resource(
    DeleteData, '/delete_data/<string:keyspace_name>/<string:table_name>')
api.add_resource(
    UpdateData, '/update_data/<string:keyspace_name>/<string:table_name>')

if __name__ == '__main__':
  app.run()
