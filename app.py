import os
import logging
from flask import Flask, request, jsonify
from flask_restful import Api, Resource
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__)
api = Api(app)

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Replace with your Scylla/Cassandra credentials if needed.
auth_provider = PlainTextAuthProvider(
  username='cassandra', password='cassandra')
cluster = Cluster(['localhost'], port=9042, auth_provider=auth_provider)
session = cluster.connect()

# In a real-world scenario, store and manage these securely.
API_KEYS = ["API_KEY_1234"]

def authenticate(api_key):
  if api_key not in API_KEYS:
    return False
  return True


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

class CreateKeyspace(Resource):
  def post(self):
    api_key = request.headers.get('API-Key')
    if not authenticate(api_key):
      return {'message': 'Unauthorized'}, 401

    data = request.get_json()
    keyspace_name = data['keyspace_name']
    # default replication factor is 1
    replication_factor = data.get('replication_factor', 1)

    create_keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace_name} 
    WITH replication = {{'class':'SimpleStrategy', 'replication_factor':{replication_factor}}}
    """
    session.execute(create_keyspace_query)
    return {'message': f'Keyspace {keyspace_name} created successfully.'}


class CreateTable(Resource):
  def post(self, keyspace_name):
    api_key = request.headers.get('API-Key')
    if not authenticate(api_key):
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
    if not authenticate(api_key):
      return {'message': 'Unauthorized'}, 401

    list_tables_query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace_name}'"
    rows = session.execute(list_tables_query)
    return {'tables': [row.table_name for row in rows]}


class InsertData(Resource):
  def post(self, keyspace_name, table_name):
    api_key = request.headers.get('API-Key')
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
  def get(self, keyspace_name, table_name):
    api_key = request.headers.get('API-Key')
    if not authenticate(api_key):
      return {'status': 'error', 'message': 'Unauthorized'}, 401

    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 50))
    offset = (page - 1) * limit

    select_data_query = f"SELECT * FROM {keyspace_name}.{table_name} LIMIT {limit} OFFSET {offset}"
    rows = session.execute(select_data_query)

    # Convert rows to dictionaries using _asdict()
    return {'status': 'success', 'data': [row._asdict() for row in rows]}


api.add_resource(CreateTable, '/create_table/<string:keyspace_name>')
api.add_resource(CreateKeyspace, '/create_keyspace')
api.add_resource(ListTables, '/list_tables/<string:keyspace_name>')
api.add_resource(
  InsertData, '/insert_data/<string:keyspace_name>/<string:table_name>')
api.add_resource(
  QueryData, '/query_data/<string:keyspace_name>/<string:table_name>')

@app.errorhandler(Exception)
def handle_exception(e):
  logger.exception("Unhandled Exception: %s", str(e))
  return {'message': 'Internal server error'}, 500

if __name__ == '__main__':
  app.run()
