import requests

BASE_URL = "http://localhost:5000"

headers = {
    "Content-Type": "application/json"
}


def generate_api_key():
    response = requests.post(f"{BASE_URL}/generate_api_key")
    if response.status_code == 200:
        api_key = response.json().get('api_key')
        keyspace = response.json().get('keyspace')
        print(f"Generated API Key: {api_key}")
        print(f"Assigned Keyspace: {keyspace}")
        headers["API-Key"] = api_key
        return api_key, keyspace
    else:
        print("Failed to generate API key:", response.json())
        return None, None


def test_create_table(keyspace):
    table_schema = {
        "table_name": "testtable",
        "columns": {
            "id": "int PRIMARY KEY",
            "name": "text"
        }
    }
    response = requests.post(
        f"{BASE_URL}/create_table/{keyspace}", json=table_schema, headers=headers)
    print("Create Table:", response.json())


def test_list_tables(keyspace):
    response = requests.get(
        f"{BASE_URL}/list_tables/{keyspace}", headers=headers)
    print("List Tables:", response.json())


def test_insert_data(keyspace):
    data = {
        "id": 1,
        "name": "Lucas"
    }
    response = requests.post(
        f"{BASE_URL}/insert_data/{keyspace}/testtable", json=data, headers=headers)
    print("Insert Data:", response.json())


def test_query_data(keyspace):
    response = requests.get(
        f"{BASE_URL}/query_data/{keyspace}/testtable", headers=headers)
    print("Query Data:", response.json())


def test_update_data(keyspace):
    data = {
        "id": 1,
        "name": "Lucas Updated"
    }
    response = requests.put(
        f"{BASE_URL}/update_data/{keyspace}/testtable", json=data, headers=headers)
    print("Update Data:", response.json())


def test_delete_data(keyspace):
    data = {
        "id": 1
    }
    response = requests.delete(
        f"{BASE_URL}/delete_data/{keyspace}/testtable", json=data, headers=headers)
    print("Delete Data:", response.json())


def main():
    api_key, keyspace = generate_api_key()
    if api_key and keyspace:
        test_create_table(keyspace)
        test_list_tables(keyspace)
        test_insert_data(keyspace)
        test_query_data(keyspace)
        test_update_data(keyspace)
        test_query_data(keyspace)
        test_delete_data(keyspace)


if __name__ == "__main__":
    main()