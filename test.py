import requests

BASE_URL = "http://localhost:5000"

headers = {
    "Content-Type": "application/json"
}


def generate_api_key():
    response = requests.post(f"{BASE_URL}/generate_api_key")
    if response.status_code == 200:
        api_key = response.json().get('api_key')
        print(f"Generated API Key: {api_key}")
        headers["API-Key"] = api_key
        return api_key
    else:
        print("Failed to generate API key:", response.json())
        return None


def test_create_keyspace():
    response = requests.post(
        f"{BASE_URL}/create_keyspace", json={"keyspace_name": "testkeyspace"}, headers=headers)
    print("Create Keyspace:", response.json())


def test_create_table():
    table_schema = {
        "table_name": "testtable",
        "columns": {
            "id": "int PRIMARY KEY",
            "name": "text"
        }
    }
    response = requests.post(
        f"{BASE_URL}/create_table/testkeyspace", json=table_schema, headers=headers)
    print("Create Table:", response.json())


def test_list_tables():
    response = requests.get(
        f"{BASE_URL}/list_tables/testkeyspace", headers=headers)
    print("List Tables:", response.json())


def test_insert_data():
    data = {
        "id": 1,
        "name": "Lucas"
    }
    response = requests.post(
        f"{BASE_URL}/insert_data/testkeyspace/testtable", json=data, headers=headers)
    print("Insert Data:", response.json())


def test_query_data():
    response = requests.get(
        f"{BASE_URL}/query_data/testkeyspace/testtable", headers=headers)
    print("Query Data:", response.json())


def test_update_data():
    data = {
        "id": 1,
        "name": "Lucas Updated"
    }
    response = requests.put(
        f"{BASE_URL}/update_data/testkeyspace/testtable", json=data, headers=headers)
    print("Update Data:", response.json())


def test_delete_data():
    data = {
        "id": 1
    }
    response = requests.delete(
        f"{BASE_URL}/delete_data/testkeyspace/testtable", json=data, headers=headers)
    print("Delete Data:", response.json())


def main():
    if generate_api_key():
        test_create_keyspace()
        test_create_table()
        test_list_tables()
        test_insert_data()
        test_query_data()
        test_update_data()
        test_delete_data()


if __name__ == "__main__":
    main()