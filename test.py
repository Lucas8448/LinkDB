import requests

BASE_URL = "http://localhost:5000"
API_KEY = "API_KEY_1234"

headers = {
    "API-Key": API_KEY,
    "Content-Type": "application/json"
}

# Test Create Keyspace


def test_create_keyspace():
    response = requests.post(f"{BASE_URL}/create_keyspace",
                             json={"keyspace_name": "testkeyspace"}, headers=headers)
    print("Create Keyspace:", response.json())

# Test Create Table


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


# Test List Tables


def test_list_tables():
    response = requests.get(
        f"{BASE_URL}/list_tables/testkeyspace", headers=headers)
    print("List Tables:", response.json())

# Test Insert Data


def test_insert_data():
    data = {
        "id": 1,
        "name": "Lucas"
    }
    response = requests.post(
        f"{BASE_URL}/insert_data/testkeyspace/testtable", json=data, headers=headers)
    print("Insert Data:", response.json())

# Test Query Data


def test_query_data():
    response = requests.get(
        f"{BASE_URL}/query_data/testkeyspace/testtable", headers=headers)
    print("Query Data:", response.json())


def main():
    test_create_keyspace()
    test_create_table()
    test_list_tables()
    test_insert_data()
    test_query_data()


if __name__ == "__main__":
    main()