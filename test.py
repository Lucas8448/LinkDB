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


def test_create_table():
    table_schema = {
        "table_name": "testtable",
        "columns": {
            "id": "int PRIMARY KEY",
            "name": "text"
        }
    }
    response = requests.post(
        f"{BASE_URL}/create_table", json=table_schema, headers=headers)
    print("Create Table:", response.json())


def test_list_tables():
    response = requests.get(f"{BASE_URL}/list_tables", headers=headers)
    print("List Tables:", response.json())


def test_insert_data():
    data = {
        "id": 1,
        "name": "Lucas"
    }
    response = requests.post(
        f"{BASE_URL}/insert_data/testtable", json=data, headers=headers)
    print("Insert Data:", response.json())


def test_query_data():
    response = requests.get(
        f"{BASE_URL}/query_data/testtable", headers=headers)
    print("Query Data:", response.json())


def test_update_data():
    data = {
        "id": 1,
        "name": "Lucas Updated"
    }
    response = requests.put(
        f"{BASE_URL}/update_data/testtable", json=data, headers=headers)
    print("Update Data:", response.json())


def test_delete_data():
    data = {
        "id": 1
    }
    response = requests.delete(
        f"{BASE_URL}/delete_data/testtable", json=data, headers=headers)
    print("Delete Data:", response.json())


def get_usage_costs():
    response = requests.get(f"{BASE_URL}/get_usage_costs", headers=headers)
    if response.status_code == 200:
        cost = response.json().get('cost')
        print(f"Accumulated Cost: ${cost:.3f}")
    else:
        print("Failed to retrieve usage costs:", response.json())


def main():
    api_key = generate_api_key()
    if api_key:
        test_create_table()
        test_list_tables()
        test_insert_data()
        test_query_data()
        test_update_data()
        test_query_data()
        test_delete_data()
        get_usage_costs()


if __name__ == "__main__":
    main()