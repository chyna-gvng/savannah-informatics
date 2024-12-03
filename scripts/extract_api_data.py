import requests
import json
import os
import logging
from config.config import RAW_DIR

logging.basicConfig(level=logging.INFO)

def fetch_data(api_url, file_path):
    """
    Fetch data from an API and save it to a file.

    Parameters:
    api_url (str): The URL of the API.
    file_path (str): The path to save the JSON data.
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(data, f)
        logging.info(f"Data fetched and saved to {file_path}")
    except requests.RequestException as e:
        logging.error(f"Error fetching data from {api_url}: {e}")

def main():
    apis = {
        'users': 'https://dummyjson.com/users',
        'products': 'https://dummyjson.com/products',
        'carts': 'https://dummyjson.com/carts'
    }

    for name, url in apis.items():
        fetch_data(url, f'{RAW_DIR}/{name}.json')

if __name__ == "__main__":
    main()
