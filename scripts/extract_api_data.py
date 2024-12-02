# scripts/extract_api_data.py

import requests
import json
import os

def fetch_data(api_url, file_path):
    response = requests.get(api_url)
    data = response.json()
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as f:
        json.dump(data, f)

def main():
    # API URLs
    apis = {
        'users': 'https://dummyjson.com/users',
        'products': 'https://dummyjson.com/products',
        'carts': 'https://dummyjson.com/carts'
    }

    for name, url in apis.items():
        fetch_data(url, f'data/raw/{name}.json')

if __name__ == "__main__":
    main()
