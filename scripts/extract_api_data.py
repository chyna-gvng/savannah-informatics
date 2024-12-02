import requests
import json

def fetch_data(api_url, file_path):
    response = requests.get(api_url)
    data = response.json()
    with open(file_path, 'w') as f:
        json.dump(data, f)

# API URLs
apis = {
    'users': 'https://dummyjson.com/users',
    'products': 'https://dummyjson.com/products',
    'carts': 'https://dummyjson.com/carts'
}

for name, url in apis.items():
    fetch_data(url, f'data/raw/{name}.json')