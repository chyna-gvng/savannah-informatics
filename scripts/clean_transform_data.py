import pandas as pd
import json
import logging
from config.config import RAW_DIR, PROCESSED_DIR
from typing import Dict, Any
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def load_json(file_path: str) -> Dict[str, Any]:
    """
    Load JSON data from a file.

    Parameters:
    file_path (str): The path to the JSON file.

    Returns:
    dict: The loaded JSON data.
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {file_path}")
        return None

def clean_users(data: Dict[str, Any]) -> pd.DataFrame:
    """
    Clean and normalize users data.

    Parameters:
    data (dict): The raw users data.

    Returns:
    pd.DataFrame: The cleaned users data.
    """
    users = pd.json_normalize(data.get('users', []))
    users.rename(columns={
        'id': 'user_id',
        'firstName': 'first_name',
        'lastName': 'last_name',
        'address.address': 'street',
        'address.city': 'city',
        'address.postalCode': 'postal_code'
    }, inplace=True)
    
    # Add timestamp column
    users['ingestion_timestamp'] = datetime.now().isoformat()
    
    return users[['user_id', 'first_name', 'last_name', 'gender', 'age', 'street', 'city', 'postal_code', 'ingestion_timestamp']]

def clean_products(data: Dict[str, Any]) -> pd.DataFrame:
    """
    Clean and normalize products data.

    Parameters:
    data (dict): The raw products data.

    Returns:
    pd.DataFrame: The cleaned products data.
    """
    products = pd.json_normalize(data.get('products', []))
    products = products[products['price'] > 50]
    products.rename(columns={
        'id': 'product_id',
        'title': 'name'
    }, inplace=True)
    
    # Add timestamp column
    products['ingestion_timestamp'] = datetime.now().isoformat()
    
    return products[['product_id', 'name', 'category', 'brand', 'price', 'ingestion_timestamp']]

def clean_carts(data: Dict[str, Any]) -> pd.DataFrame:
    """
    Clean and normalize carts data.

    Parameters:
    data (dict): The raw carts data.

    Returns:
    pd.DataFrame: The cleaned carts data.
    """
    # Create a list to store expanded cart data
    expanded_carts = []
    ingestion_timestamp = datetime.now().isoformat()

    for cart in data.get('carts', []):
        cart_id = cart['id']
        user_id = cart['userId']
        total_cart_value = cart['total']
        
        for product in cart['products']:
            expanded_cart_entry = {
                'cart_id': cart_id,
                'user_id': user_id,
                'product_id': product['id'],
                'product_name': product['title'],
                'quantity': product['quantity'],
                'price': product['price'],
                'total': product['total'],
                'total_cart_value': total_cart_value,
                'ingestion_timestamp': ingestion_timestamp
            }
            expanded_carts.append(expanded_cart_entry)

    # Convert to DataFrame
    carts_df = pd.DataFrame(expanded_carts)
    
    return carts_df[['cart_id', 'user_id', 'product_id', 'product_name', 
                     'quantity', 'price', 'total', 'total_cart_value', 'ingestion_timestamp']]

def save_data(df: pd.DataFrame, file_path: str) -> None:
    """
    Save a DataFrame to a CSV file.

    Parameters:
    df (pd.DataFrame): The DataFrame to save.
    file_path (str): The path to save the CSV file.
    """
    df.to_csv(file_path, index=False)
    logging.info(f"Data saved to {file_path}")

def main():
    """
    Main function to clean and transform data from raw JSON files.
    """
    raw_users_file = f'{RAW_DIR}/users.json'
    raw_products_file = f'{RAW_DIR}/products.json'
    raw_carts_file = f'{RAW_DIR}/carts.json'

    processed_users_file = f'{PROCESSED_DIR}/users.csv'
    processed_products_file = f'{PROCESSED_DIR}/products.csv'
    processed_carts_file = f'{PROCESSED_DIR}/carts.csv'

    users_raw = load_json(raw_users_file)
    products_raw = load_json(raw_products_file)
    carts_raw = load_json(raw_carts_file)

    if users_raw and products_raw and carts_raw:
        users_clean = clean_users(users_raw)
        products_clean = clean_products(products_raw)
        carts_clean = clean_carts(carts_raw)

        save_data(users_clean, processed_users_file)
        save_data(products_clean, processed_products_file)
        save_data(carts_clean, processed_carts_file)
    else:
        logging.error("Failed to load raw data")

if __name__ == '__main__':
    main()
