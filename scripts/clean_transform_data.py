import pandas as pd
import json

# Load raw data
def load_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

# Clean Users data
def clean_users(data):
    users = pd.json_normalize(data.get('users', []))
    
    # Rename columns first
    users.rename(columns={
        'id': 'user_id', 
        'firstName': 'first_name', 
        'lastName': 'last_name',
        'address.address': 'street',
        'address.city': 'city',
        'address.postalCode': 'postal_code'
    }, inplace=True)
    
    return users[['user_id', 'first_name', 'last_name', 'gender', 'age', 'street', 'city', 'postal_code']]

# Clean Products data
def clean_products(data):
    products = pd.json_normalize(data.get('products', []))
    
    # Exclude products with price <= 50
    products = products[products['price'] > 50]
    
    # Rename columns and select relevant fields
    products.rename(columns={
        'id': 'product_id', 
        'title': 'name'
    }, inplace=True)
    
    # Select and reorder columns
    return products[['product_id', 'name', 'category', 'brand', 'price']]

# Clean Carts data
def clean_carts(data):
    # Normalize carts data with nested products
    carts = pd.json_normalize(
        data.get('carts', []), 
        record_path='products', 
        meta=['id', 'userId'], 
        meta_prefix='cart_'
    )
    
    # Rename columns 
    carts.rename(columns={
        'id': 'product_id', 
        'cart_userId': 'user_id',
        'cart_id': 'cart_id',
        'title': 'product_name'
    }, inplace=True)
    
    # Calculate total cart value
    carts['total_cart_value'] = carts.groupby('cart_id')['total'].transform('first')
    
    # Select and reorder columns
    return carts[['cart_id', 'user_id', 'product_id', 'product_name', 'quantity', 'price', 'total_cart_value']]

# Save processed data
def save_data(df, file_path):
    df.to_csv(file_path, index=False)

# Main function
def main():
    # File paths
    raw_users_file = 'data/raw/users.json'
    raw_products_file = 'data/raw/products.json'
    raw_carts_file = 'data/raw/carts.json'

    processed_users_file = 'data/processed/users.csv'
    processed_products_file = 'data/processed/products.csv'
    processed_carts_file = 'data/processed/carts.csv'

    # Load raw data
    users_raw = load_json(raw_users_file)
    products_raw = load_json(raw_products_file)
    carts_raw = load_json(raw_carts_file)

    # Process and clean data
    users_clean = clean_users(users_raw)
    products_clean = clean_products(products_raw)
    carts_clean = clean_carts(carts_raw)

    # Save cleaned data
    save_data(users_clean, processed_users_file)
    save_data(products_clean, processed_products_file)
    save_data(carts_clean, processed_carts_file)

if __name__ == '__main__':
    main()
