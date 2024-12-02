import pandas as pd
import json

# Load raw data
def load_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

# Clean Users data
def clean_users(data):
    users = pd.json_normalize(data, record_path='data')
    users[['street', 'city', 'postal_code']] = users['address'].str.split(', ', expand=True)
    return users[['user_id', 'first_name', 'last_name', 'gender', 'age', 'street', 'city', 'postal_code']]

# Clean Products data
def clean_products(data):
    products = pd.json_normalize(data, record_path='data')
    products = products[products['price'] > 50]
    return products[['product_id', 'name', 'category', 'brand', 'price']]

# Clean Carts data
def clean_carts(data):
    carts = pd.json_normalize(data, record_path='data')
    carts_exp = carts.explode('products')
    carts_exp = carts_exp.reset_index(drop=True)
    carts_exp['total_cart_value'] = carts_exp['products'].apply(lambda x: x['price'] * x['quantity'])
    carts_final = carts_exp[['cart_id', 'user_id', 'products.product_id', 'products.quantity', 'products.price', 'total_cart_value']]
    return carts_final

# Save processed data
def save_data(df, file_path):
    df.to_csv(file_path, index=False)

# Main function
def main():
    users_raw = load_json('data/raw/users.json')
    products_raw = load_json('data/raw/products.json')
    carts_raw = load_json('data/raw/carts.json')

    users_clean = clean_users(users_raw)
    products_clean = clean_products(products_raw)
    carts_clean = clean_carts(carts_raw)

    save_data(users_clean, 'data/processed/users.csv')
    save_data(products_clean, 'data/processed/products.csv')
    save_data(carts_clean, 'data/processed/carts.csv')

if __name__ == '__main__':
    main()
