import os
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd

# Load environment variables from .env
load_dotenv()

def load_to_bigquery(table_id, file_path, client):
    df = pd.read_csv(file_path)
    job = client.load_table_from_dataframe(df, table_id)
    job.result()

def create_tables(client):
    # Read the SQL queries from the create_tables.sql file
    sql_file_path = os.path.join(os.path.dirname(__file__), '../sql/create_tables.sql')
    with open(sql_file_path, 'r') as sql_file:
        create_tables_query = sql_file.read()

    # Execute the queries to create tables
    job = client.query(create_tables_query)
    job.result()  # Wait for the query to finish
    print("Tables created successfully.")

def main():
    # Verify that GOOGLE_APPLICATION_CREDENTIALS is set
    if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not found. Did you forget to set it in your .env file?")
    
    # Use the environment variable for credentials path
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path  # Redundant but explicit
    
    client = bigquery.Client.from_service_account_json(credentials_path)
    
    # Create tables by running the SQL script
    create_tables(client)

    datasets = ['users_table', 'products_table', 'carts_table']
    files = ['users.csv', 'products.csv', 'carts.csv']
    project_id = os.environ['GCP_PROJECT_ID']
    dataset_id = os.environ['BQ_DATASET']

    for ds, file in zip(datasets, files):
        table_id = f'{project_id}.{dataset_id}.{ds}'
        load_to_bigquery(table_id, f'data/processed/{file}', client)

if __name__ == '__main__':
    main()
