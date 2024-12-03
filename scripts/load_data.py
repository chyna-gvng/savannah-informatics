import os
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd
import logging
from config.config import PROCESSED_DIR

logging.basicConfig(level=logging.INFO)

load_dotenv()

def load_to_bigquery(table_id: str, file_path: str, client: bigquery.Client) -> None:
    """
    Load a DataFrame from a CSV file into a BigQuery table.

    Parameters:
    table_id (str): The ID of the BigQuery table.
    file_path (str): The path to the CSV file.
    client (bigquery.Client): The BigQuery client.
    """
    try:
        df = pd.read_csv(file_path)
        job = client.load_table_from_dataframe(df, table_id)
        job.result()
        logging.info(f"Data loaded into {table_id}")
    except Exception as e:
        logging.error(f"Error loading data into {table_id}: {e}")

def main():
    """
    Main function to load cleaned data into BigQuery tables.
    """
    if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
        raise ValueError("Credentials not set.")

    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    client = bigquery.Client.from_service_account_json(credentials_path)

    datasets = ['users_table', 'products_table', 'carts_table']
    files = ['users.csv', 'products.csv', 'carts.csv']
    project_id = os.environ['GCP_PROJECT_ID']
    dataset_id = os.environ['BQ_DATASET']

    for ds, file in zip(datasets, files):
        table_id = f'{project_id}.{dataset_id}.{ds}'
        load_to_bigquery(table_id, f'{PROCESSED_DIR}/{file}', client)

if __name__ == '__main__':
    main()
