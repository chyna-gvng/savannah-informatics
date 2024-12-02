from google.cloud import bigquery
import pandas as pd

def load_to_bigquery(table_id, file_path, client):
    df = pd.read_csv(file_path)
    job = client.load_table_from_dataframe(df, table_id)
    job.result()

def main():
    client = bigquery.Client()
    datasets = ['users_table', 'products_table', 'carts_table']
    files = ['users.csv', 'products.csv', 'carts.csv']

    for ds, file in zip(datasets, files):
        table_id = f'your_project.your_dataset.{ds}'
        load_to_bigquery(table_id, f'data/processed/{file}', client)

if __name__ == '__main__':
    main()
