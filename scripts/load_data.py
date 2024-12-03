import os
from google.cloud import bigquery
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)

def main():
    """
    Main function to execute SQL queries to generate insights in BigQuery.
    """
    load_dotenv()

    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    project_id = os.getenv('GCP_PROJECT_ID')
    if not credentials_path or not project_id:
        raise ValueError("Missing required environment variables.")

    client = bigquery.Client.from_service_account_json(credentials_path)

    sql_file_path = os.path.join(os.path.dirname(__file__), '../sql/generate_insights.sql')
    with open(sql_file_path, 'r') as sql_file:
        queries = sql_file.read().split(';')

    for query in queries:
        if query.strip():
            try:
                logging.info(f"Executing query: {query}")
                job = client.query(query)
                job.result()
                logging.info(f"Query completed successfully.")
            except Exception as e:
                logging.error(f"Error executing query: {e}")

if __name__ == '__main__':
    main()
