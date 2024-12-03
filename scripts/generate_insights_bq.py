import os
from google.cloud import bigquery
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()

    # Verify that credentials and project ID are set
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    project_id = os.getenv('GCP_PROJECT_ID')
    if not credentials_path or not project_id:
        raise ValueError("Missing required environment variables.")

    # Set up BigQuery client
    client = bigquery.Client.from_service_account_json(credentials_path)

    # Read SQL queries from file
    sql_file_path = os.path.join(os.path.dirname(__file__), '../sql/generate_insights.sql')
    with open(sql_file_path, 'r') as sql_file:
        queries = sql_file.read().split(';')  # Split queries by semicolon

    # Execute each query
    for query in queries:
        if query.strip():  # Skip empty queries
            print(f"Executing query: {query}")
            job = client.query(query)
            job.result()  # Wait for query to finish
            print(f"Query completed successfully.")
