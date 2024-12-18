# Savannah Informatics

## Overview

This repository contains the implementation of a data pipeline for Savannah Informatics. The pipeline extracts data from multiple APIs, cleans and normalizes the data, loads it into Google BigQuery, and generates insights through SQL queries. The workflow is orchestrated using Apache Airflow.

## Tech Stack

- **Programming Language:** Python
- **Orchestration:** Apache Airflow
- **Data Warehouse:** Google BigQuery
- **Data Storage:** Google Cloud Storage (GCS)

## Directory Structure

```
savannah-informatics/
│
├── dags/
│   └── savannah_dag.py
├── scripts/
│   ├── extract_api_data.py
│   ├── clean_transform_data.py
│   ├── generate_insights_bq.py
│   └── load_data.py
├── sql/
│   └── generate_insights.sql
├── data/
│   ├── raw/
│   └── processed/
├── config/
│   └── config.py
├── requirements.txt
└── README.md
```

## Environment Setup

1. **Clone the Repository:**
   ```sh
   git clone https://github.com/chyna-gvng/savannah-informatics.git
   cd savannah-informatics
   ```

2. **Create a Virtual Environment:**
   ```sh
   python -m venv savannah-env
   ```

3. **Activate the Virtual Environment:**
   ```sh
   source savannah-env/bin/activate
   ```

4. **Set Airflow Home Directory:**
   ```sh
   export AIRFLOW_HOME=$(pwd)/.airflow
   ```

5. **Install Dependencies:**
   ```sh
   pip install -r requirements.txt
   ```

6. **Configure Environment Variables:**
   ```sh
   mv example.env .env
   ```

## Airflow Configuration

1. **Initialize Airflow Database:**
   ```sh
   airflow db init
   ```

2. **Create an Airflow User:**
   ```sh
   airflow users create --username admin --firstname Savannah --lastname Informatics --role Admin --email admin@savannah.com --password admin001
   ```

3. **Modify Airflow Configuration:**
   - In `.airflow/airflow.cfg`, set the `dags_folder` to the correct path.
   - Set `load_examples` to `False`.
   - Set `default_timezone` to `system`.

4. **Start Airflow:**
   ```sh
   airflow standalone
   ```

## Pipeline Workflow

The pipeline consists of the following steps:

1. **Extract Data from APIs:**
   - Fetches data from public APIs and saves the raw JSON data in Google Cloud Storage (GCS).

2. **Clean and Normalize Data:**
   - Processes the raw data to clean and normalize it into structured formats.

3. **Load Data into BigQuery:**
   - Loads the cleaned data into separate BigQuery tables.

4. **Generate Insights:**
   - Executes SQL queries to join and enrich data, generating insights and summary tables.

## Scripts Overview

- **`savannah_dag.py`:** Defines the Airflow DAG and tasks.
- **`extract_api_data.py`:** Fetches data from APIs and saves it locally.
- **`clean_transform_data.py`:** Cleans and normalizes the raw data.
- **`generate_insights_bq.py`:** Executes SQL queries to generate insights in BigQuery.
- **`load_data.py`:** Loads cleaned data into BigQuery tables.

## SQL Queries

The SQL queries for generating insights are stored in `sql/generate_insights.sql`. These queries include:

- **User Summary:** Aggregates total spending and number of purchases per user.
- **Category Summary:** Aggregates sales by product category.
- **Cart Details:** Provides transaction-level details enriched with user and product data.

## Assumptions and Trade-offs
### **Assumptions**  
- **Operating System**: The pipeline is designed to run on a Linux-based environment.  
- **API Data Completeness**: The data fetched from APIs is assumed to be complete and correctly structured.  
- **Pipeline Schedule**: The pipeline is designed to run daily, ensuring up-to-date data processing.

### **Trade-offs**  
- **WRITE_TRUNCATE vs. WRITE_APPEND**:  
  - The pipeline uses `WRITE_TRUNCATE` mode for BigQuery, replacing table contents on each run.  
  - This avoids complex deduplication logic but sacrifices the ability to retain historical data.

## Resources

- [Google Cloud Platform](https://console.cloud.google.com)
- [Dummy JSON API](https://dummyjson.com)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
