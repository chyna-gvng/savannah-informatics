# Savannah Informatics
## Stack:
- Python (Apache Airflow)
- Google BigQuery

## Environment Setup:
- ``python -m venv savannah-env``
- ``source savannah-env/bin/activate``
- ``pip install -r requirements.txt``
- ``mv example.env .env``

## Airflow Config:
- ``airflow db init``
- ``airflow users create --username admin --firstname Savannah --lastname Informatics --role Admin --email admin@savannah.com --password admin``
- ``airflow standalone``