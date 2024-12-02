# Savannah Informatics
## Stack:
- Python (Apache Airflow)
- Google BigQuery

## Environment Setup:
- ``python -m venv savannah-env``
- ``source savannah-env/bin/activate``
- ``export AIRFLOW_HOME=$(pwd)/.airflow``
- ``pip install -r requirements.txt``
- ``mv example.env .env``

## Airflow Config:
- ``airflow db init``
- ``airflow users create --username admin --firstname Savannah --lastname Informatics --role Admin --email admin@savannah.com --password admin001``
- In ``.airflow/airflow.cfg`` modify ``dags_folder`` to the right path & set ``load_examples`` to ``False``
- ``airflow standalone``