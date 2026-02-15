import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv

# Import your custom logic
from src.transform import transform_data
from src.feature_extraction import main as run_feature_extraction
from src.error_plotting import main as run_error_plotting
from src.load import upload_to_gdrive

# 1. Load the .env file
# This assumes your .env is in the project root
load_dotenv()

# 2. Path Logic (Root detection)
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(DAGS_DIR, ".."))
LOCAL_PATH = os.path.join(PROJECT_ROOT, "etl/unprocessed")

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="s3_to_gdrive_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    extract_from_s3 = BashOperator(
        task_id="extract_from_s3",
        bash_command=f"""
        echo "Root detected as: {PROJECT_ROOT}" && \
        mkdir -p {LOCAL_PATH} && \
        aws s3 sync s3://empatica-us-east-1-prod-data/v2/1536/1/1/participant_data/ {LOCAL_PATH} && \
        echo "Check this folder now: {LOCAL_PATH}" && \
        ls -F {LOCAL_PATH}
        """,
        # ONLY pass the keys you need, not the whole os.environ
        env={
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            "PATH": os.environ.get("PATH") # Keep PATH so bash can find 'aws'
        }
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    feature_extraction_task = PythonOperator(
        task_id="feature_extraction",
        python_callable=run_feature_extraction
    )

    error_plotting_task = PythonOperator(
        task_id="error_plotting",
        python_callable=run_error_plotting
    )

    load_task = PythonOperator(
        task_id="upload_to_gdrive",
        python_callable=upload_to_gdrive
    )

    extract_from_s3 >> transform_task >> feature_extraction_task >> error_plotting_task >> load_task