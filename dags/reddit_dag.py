from airflow import DAG
from datetime import datetime
import os
import sys

# Output of below is : Reddit-data-pipeline
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner': 'Mitesh Agarwal',
    'start_date': datetime(2025, 7, 15)
}

file_postfix = datetime.now().strftime("%Y%m%d%")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)

# First we want to extract the data from Reddit

extract = PythonOperator(
    task_id = 'reddit_extraction',
    python_callable = reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100 
    }
)
# Upload to S3 bucket