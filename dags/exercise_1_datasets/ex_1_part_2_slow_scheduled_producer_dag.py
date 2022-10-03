"""Make this DAG into a DAG with a producing task."""

from airflow import DAG, Dataset
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.decorators import task
from datetime import datetime
import requests

# Add your S3 Bucket and name
S3_BUCKET = "YOUR_S3_BUCKET"
YOUR_NAME = "YOURNAME"

API = f"https://api.agify.io/?name={YOUR_NAME}"

with DAG(
    dag_id="ex_1_part_2_slow_scheduled_producer_dag",
    start_date=datetime(2022, 10, 1),
    schedule="*/5 * * * *",
    tags=["exercise_1", "datasets", "task", "ex_1_part_2"],
    catchup=False
):

    @task
    def get_age_estimate():
        r = requests.get(API)
        return r.json()["age"]

    create_object_in_S3 = S3CreateObjectOperator(
        task_id="create_object_in_S3",
        aws_conn_id="aws_conn",
        s3_key=f"s3://{S3_BUCKET}/" + "age_estimate.txt",
        data="{{ ti.xcom_pull(task_ids=['get_age_estimate'], key='return_value') }}",
        replace=True
    )

    get_age_estimate() >> create_object_in_S3
