"""This DAG runs every 5 minutes."""

from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
import random

S3_BUCKET = "myexamplebucketone"

random_number_dataset = Dataset(f"s3://{S3_BUCKET}/random_number.txt")

with DAG(
    dag_id="ex_1_part_2_fast_scheduled_producer_dag_solution",
    start_date=datetime(2022, 10, 1),
    schedule="*/2 * * * *",
    tags=["exercise_1", "datasets", "task", "ex_1_part_2"],
    catchup=False
):

    @task
    def create_random_number():
        random_number = random.randint(0,100)
        return random_number

    create_object_in_S3 = S3CreateObjectOperator(
        task_id="create_object_in_S3",
        aws_conn_id="aws_conn",
        s3_key=f"s3://{S3_BUCKET}/" + "random_number.txt",
        data="{{ ti.xcom_pull(task_ids=['create_random_number'], key='return_value') }}",
        replace=True,
        outlets=[random_number_dataset]
    )

    create_random_number() >> create_object_in_S3





