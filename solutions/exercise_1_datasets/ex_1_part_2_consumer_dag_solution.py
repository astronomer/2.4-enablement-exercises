from airflow import DAG, Dataset, XComArg
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateObjectOperator, S3ListOperator
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
from datetime import datetime

S3_BUCKET = "myexamplebucketone"

API = "http://numbersapi.com/"

age_estimate_dataset = Dataset(f"s3://{S3_BUCKET}/ex1/age_estimate.txt")
random_number_dataset = Dataset(f"s3://{S3_BUCKET}/ex1/random_number.txt")

with DAG(
    dag_id="ex_1_part_2_consumer_dag_solution",
    start_date=datetime(2022, 10, 1),
    schedule=[age_estimate_dataset, random_number_dataset],
    tags=["exercise_1", "datasets", "task", "ex_1_part_2"],
    catchup=False
):

    list_files_ingest_bucket = S3ListOperator(
        task_id="list_files_ingest_bucket",
        aws_conn_id="aws_conn",
        bucket=S3_BUCKET,
        prefix="ex1/"
    )

    @task
    def read_keys_form_s3(source_key_list):
        s3_hook = S3Hook(aws_conn_id='aws_conn')
        num_sum = 0
        for key in source_key_list:
            file_content = s3_hook.read_key(
                key=key,
                bucket_name=S3_BUCKET
            )
            num_sum += int(file_content[1:-1])
        return num_sum

    @task
    def get_a_number_fact(num_sum):
        print(str(num_sum))
        r = requests.get(API + str(num_sum) + "/math")
        return r.text

    get_a_number_fact(read_keys_form_s3(XComArg(list_files_ingest_bucket)))