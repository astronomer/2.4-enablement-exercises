from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

S3_BUCKET = "myexamplebucketone"

# added the age dataset
estimate_age_dataset = Dataset("file://localhost/airflow/include/estimated_age.txt")

with DAG(
    dag_id="ex_2_getting_data_from_S3_solution",
    start_date=datetime(2022, 10, 1),
    schedule="@daily",
    tags=["exercise_2", "datasets", "solution"],
    catchup=False
):

    @task
    def get_my_estimated_age():
        s3_hook = S3Hook(aws_conn_id='aws_conn')
        file_content = s3_hook.read_key(
                key="age_estimate.txt",
                bucket_name=S3_BUCKET
        )
        return int(file_content[1:-1])

    @task(outlets=[estimate_age_dataset])
    def write_age_to_local_file(estimated_age):
        f = open("include/estimated_age.txt", "w")
        f.write(f"{estimated_age}\n")
        f.close()

    write_age_to_local_file(get_my_estimated_age())