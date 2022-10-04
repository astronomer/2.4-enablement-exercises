from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator
)
from os import listdir
from os.path import isfile, join

# Add your S3 bucket
S3_BUCKET = "Your S3 bucket"

dataset_exercise_3 = Dataset(f"s3://{S3_BUCKET}/ex3")

with DAG(
    dag_id="ex_3_upload_files",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    tags=["exercise_3", "dynamic_tasks", "task"],
    catchup=False
):

    # This task creates the input that the downstream task maps over
    @task
    def get_filenames_and_dest_keys():
        filenames_and_dest_keys = [
            """

            Enter your code here! If you are using the same S3 bucket for
            all exercises it is recommended you store the files from
            exercise 3 in a seperate folder.
                
            """ for f in listdir("include/ex_3") 
            if isfile(join("include/ex_3", f))
        ]
        return filenames_and_dest_keys

    # this task should use dynamic task mapping
    upload_files = LocalFilesystemToS3Operator(
        task_id="upload_files",
        dest_bucket=S3_BUCKET,
        aws_conn_id="aws_conn",
        replace=True,
        outlets=[dataset_exercise_3],
        dest_key="MAP ME",
        filename="MAP ME"
    )