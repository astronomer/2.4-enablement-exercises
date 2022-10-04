from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from os import listdir
from os.path import isfile, join

S3_BUCKET = "myexamplebucketone"

dataset_exercise_3 = Dataset(f"s3://{S3_BUCKET}/ex3")

with DAG(
    dag_id="ex_3_upload_files_solution",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    tags=["exercise_3", "dynamic_tasks", "solution"],
    catchup=False
):

    @task
    def get_filenames_and_dest_keys():
        filenames_and_dest_keys = [
            {
                "filename": f"include/ex_3/{f}",
                "dest_key": f"ex3/{f}"
            } for f in listdir("include/ex_3") 
            if isfile(join("include/ex_3", f))
        ]
        return filenames_and_dest_keys

    upload_files = LocalFilesystemToS3Operator.partial(
        task_id="upload_files",
        dest_bucket=S3_BUCKET,
        aws_conn_id="aws_conn",
        replace=True,
        outlets=[dataset_exercise_3]
    ).expand_kwargs(get_filenames_and_dest_keys())