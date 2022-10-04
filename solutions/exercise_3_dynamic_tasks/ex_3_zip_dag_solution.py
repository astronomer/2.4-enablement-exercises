from airflow import DAG, XComArg, Dataset
from airflow.decorators import task
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator, S3CopyObjectOperator, S3CreateObjectOperator
)
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from os import listdir
from os.path import isfile, join
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_BUCKET = "myexamplebucketone"

dataset_exercise_3 = Dataset(f"s3://{S3_BUCKET}/ex3")

with DAG(
    dag_id="ex_3_zip_dag_solution",
    start_date=datetime(2022, 10, 1),
    schedule=[dataset_exercise_3],
    tags=["exercise_3", "dynamic_tasks", "solution"],
    catchup=False
):

    list_files_S3_bucket = S3ListOperator(
        task_id="list_files_S3_bucket",
        aws_conn_id="aws_conn",
        bucket=S3_BUCKET,
        prefix="ex3"
    )

    @task
    def get_file_contents(source_key_list):
        s3_hook = S3Hook(aws_conn_id='aws_conn')
        file_contents = []
        for key in source_key_list:
            file_content = s3_hook.read_key(
                key=key,
                bucket_name=S3_BUCKET
            )
            file_contents.append(file_content)
        return file_contents

    file_contents = get_file_contents(XComArg(list_files_S3_bucket))

    zipped_arguments = XComArg(list_files_S3_bucket).zip(file_contents)
    
    @task 
    def write_new_files(args):
        
        old_file_name=args[0][4:]
        file_content=args[1]

        protagonist=file_content.split(" ")[0]

        new_file_name=protagonist + "_" + old_file_name

        f = open(f"include/ex_3/solutions/{new_file_name}", "w")
        f.write(f"On {old_file_name} {file_content}")
        f.close()

    write_new_files.partial().expand(args=zipped_arguments)

