from airflow import DAG
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator, S3CopyObjectOperator, S3CreateObjectOperator
)

# Add your buckets
S3_BUCKET = "Your origin bucket, can be empty"
S3_BUCKET_YML = "a target bucket for yml files"
S3_BUCKET_TXT = "a target bucket for txt files"

with DAG(
    dag_id="ex_4_expand_kwargs_dag",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    tags=["exercise_4", "dynamic_tasks", "task"],
    catchup=False
):

    # this creates lists of sample file names
    txt_keys = []
    yml_keys = []
    for i in range(10):
        txt_keys.append(f"s3://{S3_BUCKET}/ex4/{i}.txt")
        yml_keys.append(f"s3://{S3_BUCKET}/ex4/{i}.yml")

    # dynamically map this task create 10 text files in the S3 bucket
    create_txt_files_in_S3 = S3CreateObjectOperator(
        task_id="create_txt_files_in_S3",
        aws_conn_id="aws_conn",
        data="This is a text file.",
        replace=True,
        s3_key="MAP ME"
    )

    # dynamically map this task create 10 yml files in the S3 bucket
    create_yml_files_in_S3 = S3CreateObjectOperator(
        task_id="create_yml_files_in_S3",
        aws_conn_id="aws_conn",
        data="This is a yml file.",
        replace=True,
        s3_key="MAP ME"
    )

    # lists all files in the first S3 Bucket
    list_files_S3 = S3ListOperator(
        task_id="list_files_S3",
        aws_conn_id="aws_conn",
        bucket=S3_BUCKET
    )

    # add your code here:
    # txt files should go into S3_BUCKET_TXT, yaml files into S3_BUCKET_YML 
    # use dynamic task mapping over the S3CopyObjectOperator
    # hint: a very similar use case can be found under "Transforming mapped data"
    # section of the offical documentation

    copy_files = S3CopyObjectOperator(
        task_id="copy_files",
        aws_conn_id="aws_conn",
        dest_bucket_key="MAP ME",
        source_bucket_key="MAP ME"
    )

    [create_txt_files_in_S3, create_yml_files_in_S3] >> list_files_S3 
    list_files_S3 >> copy_files
