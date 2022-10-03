from airflow import DAG
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator, S3CopyObjectOperator, S3CreateObjectOperator
)

S3_BUCKET = "Your origin bucket, can be empty"
S3_BUCKET_YML = "a target bucket for yml files"
S3_BUCKET_TXT = "a target bucket for txt files"

with DAG(
    dag_id="ex_4_expand_kwargs_dag",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    tags=["exercise_4", "datasets", "task"],
    catchup=False
):

    # this section creates files to move using 2.3 dynamic task mapping features
    txt_keys = []
    yml_keys = []
    for i in range(10):
        txt_keys.append(f"s3://{S3_BUCKET}/{i}.txt")
        yml_keys.append(f"s3://{S3_BUCKET}/{i}.yml")

    create_txt_files_in_S3 = S3CreateObjectOperator.partial(
        task_id="create_txt_files_in_S3",
        aws_conn_id="aws_conn",
        data="This is a text file.",
        replace=True
    ).expand(s3_key=txt_keys)

    create_yml_files_in_S3 = S3CreateObjectOperator.partial(
        task_id="create_yml_files_in_S3",
        aws_conn_id="aws_conn",
        data="This is a yml file.",
        replace=True
    ).expand(s3_key=yml_keys)


    # list all files in the first S3 Bucket
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

    [create_txt_files_in_S3, create_yml_files_in_S3] >> list_files_S3
