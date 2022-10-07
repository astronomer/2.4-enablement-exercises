from airflow import DAG
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator, S3CopyObjectOperator, S3CreateObjectOperator
)

S3_BUCKET = "myexamplebucketone"
S3_BUCKET_YML = "myymlbucket"
S3_BUCKET_TXT = "mytxtbucket"

with DAG(
    dag_id="ex_4_expand_kwargs_dag_solution",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    tags=["exercise_4", "dynamic_tasks", "solution"],
    catchup=False
):

    txt_keys = []
    yml_keys = []
    for i in range(10):
        txt_keys.append(f"s3://{S3_BUCKET}/ex4/{i}.txt")
        yml_keys.append(f"s3://{S3_BUCKET}/ex4/{i}.yml")

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

    list_files_S3 = S3ListOperator(
        task_id="list_files_S3",
        aws_conn_id="aws_conn",
        bucket=S3_BUCKET
    )

    # this is the function .map uses to transform the output from list_files_S3
    def create_copy_kwargs(filename):
        if filename.rsplit(".", 1)[-1] == "txt":
            return {
                "source_bucket_key": f"s3://{S3_BUCKET}/ex4/{filename}",
                "dest_bucket_key": f"s3://{S3_BUCKET_TXT}/{filename}"
            }
        elif filename.rsplit(".", 1)[-1] == "yml":
            return {
                "source_bucket_key": f"s3://{S3_BUCKET}/ex4/{filename}",
                "dest_bucket_key": f"s3://{S3_BUCKET_YML}/{filename}"
            }
        else:
            raise AirflowSkipException(f"Skip moving {filename}, unknown filetype")

    # copy kwargs are sets of keyword arguments (plus maybe SkipExceptions)
    copy_kwargs = list_files_S3.output.map(create_copy_kwargs)

    # Use expand_kwargs to map over keyword arguments
    copy_files = S3CopyObjectOperator.partial(
        task_id="copy_files",
        aws_conn_id="aws_conn",
    ).expand_kwargs(copy_kwargs)


    [create_txt_files_in_S3, create_yml_files_in_S3] >> list_files_S3
    list_files_S3 >> copy_files