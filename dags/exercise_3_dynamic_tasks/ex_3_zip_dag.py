from airflow import DAG, XComArg, Dataset
from airflow.decorators import task
from datetime import datetime
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Add your S3 bucket
S3_BUCKET = "Your S3 bucket"

dataset_exercise_3 = Dataset(f"s3://{S3_BUCKET}/ex3")

with DAG(
    dag_id="ex_3_zip_dag",
    start_date=datetime(2022, 10, 1),
    schedule=[dataset_exercise_3],
    tags=["exercise_3", "dynamic_tasks", "task"],
    catchup=False
):

    list_files_S3_bucket = S3ListOperator(
        task_id="list_files_S3_bucket",
        aws_conn_id="aws_conn",
        bucket=S3_BUCKET
        # if the ex 3 files are in a seperate folder you might want to use
        # the prefix parameter here
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


    # This task should map over the file names and the file contents
    # using dynamic task mapping
    @task 
    def write_new_files(args):
        
        old_file_name="ADD CODE HERE" # provide the mapped file name
        file_content="ADD CODE HERE" # provide the mapped file content

        # Don't change the code below
        protagonist=file_content.split(" ")[0]

        new_file_name=protagonist + "_" + old_file_name

        f = open(f"include/ex_3/solutions/{new_file_name}", "w")
        f.write(f"On {old_file_name[:-4]} {file_content}")
        f.close()

    # call write_new_files with the right input here


