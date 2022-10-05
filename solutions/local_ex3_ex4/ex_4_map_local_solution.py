from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from os import listdir
from os.path import isfile, join
import shutil

with DAG(
    dag_id="ex_4_expand_kwargs_dag_local_solution",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    tags=["exercise_4", "dynamic_tasks", "solution", "local"],
    catchup=False
):

    # this creates lists of sample file names
    txt_keys = []
    yml_keys = []
    for i in range(10):
        txt_keys.append(f"include/ex_4/{i}.txt")
        yml_keys.append(f"include/ex_4/{i}.yml")

    @task 
    def write_txt_files(key):
        f = open(key, "w")
        f.close()

    @task 
    def write_yml_files(key):
        f = open(key, "w")
        f.close()

    @task
    def list_files_locally():
        filenames = [
            f for f in listdir("include/ex_4") 
            if isfile(join("include/ex_4", f))
        ]
        return filenames

    txt_created = write_txt_files.partial().expand(key=txt_keys)
    yml_created = write_yml_files.partial().expand(key=yml_keys)

    def create_copy_arg(filename):
        if filename.rsplit(".", 1)[-1] == "txt":
            return (f"include/ex_4/{filename}", f"include/ex_4/txt/{filename}")
        elif filename.rsplit(".", 1)[-1] == "yml":
            return (f"include/ex_4/{filename}", f"include/ex_4/yml/{filename}")
        else:
            raise AirflowSkipException(f"Skip moving {filename}, unknown filetype")

    @task
    def copy_files(src_dst):
        shutil.copyfile(src_dst[0], src_dst[1])

    local_files = list_files_locally()

    [txt_created, yml_created] >> local_files

    mapped_task = copy_files.partial().expand(src_dst=local_files.map(create_copy_arg))

    