from airflow import DAG, XComArg, Dataset
from airflow.decorators import task
from datetime import datetime
from os import listdir
from os.path import isfile, join

with DAG(
    dag_id="ex_3_zip_dag_local_solution",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    tags=["exercise_3", "dynamic_tasks", "task", "local"],
    catchup=False
):

    @task
    def list_files_locally():
        filenames = [
            f for f in listdir("include/ex_3") 
            if isfile(join("include/ex_3", f))
        ]
        return filenames

    @task 
    def read_files_locally(filenames):
        file_contents = []
        for filename in filenames:
            f = open(f"include/ex_3/{filename}", "r")
            file_content = f.read()
            file_contents.append(file_content)
            f.close
        return file_contents

    local_filelist = list_files_locally()
    file_contents = read_files_locally(local_filelist)

    zipped_arguments = local_filelist.zip(file_contents)
    
    @task 
    def write_new_files(args):
        
        old_file_name=args[0]
        file_content=args[1]

        protagonist=file_content.split(" ")[0]

        new_file_name=protagonist + "_" + old_file_name

        f = open(f"include/ex_3/solutions/{new_file_name}", "w")
        f.write(f"On {old_file_name[:-5]} {file_content}")
        f.close()

    write_new_files.partial().expand(args=zipped_arguments)


