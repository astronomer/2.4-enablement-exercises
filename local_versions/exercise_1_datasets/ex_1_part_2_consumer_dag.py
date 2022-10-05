"""Schedule this DAG on Datasets."""

from airflow import DAG, Dataset, XComArg
from airflow.decorators import task
import requests
from datetime import datetime
from os import listdir
from os.path import isfile, join

API = "http://numbersapi.com/"

with DAG(
    dag_id="ex_1_part_2_consumer_dag_local",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    tags=["exercise_1", "datasets", "task", "ex_1_part_2", "local"],
    catchup=False
):

    @task
    def list_files_locally():
        filenames = [
            f for f in listdir("include/ex_1") 
            if isfile(join("include/ex_1", f))
        ]
        return filenames

    @task 
    def read_files_locally(filenames):
        num_sum = 0
        for filename in filenames:
            f = open(f"include/ex_1/{filename}", "r")
            file_content = f.read()
            num_sum += int(file_content)
            f.close()
        return num_sum

    @task
    def get_a_number_fact(num_sum):
        print(str(num_sum))
        r = requests.get(API + str(num_sum) + "/math")
        return r.text

    get_a_number_fact(read_files_locally(list_files_locally()))