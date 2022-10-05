from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="ex_2_getting_data_from_local",
    start_date=datetime(2022, 10, 1),
    schedule="@daily",
    tags=["exercise_2", "datasets", "task", "local"],
    catchup=False
):

    # this task reads a number created in exercise 1!
    @task
    def get_my_estimated_age():
        f = open(f"include/ex_1/age_estimate.txt", "r")
        file_content = f.read()
        f.close()
        return int(file_content)

    @task 
    def write_age_to_local_file(estimated_age):
        f = open("include/estimated_age.txt", "w")
        f.write(f"{estimated_age}\n")
        f.close()

    write_age_to_local_file(get_my_estimated_age())