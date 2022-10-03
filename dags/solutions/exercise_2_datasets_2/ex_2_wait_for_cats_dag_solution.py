from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor

# Added dataset
cats_dataset = Dataset("file://localhost/airflow/include/cats.txt")

with DAG(
    dag_id="ex_2_wait_for_cats_dag_solution",
    start_date=datetime(2022, 10, 1),
    schedule=[cats_dataset],
    tags=["exercise_2", "datasets", "solution"],
    catchup=False
):

    @task 
    def read_cat_from_file():
        f = open("include/cats.txt", "r")
        newest_cat = f.readlines()[-1]
        f.close()
        return newest_cat

    read_cat_from_file()