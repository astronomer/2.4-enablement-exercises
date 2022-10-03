from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="ex_2_wait_for_cats_dag",
    start_date=datetime(2022, 10, 1),
    schedule="@daily",
    tags=["exercise_2", "datasets", "task"],
    catchup=False
):

    # replace the ExternalTaskSensor with datasets
    wait_for_cats = ExternalTaskSensor(
        task_id="wait_for_cats",
        external_dag_id="ex_2_getting_animal_pictures",
        external_task_id="write_cat_to_file"
    )

    @task 
    def read_cat_from_file():
        f = open("include/cats.txt", "r")
        newest_cat = f.readlines()[-1]
        f.close()
        return newest_cat

    wait_for_cats >> read_cat_from_file()