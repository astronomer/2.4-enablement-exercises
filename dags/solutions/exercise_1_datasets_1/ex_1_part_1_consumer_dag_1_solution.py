from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
import numpy as np

# added dataset
ex_1_activity_dataset = Dataset('file://localhost/airflow/include/activity.txt')

with DAG(
    dag_id="ex_1_part_1_consumer_dag_1_solution",
    start_date=datetime(2022, 10, 1),
    schedule=[ex_1_activity_dataset], # scheduled on the dataset
    tags=["exercise_1", "datasets", "solution", "ex_1_part_1"],
    catchup=False
):

    @task 
    def read_activity_from_file():
        f = open("include/activity.txt", "r")
        lines = f.readlines()
        f.close()
        chosen_activity = np.random.choice(lines, 1)[0]
        return "Today you will: " + chosen_activity

    read_activity_from_file()