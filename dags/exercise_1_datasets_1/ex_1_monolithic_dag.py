"""Break up this monolithic DAG into at least 2 smaller DAGs. Use datasets."""

from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import requests
import numpy as np

API = "https://www.boredapi.com/api/activity"

with DAG(
    dag_id="ex_1_monolithic_dag",
    start_date=datetime(2022, 10, 1),
    schedule="@daily",
    tags=["exercise_1", "datasets", "task"],
    catchup=False
):

    @task
    def get_activity():
        r = requests.get(API)
        return r.json()

    @task 
    def write_activity_to_file(response):
        f = open("include/activity.txt", "a")
        f.write(f"{response['activity']}\n")
        f.close()

    @task 
    def read_activity_from_file():
        f = open("include/activity.txt", "r")
        lines = f.readlines()
        f.close()
        chosen_activity = np.random.choice(lines, 1)[0]
        return "Today you will: " + chosen_activity

    @task
    def return_activity_count():
        f = open("include/activity.txt", "r")
        num_of_activities = len(f.readlines())
        f.close()
        return num_of_activities

    write_activity_to_file(get_activity()) >> [
        read_activity_from_file(),
        return_activity_count()
    ]
