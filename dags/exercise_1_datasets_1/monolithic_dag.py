from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests

API = "https://www.boredapi.com/api/activity"

with DAG(
    dag_id="monolithic_dag",
    start_date=datetime(2022, 10, 1),
    schedule="@daily",
    tags=["exercise_1", "datasets", "task"]
) as dag:

    @task
    def get_activity():
        r = requests.get(API)
        return r.json()

    @task 
    def write_activity_to_file(response):
        f = open("include/activity.txt", "a")
        f.write(f"Today you will: {response['activity']}")
        f.close()

    @task 
    def read_activity_from_file():
        f = open("include/activity.txt", "r")
        print(f.read(5))
        f.close()

    write_activity_to_file(get_activity()) >> read_activity_from_file()