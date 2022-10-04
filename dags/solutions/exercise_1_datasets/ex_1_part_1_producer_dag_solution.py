from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
import requests

API = "https://www.boredapi.com/api/activity"

# added dataset
ex_1_activity_dataset = Dataset('file://localhost/airflow/include/activity.txt')

with DAG(
    dag_id="ex_1_part_1_producer_dag_solution",
    start_date=datetime(2022, 10, 1),
    schedule="@daily",
    tags=["exercise_1", "datasets", "solution", "ex_1_part_1"],
    catchup=False
):

    @task
    def get_activity():
        r = requests.get(API)
        return r.json()

    # turned this task into a producer task
    @task(outlets=[ex_1_activity_dataset]) 
    def write_activity_to_file(response):
        f = open("include/activity.txt", "a")
        f.write(f"{response['activity']}\n")
        f.close()

    write_activity_to_file(get_activity())