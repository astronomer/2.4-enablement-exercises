"""Make this DAG into a DAG with a producing task."""

from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
import requests

# Add your S3 Bucket and name
YOUR_NAME = "YOURNAME"

API = f"https://api.agify.io/?name={YOUR_NAME}"

with DAG(
    dag_id="ex_1_part_2_slow_scheduled_producer_dag_local",
    start_date=datetime(2022, 10, 1),
    schedule="*/5 * * * *",
    tags=["exercise_1", "datasets", "task", "ex_1_part_2", "local"],
    catchup=False
):

    @task
    def get_age_estimate():
        r = requests.get(API)
        return r.json()["age"]

    @task
    def create_object_locally(age_estimate):
        f = open("include/ex_1/age_estimate.txt", "w")
        f.write(f"{age_estimate}")
        f.close()

    create_object_locally(get_age_estimate())
