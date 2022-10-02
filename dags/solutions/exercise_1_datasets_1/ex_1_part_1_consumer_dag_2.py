from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime

# added dataset
ex_1_activity_dataset = Dataset('file://localhost/airflow/include/activity.txt')

with DAG(
    dag_id="ex_1_part_1_consumer_dag_2_solution",
    start_date=datetime(2022, 10, 1),
    schedule=[ex_1_activity_dataset], # scheduled on the dataset
    tags=["exercise_1", "datasets", "solution", "ex_1_part_1"],
    catchup=False
):

    @task
    def return_activity_count():
        f = open("include/activity.txt", "r")
        num_of_activities = len(f.readlines())
        f.close()
        return num_of_activities

    return_activity_count()