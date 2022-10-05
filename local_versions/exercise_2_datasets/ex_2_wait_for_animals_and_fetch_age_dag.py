from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
import numpy as np

with DAG(
    dag_id="ex_2_wait_for_animals_and_fetch_age_dag_local",
    start_date=datetime(2022, 10, 1),
    schedule="@daily",
    tags=["exercise_2", "datasets", "task", "local"],
    catchup=False
):

    # replace the external task sensors with datasets
    wait_for_dogs = ExternalTaskSensor(
        task_id="wait_for_dogs",
        external_dag_id="ex_2_getting_animal_pictures_local",
        external_task_id="write_dog_to_file"
    )

    wait_for_age_data = ExternalTaskSensor(
        task_id="wait_for_age_data",
        external_dag_id="ex_2_getting_data_from_local",
        external_task_id="write_age_to_local_file"
    )

    wait_for_cats = ExternalTaskSensor(
        task_id="wait_for_cats",
        external_dag_id="ex_2_getting_animal_pictures_local",
        external_task_id="write_cat_to_file"
    )

    with TaskGroup(group_id="cat_image") as cat_image:

        @task 
        def read_cat_from_file():
            f = open("include/cats.txt", "r")
            newest_cat = f.readlines()[-1]
            f.close()
            return newest_cat

        log_the_cat = BashOperator(
            task_id="log_the_cat",
            bash_command="sleep 5 && echo 'There is a new cat!'"
        )

        read_cat_from_file() >> log_the_cat

    with TaskGroup(group_id="dog_image") as dog_image:

        @task 
        def read_dog_from_file():
            f = open("include/dogs.txt", "r")
            newest_dog = f.readlines()[-1]
            f.close()
            return newest_dog

        log_the_dog = BashOperator(
            task_id="log_the_dog",
            bash_command="sleep 5 && echo 'There is a new dog!'"
        )

        read_dog_from_file() >> log_the_dog

    with TaskGroup(group_id="dog_age") as dog_age:

        @task 
        def read_age_from_file():
            f = open("include/estimated_age.txt", "r")
            age = f.read()
            f.close()
            return int(age)

        @task
        def calculate_age_in_dog_years(age):
            your_age_in_dog_years = np.exp((age - 31) / 16)
            # source: https://www.akc.org/expert-advice/health/how-to-calculate-dog-years-to-human-years

            return f"You are {your_age_in_dog_years} years old in dog years."

        calculate_age_in_dog_years(read_age_from_file())

    wait_for_dogs >> dog_image
    wait_for_age_data >> dog_age
    wait_for_cats >> cat_image