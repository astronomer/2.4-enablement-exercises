from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
import requests

DOG_API = "https://dog.ceo/api/breeds/image/random"
CAT_API = "https://api.thecatapi.com/v1/images/search"

with DAG(
    dag_id="ex_2_getting_animal_pictures_local",
    start_date=datetime(2022, 10, 1),
    schedule="@daily",
    tags=["exercise_2", "datasets", "task", "local"],
    catchup=False
):

    @task
    def get_a_dog():
        r = requests.get(DOG_API)
        return r.json()["message"]

    @task
    def write_dog_to_file(link_to_dog_image):
        f = open("include/dogs.txt", "a")
        f.write(f"{link_to_dog_image}\n")
        f.close()

    @task
    def get_a_cat():
        r = requests.get(CAT_API)
        return r.json()[0]["url"]

    @task
    def write_cat_to_file(link_to_cat_image):
        f = open("include/cats.txt", "a")
        f.write(f"{link_to_cat_image}\n")
        f.close()

    write_dog_to_file(get_a_dog())
    write_cat_to_file(get_a_cat())

