from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ex_3_map_kwargs_local_solution",
    start_date=datetime(2022, 10, 1),
    schedule=None,
    tags=["exercise_3", "dynamic_tasks", "solution", "local"],
    catchup=False
):

    # This task creates a set of keyword arguments
    @task
    def create_kwargs():
        commands_and_words = [
            {
                "bash_command": "echo $word",
                "env": {"word": "hello"}
            },
            {
                "bash_command": "echo `expr length $WORD`",
                "env": {"word": "bye"}
            }
        ]
        return commands_and_words

    # this task should use dynamic task mapping, iterate over different
    # env and bash_commands in pairs
    upload_files = BashOperator.partial(
        task_id="say_the_words"
    ).expand_kwargs(create_kwargs())