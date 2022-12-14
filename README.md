## Enablement session: New Airflow features in 2.4

### Instructions

This repository contains 4 exercises about new Airflow features in version 2.4. For some exercises a connection to AWS (`aws_conn`) with an S3 bucket existing is needed. Exercise 4 uses 3 seperate S3 buckets. If you don't have an S3 bucket available use the dags in the folder `local_versions` for local versions of the same concepts.

Possible solutions are shown in the `solutions` folder, there often are several ways to solve the task.

### Resources

Feel free to use the following resources:
- [Datasets and Data-Aware Scheduling in Airflow guide](https://www.astronomer.io/guides/airflow-datasets/)
- [Dynamic Tasks in Airflow guide](https://www.astronomer.io/guides/dynamic-tasks/)
- [Offical docs on Data-aware Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html)
- [Official docs on Dynamic Task mapping](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dynamic-task-mapping.html)

### Exercise 1 - Datasets

#### Part 1 - Break up a monolithic DAG.

In `dags/exercise_1_datasets` you will find a DAG called `monolithic_dag` (tagged with `ex_1_part_1`) gathering weekend activities for you.

> Task: Break the monolithic DAG up into 2 (or more) seperate DAGs using Datasets.

#### Part 2 - Schedule a DAG on two Datasets.

In `dags/exercise_1_datasets` you will find the following tags (tagged `ex_1_part_2`):

- `ex_1_part_2_fast_scheduled_producer_DAG`: A DAG scheduled every 2 minutes writing a random number to a file in an S3 Bucket.
- `ex_1_part_2_slow_scheduled_producer_dag`: A DAG scheduled to run every 5 minutes writing your estimated age (based on your name) to a file in an S3 Bucket.
- `ex_1_part_2_consumer_dag`: A DAG that should only run after both files in the S3 Bucket were updated to get a math fact!

> Task: Use datasets to schedule the dependencies between the DAGs. What happens when the faster producer DAG runs twice before the slower producer DAG runs?

### Exercise 2 - Datasets

In `dags/exercise_2_datasets` there are 4 DAGs (tagged `exercise_2`):

- `ex_2_getting_animal_pictures`: A DAG gathering links to images of dogs and cats in local files.
- `ex_2_getting_data_from_S3`: A DAG reading your estimated age you wrote to the S3 bucket in exercise 1.
- `ex_2_wait_for_cats_dag`: A DAG with 1 ExternalTaskSensor that waits for a new link to a cat image to be written to the local file.
- `ex_2_wait_for_animals_and_fetch_age_dag`: A DAG with 3 branches depending using 3 ExternalTaskSensors.

> Task: Replace all ExternalTaskSensors with Datasets. How does using Datasets change the functionality of the DAG?

### Exercise 3 - Dynamic Task Mapping - expand kwargs and zip

In `dags/exercise_3_dynamic_tasks` there are two DAGs (one scheduled on the other using a Dataset), tagged with `exercise_3`. In `include/ex_3` there are several text files named with a date that contain sentences starting with a "protagonist".

- `ex_3_upload_files` is an incomplete DAG which is supposed to upload all files in `include/ex_3/` to an S3 bucket using dynamic task mapping over the `LocalFilesystemToS3Operator`. 
- `ex_3_zip_dag` is the incomplete downstream DAG reading the file names and their contents and writing a modified version of the files to `include/ex_3/solutions/`. 

> Task: Complete the DAGs using dynamic task mapping. `ex_3_upload_files` should have one mapped task for each pair of `filename` and `dest_key` and the `write_new_files` task in the `ex_3_zip_dag` should have one mapped task for each pair of `old_file_name` and `file_content`.

### Exercise 4 - Dynamic Task Mapping - map and expand kwargs

In `dags/exercise_4_dynamic_tasks` there is a DAG which is creating several yml and txt files in an S3 bucket.

> Task: Use dynamic task mapping over the S3CopyObjectOperator to sort the files by filetype into a "yml bucket" and a "txt bucket". The goal is to have one dynamically mapped task combination of `source_bucket_key` and `dest_bucket_key`.

Hint: Write a function to use with `map` to help you sort the files. A very similar use case can be found in the "Transforming mapped data" section of the offical documentation.
