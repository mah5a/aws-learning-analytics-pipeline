from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
import boto3


def check_s3():
    print("Checking raw files in S3...")


import boto3
import time

def run_glue():
    glue = boto3.client("glue", region_name="ca-central-1")

    response = glue.start_job_run(
        JobName="oulad-student-features-etl"
    )

    job_run_id = response["JobRunId"]
    print(f"Glue Job started: {job_run_id}")

    while True:
        status = glue.get_job_run(
            JobName="oulad-student-features-etl",
            RunId=job_run_id
        )["JobRun"]["JobRunState"]

        print(f"Current status: {status}")

        if status == "SUCCEEDED":
            print("Glue Job completed successfully.")
            break

        if status in ["FAILED", "STOPPED", "TIMEOUT"]:
            raise Exception(f"Glue Job failed with status: {status}")

        time.sleep(30)


def run_crawler():
    print("Running Glue Crawler...")


def validate_athena():
    print("Running Athena validation query...")


with DAG(
    dag_id="oulad_pipeline",
    description="End-to-End AWS Learning Analytics Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aws", "glue", "athena", "oulad"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_s3_task = PythonOperator(
        task_id="check_s3",
        python_callable=check_s3,
    )

    glue_task = PythonOperator(
        task_id="run_glue_job",
        python_callable=run_glue,
    )

    crawler_task = PythonOperator(
        task_id="run_glue_crawler",
        python_callable=run_crawler,
    )

    athena_task = PythonOperator(
        task_id="validate_athena",
        python_callable=validate_athena,
    )

    end = EmptyOperator(task_id="end")

    start >> check_s3_task >> glue_task >> crawler_task >> athena_task >> end