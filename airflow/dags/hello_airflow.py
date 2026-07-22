from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def print_message():
    print("Airflow is working successfully!")


with DAG(
    dag_id="hello_airflow",
    description="My first Airflow DAG",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
) as dag:

    start = EmptyOperator(task_id="start")

    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=print_message,
    )

    end = EmptyOperator(task_id="end")

    start >> say_hello >> end