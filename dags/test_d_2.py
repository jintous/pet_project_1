from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import string
import csv
import os

DATA_DIR = "/opt/airflow/data"
CSV_PATH = f"{DATA_DIR}/example.csv"

os.makedirs(DATA_DIR, exist_ok=True)

def generate_random_text(ti, **context):
    text = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    ti.xcom_push(key="random_text", value=text)


def write_to_csv(ti, **context):

    text = ti.xcom_pull(
        key="random_text",
        task_ids="generate_random_text"
    )

    file_exists = os.path.isfile(CSV_PATH)

    with open(CSV_PATH, mode="a", newline="") as f:
        writer = csv.writer(f)

        # если файл новый — пишем заголовок
        if not file_exists:
            writer.writerow(["text"])

        writer.writerow([text])


def duplicate_last_row(**context):
    rows = []

    with open(CSV_PATH, mode="r") as f:
        reader = csv.reader(f)
        rows = list(reader)

    # защита: если только заголовок
    if len(rows) <= 1:
        return

    last_row = rows[-1]

    with open(CSV_PATH, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(last_row)


with DAG(
    dag_id="csv_xcom_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['pet'],
) as dag:

    t1 = PythonOperator(
        task_id="generate_random_text",
        python_callable=generate_random_text,
    )

    t2 = PythonOperator(
        task_id="write_to_csv",
        python_callable=write_to_csv,
    )

    t3 = PythonOperator(
        task_id="duplicate_last_row",
        python_callable=duplicate_last_row,
    )

    t1 >> t2 >> t3
