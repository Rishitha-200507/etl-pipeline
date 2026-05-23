from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# Extract
def extract():
    data = {
        "name": ["Krish", "Ram", "Sam"],
        "age": [21, 22, 23]
    }

    df = pd.DataFrame(data)

    df.to_csv("/tmp/data.csv", index=False)

    print("Data Extracted")


# Transform
def transform():
    df = pd.read_csv("/tmp/data.csv")

    df["age"] = df["age"] + 1

    df.to_csv("/tmp/transformed_data.csv", index=False)

    print("Data Transformed")


# Load
def load():
    df = pd.read_csv("/tmp/transformed_data.csv")

    engine = create_engine(
        "postgresql://etl_user:etl_pass@postgres_etl:5432/etl_db"
    )

    df.to_sql("users", engine, if_exists="replace", index=False)

    print("Data Loaded into PostgreSQL")


with DAG(
    dag_id="simple_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    extract_task >> transform_task >> load_task