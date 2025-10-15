from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

CSV_PATH = '/opt/airflow/data/users.csv'
TABLE = 'public.users'
CONN = dict(host='postgres', dbname='airflow', user='airflow', password='airflow', port=5432)

def create_table():
    conn = psycopg2.connect(**CONN)
    conn.autocommit = True
    with conn, conn.cursor() as cur:
        # Drop the table first to ensure correct column structure
        cur.execute(f"DROP TABLE IF EXISTS {TABLE};")
        cur.execute(f"""
            CREATE TABLE {TABLE} (
                id INTEGER,
                name TEXT,
                email TEXT,
                age INTEGER,
                signup_date DATE,
                country TEXT
            );
        """)

def load_csv():
    conn = psycopg2.connect(**CONN)
    conn.autocommit = True
    with conn, conn.cursor() as cur, open(CSV_PATH, 'r', encoding='utf-8') as f:
        # Empty the table and load fresh data
        cur.execute(f'TRUNCATE {TABLE};')
        cur.copy_expert(
            f"""COPY {TABLE} (id, name, email, age, signup_date, country)
                FROM STDIN WITH (FORMAT csv, HEADER true)""",
            f
        )

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='dagselt_pipeline_postgres',
    description='Load users.csv into Postgres metadata DB (6-column schema)',
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,   # for Airflow 2.2.x
    catchup=False,
    default_args=default_args,
    tags=['lab6', 'elt', 'postgres'],
) as dag:

    t1 = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    t2 = PythonOperator(
        task_id='load_csv',
        python_callable=load_csv
    )

    t1 >> t2
