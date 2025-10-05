from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def my_function():
    return  " This is a Python function."


with DAG(
    'sampledagname',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),

    },
    description='A simple tutorial DAG',
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example12'],
) as dag:
    t1 = PythonOperator(
        task_id='myfunction',
        python_callable=my_function,
        dag=dag


    )
    t2 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t1 >> t2