from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# from dags.etl1 import airflow_task
from etl import task_etl

dag_args={
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dag",
    description="A simple tutorial DAG",
    default_args=dag_args,
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"], # para agrupar workflows
) 

def test():
    return { "ok": 1 }

t0 = PythonOperator(
    task_id='inicio_funcion_paralela',
    python_callable=test,
    dag=dag
)

t1 = PythonOperator(
    task_id='funcion_paralela1',
    python_callable=task_etl,
    dag=dag,
    op_kwargs={'start_offset': 0},
)

t2 = PythonOperator(
    task_id='funcion_paralela2',
    python_callable=task_etl,
    dag=dag,
    op_kwargs={'start_offset': 5},
)

t3 = PythonOperator(
    task_id='funcion_paralela3',
    python_callable=task_etl,
    dag=dag,
    op_kwargs={'start_offset': 10},
)

t0 >> [ t1,t2,t3 ]

