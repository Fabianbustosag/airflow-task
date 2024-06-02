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

# Crear las DAGs
# dag_ids = ['dag1', 'dag2', 'dag3']
# inicio_values = [0, 5, 10]  # Valores de inicio diferentes para cada DAG

# for dag_id, inicio in zip(dag_ids, inicio_values):
#     with DAG(
#         dag_id,
#         default_args=dag_args,
#         description=f'Parallel DAG {dag_id}',
#         schedule_interval=timedelta(days=1),
#         start_date=datetime(2023, 1, 1),
#         catchup=False,
#     ) as dag:
        
#         # Definir el PythonOperator
#         run_funcion_paralela = PythonOperator(
#             task_id='run_funcion_paralela',
#             python_callable=airflow_task,
#             op_kwargs={'inicio': inicio, 'leer': 5},  # Ajusta los parámetros según sea necesario
#         )

#         globals()[dag_id] = dag


#-------------------------

dag_test = DAG(
    "dag_test",
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
    dag=dag_test
)

t1 = PythonOperator(
    task_id='funcion_paralela1',
    python_callable=task_etl,
    dag=dag_test,
    op_kwargs={'start_offset': 0},
)

t2 = PythonOperator(
    task_id='funcion_paralela2',
    python_callable=task_etl,
    dag=dag_test,
    op_kwargs={'start_offset': 5},
)

t3 = PythonOperator(
    task_id='funcion_paralela3',
    python_callable=task_etl,
    dag=dag_test,
    op_kwargs={'start_offset': 10},
)

t0 >> [ t1,t2,t3 ]

