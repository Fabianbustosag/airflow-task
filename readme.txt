export AIRFLOW_HOME=/home/fabian/Desktop/airflow

airflow standalone

{'host': '10.33.195.214', 'database': 'pmd', 'user': 'admin', 'password': 'admin', 'port': '5432'}
-------------------------------------
si quiero tener 3 funciones iguales a esta 1 que cada funcion lea 5 registros paralelamente, es decir la func1, lee del 0-4, la func2 del 5-9,la func3 del 10-14, la func1 del 15-19 y asi sucesivamente

-------------------------------------
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

def my_python_function(**kwargs):
    return { "ok": 1 }

def my_python_function2(**kwargs):
    return { "ok": 2 }

def my_python_function3(**kwargs):
    return { "ok": 3 }


dag_args={
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'on_skipped_callback': another_function, #or list of functions
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    "dag1",
    description="A simple tutorial DAG",
    default_args=dag_args,
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"], # para agrupar workflows
) 

dag = DAG(
    "dag2",
    description="A simple tutorial DAG",
    default_args=dag_args,
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"], # para agrupar workflows
) 

dag = DAG(
    "dag2",
    description="A simple tutorial DAG",
    default_args=dag_args,
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"], # para agrupar workflows
) 

def procesar_regs():
    return None

t1 = PythonOperator(
    task_id='run_python_function',
    python_callable=my_python_function,
    dag=dag
)

# t2 = PythonOperator(
#     task_id='run_python_function2',
#     python_callable=my_python_function2,
#     dag=dag
# )

# t3 = PythonOperator(
#     task_id='run_python_function3',
#     python_callable=my_python_function3,
#     dag=dag
# )

# t4 = BashOperator(
#     task_id="print_date",
#     bash_command="date",
#     dag=dag
# )


# t1 >> [ t3,t2,t4]


--------------

dag_args={
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'on_skipped_callback': another_function, #or list of functions
    # 'trigger_rule': 'all_success'
}

dag1 = DAG(
    "dag1",
    description="DAG ETL",
    default_args=dag_args,
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"], # para agrupar workflows
) 



def test():
    return { "ok": 1 }


from etl import extract_data, etl_function, funcion_paralela


def airflow_task(**kwargs):
    # Supongamos que el DataFrame se obtiene de algún lugar, por ejemplo, una lectura desde un archivo CSV
    df = extract_data()  # Reemplaza esto con la fuente de tu DataFrame
    
    # Parámetros de la función
    inicio = kwargs.get('inicio', 0)
    leer = kwargs.get('leer', 5)
    
    result = funcion_paralela(df, inicio, leer)
    
    print(f'airflow_task filas: {result.shape[0]}, head: {result.head(5)}')


t1 = PythonOperator(
    task_id='t1',
    python_callable=airflow_task,
    op_kwargs={'inicio': 0, 'leer': 5}, 
    dag=dag1
)