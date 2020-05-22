from datetime import datetime 
from pprint import pprint 

from airflow import DAG 
from airflow.operator.python_operator import PythonOperator 

dag = DAG(
    dag_id= "print_context", 
    start_date = datetime(2020, 5,20),
    schedule_interval= "@daily"
)

def _print_context(**context):
    pprint(context) 

print_context = PythonOperator(
    task_id = "print_context", 
    python_callable= _print_context,
    provide_context = True, 
    dag = dag 
)

