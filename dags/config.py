import json
import pathlib
import airflow.utils.dates
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator 
from datetime import timedelta, datetime
