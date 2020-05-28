import json
import pathlib
import airflow.utils.dates
import requests
import airflow 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator 
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook

from datetime import timedelta
from datetime import datetime as dt 
import MySQLdb
import logging 
import os,sys
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from pathlib import Path 
import time 
from airflow.models import Variables
# import yaml for file configuration
import yaml
from pprint import pprint 

logging.basicConfig(format= '%(asctime)s - %(message)s', datefmt='[%H:%M:%S]')
logger= logging.getLogger()
logger.setLevel(logging.INFO)

API_KEY = 'cd3465baaf7330bdd87969e8d733f734' 
HOST ='localhost'
DB ='airflow_test'
USER ='root'
PSWD ='password'
TABLE ='city_weather'