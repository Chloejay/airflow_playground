import json
import pathlib
import airflow.utils.dates
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator 
from airflow.hooks.mysql_hook import MySqlHook
from datetime import timedelta
from datetime import datetime as dt 
import MySQLdb
import logging 
import os,sys
import requests
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


API_KEY= 'cd3465baaf7330bdd87969e8d733f734' 
