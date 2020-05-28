from config import *  

"a python function to call, PythonOperator callable"
def _load(): 
    mysql_hook = MySqlHook(mysql_conn_id ='mysql_db')
    fileName = str(dt.now().date())+'.json'
    print(os.getcwd()) 
    tot = os.path.join(os.getcwd(), fileName)

    with open(tot, 'r') as input:
        data=json.load(input)

    city = str(data['name'])
    country = str(data['sys']['country']) 
    lat = float(data['coord']['lat']) 
    lon = float(data['coord']['lon'])
    humid = float(data['main']['humidity']) 
    press = float(data['main']['pressure'])
    min_temp = float(data['main']['temp_min'])-273.15
    max_temp = float(data['main']['temp_max'])-273.15
    temp = float(data['main']['temp'])-273.15
    weather = str(data['weather'][0]['description']) 

    valid_data = True
    list_= [lat, lon, humid, press, min_temp, max_temp, temp]
    for valid in np.isnan(list_):
        if valid is False:
            valid_data = False 
            break

    row = (city,country, lat, lon, humid, press, min_temp,max_temp, temp, weather)

    query = """insert into city_weather (city, country, latitude, longitude, humudity, pressure, min_temp, 
    max_temp, temp, weather) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    if valid_data is True:
        mysql_hook.run(query, parameters = row) 
        
default_args = {
    'owner':'cj',
    'depends_on_past':False,
    'email':['ji.jie@edhec.com'],
    'email_on_retry':False,
    'retries':5,
    'retry_delay':timedelta(minutes=4)
    }

dag = DAG(#init DAG object
    dag_id = 'getWeather_flow', #name of DAG
    default_args = default_args,
    start_date = dt(2019,11,10),
    end_date= dt(2020,5,10),
    schedule_interval = "@daily",
    template_searchpath="/tmp",
)

api_call= BashOperator(
    task_id ='get_weather',
    bash_command ='Python ~/airflow/dags/getdata.py',
    dag = dag #reference to DAG variable
)

create_database= BashOperator(
    task_id='create_database', 
    bash_command='Python ~/airflow/dags/sql.py', 
    dag= dag
)

# use Hook to communicate with
load_data= PythonOperator( #PythonOperator
    task_id='transform_load',
    python_callable= _load, #point to python function to execute
    provide_context= True,
    # op_args=""
    # op_kwargs= {"":""}
    pool = "dag_pool",
    dag = dag
)

# or directly use MySqlOperator
write_to_mysql= MySqlHook(
    task_id="write_to_mysql",
    # connection, identifier holding the credentials to MySQL database;
    mysql_conn_id="",
    sql= "etl.sql", #TODO
    dag=dag,
)

"""Can use Airflow CLI to add database connection,
airflow connections --add \
--conn_id etl_mysql \
--conn_type mysql \
--conn_host localhost \
--conn_login root \
--conn_password password
"""

# set order of execution of tasks
api_call >> create_database >> load_data 
# or use mysql hook
api_call >> create_database >> write_to_mysql 