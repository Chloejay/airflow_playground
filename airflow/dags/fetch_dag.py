from config import *  

"a python function to call, PythonOperator callable"
def _load(): 
    mysql_hook = MySqlHook(mysql_conn_id ='mysql_db')
    fileName = str(dt.now().date())+'.json'
    # print(os.getcwd()) 
    tot = os.path.join(os.getcwd(), fileName)

    with open(tot, 'r') as input_data:
        data=json.load(input_data)

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

    query = """insert into city_weather (city, country, latitude, longitude, humidity, pressure, min_temp, 
    max_temp, temp, weather) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    if valid_data is True:
        mysql_hook.run(query, parameters = row) 
        
default_args = {
    'owner':'chloeji',
    'depends_on_past':False,
    'email':['ji.jie@edhec.com'],
    'email_on_retry':False,
    'retries':5,
    'retry_delay':timedelta(minutes=4)
    }

dag = DAG(
    dag_id = 'getWeather_flow',
    default_args = default_args,
    start_date = dt(2019,11,10),
    end_date= dt(2020,5,10),
    schedule_interval = "@daily",
    template_searchpath="/tmp",
)

api_call= BashOperator(
    task_id ='get_weather',
    bash_command ='Python ~/airflow/dags/getdata.py',
    dag = dag
)

create_database= BashOperator(
    task_id='create_database', 
    bash_command='Python ~/airflow/dags/sql.py', 
    dag= dag
)

# use Hook to load data to persisted database
load_data= PythonOperator(
    task_id='transform_load',
    python_callable= _load, 
    provide_context= True,
    # op_kwargs= {"":""} #no params in `_load` func for this case; 
    pool = "dag_pool",
    dag = dag
)

write_to_mysql= MySqlOperator(#under the hook, MysqlHook do the hard work;
    task_id="write_to_mysql",
    # connection, identifier holding the credentials to MySQL database;
    mysql_conn_id="etl_mysql",
    sql= "etl.sql", #TODO, tempatable script with jinja
    dag=dag,
)

"""Can use Airflow CLI to add database connection instead of to use UI,
airflow connections --add \
--conn_id etl_mysql \
--conn_type mysql \
--conn_host localhost \
--conn_login root \
--conn_password password

#Successfully added `conn_id`=etl_mysql : mysql://root:password@localhost:
"""

# set order of execution of tasks
api_call >> create_database >> load_data 
# or
# api_call >> create_database >> write_to_mysql