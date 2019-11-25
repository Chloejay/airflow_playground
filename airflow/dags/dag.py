from config import * 

def load(ds, **kwargs):
    mysql_hook= MySqlHook(mysql_conn_id='mysql_db')
    file_name=str(dt.now().date())+'.json'
    tot_name=os.path.join(os.path.dirname(__file__),'data', file_name) 

    with open(tot_name, 'r') as inputfile:
        data=json.load(inputfile)

    city = str(data['name'])
    country = str(data['sys']['country']) 
    lat = float(data['coord']['lat']) 
    lon = float(data['coord']['lon'])
    humid = float(data['main']['humidity']) 
    press = float(data['main']['pressure'])
    min_temp = float(data['main']['temp_min'])-273.15
    max_temp =float(data['main']['temp_max'])-273.15
    temp = float(data['main']['temp'])-273.15
    weather = str(data['weather'][0]['description']) 

    valid_data=True
    list_=[lat, lon, humid, press, min_temp, max_temp, temp]
    for valid in np.isnan(list_):
        if valid is False:
            valid_data= False 
            break;

    row= (city,country, lat, lon, humid, press, min_temp,max_temp, temp, weather)

    insert_query="""insert into city_weather (city, country, latitude, longitude, humudity, pressure, min_temp, 
    max_temp, temp, weather) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    if valid_data is True:
        mysql_hook.run(insert_query, parameters= row) 
        
default_args={
    'owner':'cj',
    'depends_on_past':False,
    'email':['ji.jie@edhec.com'],
    'email_on_retry':False,
    'retries':5,
    'retry_delay':timedelta(minutes=4)
    }

dag= DAG(
    dag_id='getWeather_flow',
    default_args=default_args, 
    start_date=dt(2019,11,10),
    schedule_interval="@daily",
)

api_call= BashOperator(
    task_id= 'get_weather',
    bash_command='Python ~/airflow/dags/getData.py',
    dag=dag
)

create_database= BashOperator(
    task_id='create_database', 
    bash_command='Python ~/airflow/dags/sql.py', 
    dag= dag
)

load_data= PythonOperator(
    task_id='transform_load',
    provide_context= True,
    python_callable= load,
    dag= dag
)

api_call >> create_database >> load_data 

