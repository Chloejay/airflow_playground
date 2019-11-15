from config import * 
  
default_args = {
    'owner': 'cj',
    'start_date': datetime(2019, 11, 10),
    # 'depends_on_past': False,
    'email': ['ji.jie@edhec.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4
}

dag = DAG(
    dag_id="airflow_test",
    description="Download rocket pictures from public api for airlfow learning.",
    # start_date=airflow.utils.dates.days_ago(14),
    default_args=default_args,
    schedule_interval="@daily"
)

download = BashOperator(
    task_id="download",
    bash_command="curl -o /tmp/launches.json 'https://launchlibrary.net/1.4/launch?next=5&mode=verbose'",
    dag=dag,
)

def _get_pictures():
    pathlib.Path('tmp/images').mkdir(parents=True, exist_ok=True) 
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["rocket"]["imageURL"] for launch in launches["launches"]]
        for image_url in image_urls:
            response = requests.get(image_url)
            image_filename = image_url.split("/")[-1]
            target_file = f"tmp/images/{image_filename}"
            with open(target_file, "wb") as f:
                f.write(response.content)
            print(f"Downloaded {image_url} to {target_file}")

get_pictures = PythonOperator(
    task_id="get_pictures", 
    python_callable=_get_pictures, 
    dag=dag)

notify = BashOperator(
    task_id="notify", 
    bash_command='echo "There are now $(ls tmp/images | wc -l) images."', 
    dag=dag
)
opr_email = EmailOperator(
        task_id='send_notify_email',
        to='ji.jie@edhec.com',
        subject='airflow test case',
        html_content=""" <h6>image download is DONE!</h6> """,
        dag=dag
    )

download >> get_pictures >> notify >> opr_email 