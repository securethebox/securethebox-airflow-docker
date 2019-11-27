import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python_operator import PythonOperator
from controllers.airflow_controller import AirflowController

schedule = timedelta(seconds=60)

args = {
 'owner': 'airflow',
 'start_date': airflow.utils.dates.days_ago(0),
 'depends_on_past': False,
}

dag = DAG(
 dag_id='http_sensor_demo_dag',
 schedule_interval=schedule, 
 default_args=args
)

def create_http_connection():
    ac = AirflowController()
    ac.createHttpConnection("securethebox", "https://securethebox.us")

def t2_error_task(**context):
    instance = context['task_instance']
    print("Failed...",instance)

http_sensor = HttpSensor(
    task_id='http_sensor_task',
    http_conn_id='securethebox',
    endpoint='',
    method='GET',
    request_params=None,
    headers=None,
    response_check=False,
    extra_options=None,
    poke_interval=1, # (seconds); checking site every 5 seconds
    timeout=30, # timeout in 1 minute
    on_failure_callback=t2_error_task,
    dag=dag)

def printMessage(**context):
    xcomdata = context['task_instance'].xcom_pull(task_ids='http_sensor_task')
    print("print", xcomdata)
    
print_message = PythonOperator(task_id='print_message',
 python_callable=printMessage,
 provide_context=True,
 dag=dag)
 

create_sensor = PythonOperator(task_id='create_sensor',
    python_callable=create_http_connection,
    dag=dag)

create_sensor >> http_sensor >> print_message