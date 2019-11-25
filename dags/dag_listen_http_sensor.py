import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python_operator import PythonOperator

schedule = timedelta(seconds=5)

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

def new_file_detection(**kwargs):
 print("Was able to ping google.com")
 
http_sensor = HttpSensor(
 task_id='http_sensor_task',
 http_conn_id='http_default',
 endpoint='',
 method='GET',
 request_params=None,
 headers=None,
 response_check=None,
 extra_options=None,
 poke_interval=1, # (seconds); checking file every half an hour
 timeout=60 * 30, # timeout in 12 hours
 dag=dag)
 
print_message = PythonOperator(task_id='print_message',
 provide_context=True,
 python_callable=new_file_detection,
 dag=dag)
 
http_sensor >> print_message