import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python_operator import PythonOperator

schedule = timedelta(minutes=5)

args = {
 'owner': 'airflow',
 'start_date': airflow.utils.dates.days_ago(0),
 'depends_on_past': False,
}

dag = DAG(
 dag_id='s3_key_sensor_demo_dag',
 schedule_interval=schedule, 
 default_args=args
)

def new_file_detection(**kwargs):
 print("A new file has arrived in s3 bucket")
 
file_sensor = S3KeySensor(
 task_id='s3_key_sensor_task',
 poke_interval=60 * 30, # (seconds); checking file every half an hour
 timeout=60 * 60 * 12, # timeout in 12 hours
 bucket_key="s3://[bucket_name]/[key]",
 bucket_name=None,
 wildcard_match=False,
 dag=dag)
 
print_message = PythonOperator(task_id='print_message',
 provide_context=True,
 python_callable=new_file_detection,
 dag=dag)
 
file_sensor >> print_message