import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
import pendulum

local_tz = pendulum.timezone("America/Los_Angeles")

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
}

dag = DAG(
    dag_id='dag_save_local_disk',
    default_args=args,
    schedule_interval=None,
    catchup=False,
)

"""
When creating files, you need to make sure the HOST of this server has the appropriate permissions
to write to the folder/file
"""

def saveData(**context):
    date = context['execution_date']
    newdate = local_tz.convert(date)
    print(newdate.strftime("%Y-%m-%d"))
    print(newdate)
    with open('localdata.txt', 'w') as f:
        f.write(newdate)

def readData(**context):
    with open('localdata.txt', 'r') as f:
        print(f.read())
    
t1 = PythonOperator(
    task_id='save_data',
    python_callable=saveData,
    dag=dag
)

t2 = PythonOperator(
    task_id='read_data',
    python_callable=readData,
    dag=dag
)

t3 = DummyOperator(
    task_id='complete',
    trigger_rule='one_success',
    dag=dag
)

t1 >> t2 >> t3
