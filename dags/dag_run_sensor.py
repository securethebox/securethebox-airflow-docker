import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
import pendulum
import datetime
import time

from controllers.airflow_controller import AirflowController

local_tz = pendulum.timezone("America/Los_Angeles")

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
}

dag = DAG(
    dag_id='dag_pool_locking',
    default_args=args,
    schedule_interval='@once',
    catchup=False,
)

def addAirflowPool():
    ac = AirflowController()
    ac.loadDynamicPools()
    ac.addDynamicPool("pool1", 4)
    ac.importDynamicPoolFile()

def longTask():
    print("Start")
    time.sleep(5)
    print("Finished")

def group(t):
    return PythonOperator(
        task_id='longtask'+str(t),
        python_callable=longTask,
        pool='pool1',
        dag=dag
    )

t1 = PythonOperator(
    task_id='addAirflowPool',
    python_callable=addAirflowPool,
    dag=dag
)

t4 = DummyOperator(
    task_id='complete',
    trigger_rule='one_success',
    dag=dag
)

for x in range(0,10):
    t1 >> group(x) >> t4
