import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
import pendulum
import datetime

from controllers.airflow_controller import AirflowController

local_tz = pendulum.timezone("America/Los_Angeles")

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
}

dag = DAG(
    dag_id='dag_pool_locking',
    default_args=args,
    schedule_interval=None,
    catchup=False,
)

def addAirflowPool():
    ac = AirflowController()
    ac.loadDynamicPools()
    ac.addDynamicPool("te1st", 5)
    ac.addDynamicPool("te2st", 4)
    ac.addDynamicPool("te2st", 4)
    ac.addDynamicPool("te2st", 4)
    ac.addDynamicPool("te2st", 4)
    ac.addDynamicPool("te2st", 1)
    ac.addDynamicPool("te3aaaast", 1)
    ac.addDynamicPool("te1st", 1)
    ac.importDynamicPoolFile()

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

t1 >> t4
