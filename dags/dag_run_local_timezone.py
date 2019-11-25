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
    dag_id='dag_get_local_timezone',
    default_args=args,
    schedule_interval=None,
    catchup=False,
)

def getLocalTimezone(**context):
    date = context['execution_date']
    newdate = local_tz.convert(date)
    print(newdate.strftime("%Y-%m-%d"))
    print(newdate)
    return newdate

t1 = PythonOperator(
    task_id='get_local_timezone',
    python_callable=getLocalTimezone,
    provide_context=True,
    dag=dag
)

t2 = DummyOperator(
    task_id='complete',
    trigger_rule='one_success',
    dag=dag
)

t1 >> t2
