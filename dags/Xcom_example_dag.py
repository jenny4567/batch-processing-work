from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.xcom import XCom

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def _push_number(ti):
    x = 5
    ti.xcom_push(key='x_value', value = x)

def _pull_number(ti):
    number = ti.xcom_pull(key='x_value', task_ids='push_number')
    print("I have recieved the number ", number)

with DAG(
    'xcom_example_dag',
    default_args=default_args,
    description='Xcom example',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
)as dag:
    
    t2 = PythonOperator(
        task_id='push_number',
        python_callable=_push_number,
        dag=dag,
    )

    t3 = PythonOperator(
        task_id='pull_number',
        python_callable=_pull_number,
        dag=dag,
    )

t2 >> t3