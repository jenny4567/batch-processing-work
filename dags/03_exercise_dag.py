from airflow import DAG
from airflow.providers.mysql.hooks.mysql import  MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
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

def _create_account_summary():
    request = "DROP TABLE IF EXISTS account_summary; CREATE TABLE account_summary (account_id INT PRIMARY KEY,total_transactions INT,total_amount NUMERIC);"
    pg_hook = PostgresHook(postgres_conn_id='local_postgres', schema='batch_process_03')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    connection.commit()

def _extract_financial_data(ti):
    request = "SELECT account_id, COUNT(*) AS total_transactions, SUM(amount) AS total_amount FROM trans GROUP BY account_id;"
    ms_hook = MySqlHook(mysql_conn_id='financial_mariadb', schema='financial')
    connection = ms_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    data = cursor.fetchall()
    ti.xcom_push(key='data', value = data)

def _load_account_summary(ti):
    data = ti.xcom_pull(key='data', task_ids='extract_financial_data')
    pg_hook = PostgresHook(postgres_conn_id='local_postgres', schema='batch_process_03')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    for row in data:
        request = f"INSERT INTO account_summary(account_id, total_transactions, total_amount) VALUES ({row[0]},{row[1]},{row[2]})"
        cursor.execute(request)
    connection.commit()

with DAG(
    'financial_dag',
    default_args=default_args,
    description='A financial data extraction and loading DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
)as dag:
    
    t1 = PythonOperator(
        task_id='create_account_summary',
        python_callable=_create_account_summary,
        dag=dag,
    )

    t2 = PythonOperator(
        task_id='extract_financial_data',
        python_callable=_extract_financial_data,
        dag=dag,
    )

    t3 = PythonOperator(
        task_id='load_account_summary',
        python_callable=_load_account_summary,
        dag=dag,
    )

t1 >> t2 >> t3
