from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'sales_summary_dag',
    default_args=default_args,
    description='A DAG to extract, transform, and load sales data into a summary table',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
)

extract_and_aggregate_sales = '''
    CREATE TABLE IF NOT EXISTS aggregated_sales AS
    SELECT s.sales_rep_id, sr.region_id, COUNT(s.id) AS num_transactions, SUM(s.amount) AS total_amount
    FROM transactions s
    JOIN sales_reps sr ON s.sales_rep_id = sr.id
    JOIN regions r ON sr.region_id = r.id
    WHERE s.transaction_date >= '{{ prev_ds }}' AND s.transaction_date <= '{{ ds }}'
    GROUP BY s.sales_rep_id, sr.region_id;
'''

t1 = PostgresOperator(
    task_id='extract_and_aggregate_sales',
    sql=extract_and_aggregate_sales,
    postgres_conn_id='region_sales_conn',
    dag=dag,
)

update_sales_summary = '''
    CREATE TABLE IF NOT EXISTS sales_summary AS
    SELECT sales_rep_id, region_id, num_transactions, total_amount
    FROM aggregated_sales
    ON CONFLICT (sales_rep_id, region_id) DO UPDATE SET
        num_transactions = sales_summary.num_transactions + excluded.num_transactions,
        total_amount = sales_summary.total_amount + excluded.total_amount;
'''

t2 = PostgresOperator(
    task_id='update_sales_summary',
    sql=update_sales_summary,
    postgres_conn_id='region_sales_conn',
    dag=dag,
)

drop_aggregated_sales = '''
DROP TABLE aggregated_sales;
'''

t3 = PostgresOperator(
    task_id="drop_aggregated_sales",
    sql=drop_aggregated_sales,
    postgres_conn_id="region_sales_conn",
    dag=dag,
)

t1 >> t2 >> t3

