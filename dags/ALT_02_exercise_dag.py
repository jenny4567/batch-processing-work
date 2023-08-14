from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=4),
}

dag = DAG(
    '02_exercise_dag',
    default_args=default_args,
    description='DAG file to get regional sales figure by representative.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
)

summed_transactions_creation = """
    CREATE TABLE IF NOT EXISTS summed_transactions AS 
    SELECT sales_rep_id, SUM(amount) AS total_sales, COUNT(amount) AS no_transactions
    FROM transactions
    GROUP BY sales_rep_id;
"""


t1 = PostgresOperator(
    task_id='summed_transactions',
    sql=summed_transactions_creation,
    postgres_conn_id='region_sales_conn',
    dag=dag,
)


sales_reps_with_regions_creation = '''
    CREATE TABLE IF NOT EXISTS sales_reps_with_regions AS 
    SELECT sales_reps.id AS sales_rep_id, sales_reps.name, regions.name AS country
    FROM sales_reps JOIN regions ON region_id = regions.id;
'''


t2 = PostgresOperator(
    task_id='sales_reps_with_regions',
    sql=sales_reps_with_regions_creation,
    postgres_conn_id='region_sales_conn',
    dag=dag,
)

sales_reps_final_creation = """
CREATE TABLE IF NOT EXISTS sales_reps_final AS 
SELECT sales_reps_with_regions.sales_rep_id, name, country, total_sales, no_transactions
FROM sales_reps_with_regions JOIN summed_transactions ON sales_reps_with_regions.sales_rep_id = summed_transactions.sales_rep_id;
"""


t3 = PostgresOperator(
    task_id='sales_reps_final',
    sql=sales_reps_final_creation,
    postgres_conn_id='region_sales_conn',
    dag=dag,
)

drop_summed_transactions = '''
DROP TABLE summed_transactions;
'''

t4 = PostgresOperator(
    task_id='drop_summed_transactions',
    sql=drop_summed_transactions,
    postgres_conn_id='region_sales_conn',
    dag=dag,
)

drop_sales_reps_with_regions = '''
DROP TABLE sales_reps_with_regions;
'''

t5 = PostgresOperator(
    task_id="drop_sales_reps_with_regions",
    sql=drop_sales_reps_with_regions,
    postgres_conn_id="region_sales_conn",
    dag=dag,
)

t1 >> t2 >> t3 >> [t4, t5]
