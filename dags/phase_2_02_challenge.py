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
    'phase_2_dag',
    default_args=default_args,
    description='Phase 2 challenege DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 28),
    catchup=False,
)

extract_new_visits = """
    CREATE TABLE IF NOT EXISTS new_visits AS
    SELECT visit_id, store_id, visitor_count
    FROM daily_visits
    WHERE visit_date = '{{ ds }}';
"""

t1 = PostgresOperator(
    task_id='extract_new_visits',
    sql=extract_new_visits,
    postgres_conn_id='batching_phase_02',
    dag=dag,
)

aggregate_new_visits = '''
    CREATE TABLE IF NOT EXISTS store_visits_aggregated AS
    SELECT store_id, COUNT(*) AS total_visits, SUM(visitor_count) AS store_visitor_count
    FROM new_visits
    GROUP BY store_id;
'''

t2 = PostgresOperator(
    task_id='aggregate_new_visits',
    sql=aggregate_new_visits,
    postgres_conn_id='batching_phase_02',
    dag=dag,
)

update_visits_summary = """
    DELETE FROM store_visits_summary;
    INSERT INTO store_visits_summary (store_id, total_visits, store_visitor_count)
    SELECT store_id, total_visits, store_visitor_count
    FROM store_visits_aggregated;
"""

t3 = PostgresOperator(
    task_id='update_visits_summary',
    sql=update_visits_summary,
    postgres_conn_id='batching_phase_02',
    dag=dag,
)

drop_new_visits = '''
DROP TABLE new_visits;
'''

t4 = PostgresOperator(
    task_id='drop_new_visits',
    sql=drop_new_visits,
    postgres_conn_id='batching_phase_02',
    dag=dag,
)

drop_aggregated_visits = '''
DROP TABLE store_visits_aggregated;
'''

t5 = PostgresOperator(
    task_id="drop_aggregated_visits",
    sql=drop_aggregated_visits,
    postgres_conn_id="batching_phase_02",
    dag=dag,
)

t1 >> t2 >> t3 >> [t4, t5]