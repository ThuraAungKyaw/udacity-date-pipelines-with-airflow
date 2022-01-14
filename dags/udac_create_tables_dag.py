import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now()
}

queries = SqlQueries()

dag = DAG('udac_create_tables_dag',
          default_args=default_args,
          schedule_interval='@once',
          description='Create necessary tables in Redshift with Airflow')


create_staging_events_table = PostgresOperator(
    task_id="create_staging_events_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.create_staging_events,
)

create_staging_songs_table = PostgresOperator(
    task_id="create_staging_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.create_staging_songs,
)

create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.create_time_table,
)

create_songs_table = PostgresOperator(
    task_id="create_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.create_songs_table,
)

create_artists_table = PostgresOperator(
    task_id="create_artists_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.create_artists_table,
)

create_users_table = PostgresOperator(
    task_id="create_users_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.create_users_table,
)

create_songplays_table = PostgresOperator(
    task_id="create_songplays_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=queries.create_songplays_table,
)


