from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

S3_BUCKET='udacity-dend'
LOG_KEY = 'log_data'
SONG_KEY = 'song_data'
AWS_CREDENTIALS_ID = 'aws_credentials'
REDSHIFT_CONN_ID = "redshift"

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 1, 1),
    'catchup_by_default': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'depends_on_past': False
}

queries = SqlQueries()

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key=LOG_KEY,
    json_format='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key=SONG_KEY,
    json_format='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    query=queries.songplay_table_insert,
    table='songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    query=queries.user_table_insert,
    table='users'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    query=queries.song_table_insert,
    table='songs'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    query=queries.artist_table_insert,
    table='artists'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    query=queries.time_table_insert,
    table='time'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    tables=queries.tables,
    check_queries=queries.check_queries
  
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
run_quality_checks >> end_operator

