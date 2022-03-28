from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, 
                               LoadFactOperator,
                               LoadDimensionOperator, 
                               DataQualityOperator)
from helpers import SqlQueries

"""
Main DAG of data pipelines

Task dependencies:
                                                                             / -> load_song_dimension_table    \
                     / -> stage_events_to_redshift \                        /  -> load_user_dimension_table     \
    start_operator ->                                -> load_songplays_table                                    -> run_quality_checks -> end_operator
                     \ -> stage_songs_to_redshift  /                        \  -> load_artist_dimension_table   /
                                                                             \ -> load_time_dimension_table    /

Reference
I have been searching for some ideas or other help to find solutions in my code that are shown as below:
1. Idea and Sample for use create_tables.sql ...
    - [Udacity: How to use create_tables.sql in the project](https://knowledge.udacity.com/questions/60209)
    - [Udacity: sql_queries.py vs create_tables.sql](https://knowledge.udacity.com/questions/191189)
2. Idea and Sample for implement Data quality operator ...
    - [Udacity: How should the data quality operator be implemented?](https://knowledge.udacity.com/questions/631168)
    - [Udacity: Cascading 'query_checks' values to 'dq_checks' function within the DataQualityOperator](https://knowledge.udacity.com/questions/399236)
    - [Udacity: Defining Data Quality Checks](https://knowledge.udacity.com/questions/329453)
3. Idea and Sample for use default_args ...
    - [Airflow: Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)
    - [Airflow: DAG Runs](https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html)
    - [Udacity: How to stop multiple runs occurring at the same time?](https://knowledge.udacity.com/questions/171943)
4. Idea and Sample for COPY sql to load json data ... 
    - [Udacity: Load into table 'staging_events' failed. Check 'stl_load_errors'](https://knowledge.udacity.com/questions/253565)
5. Idea for solve some issues of Airflow ... [Udacity: Broken DAG Error](https://knowledge.udacity.com/questions/419394)
6. Idea and Sample for use Loop Lists ... [W3schools: Python - Loop Lists](https://www.w3schools.com/python/python_lists_loop.asp)

"""

#Log data: s3://udacity-dend/log_data
#Song data: s3://udacity-dend/song_data

#bucket = Variable.get('s3_bucket')      # udacity-dend
bucket = 'udacity-dend'
#logdata = Variable.get('s3_logdata')     # log_data
logdata = 'log_data'
logdata_json = 's3://udacity-dend/log_json_path.json'
#songdata = Variable.get('s3_songdata')    # song_data
songdata = 'song_data'

start_date = datetime.utcnow()

default_args = {
    'owner': 'Phakphoom Claiboon',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries' : 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
    "email_on_retry": False
}

dag = DAG('Sparkify_Main_DAG',
          default_args=default_args,
          description='Sparkify : Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket=bucket,
    s3_key=logdata,
    json=logdata_json,
    sql_copy_stmt=SqlQueries.copy_sql
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket=bucket,
    s3_key=songdata,
    sql_copy_stmt=SqlQueries.copy_sql
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    sql_load_stmt=SqlQueries.songplay_table_insert + SqlQueries.songplay_table_query
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    sql_load_stmt=SqlQueries.user_table_insert + SqlQueries.user_table_query,
    mode="delete"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    sql_load_stmt=SqlQueries.song_table_insert + SqlQueries.song_table_query,
    mode="delete"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    sql_load_stmt=SqlQueries.artist_table_insert + SqlQueries.artist_table_query,
    mode="delete"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    sql_load_stmt=SqlQueries.time_table_insert + SqlQueries.time_table_query,
    mode="delete"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=SqlQueries.all_table_check_quality,
    redshift_conn_id="redshift",
    sql_check_stmts=SqlQueries.check_quality_stmts,
    expected_results=SqlQueries.check_quality_expected_results
)

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator