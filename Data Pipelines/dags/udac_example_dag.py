from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries as s

# AWS_KEY = os.environ.get('AWS_KEY')

# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {

    'owner': 'udacity',

    'start_date': datetime(2019, 1, 12),

    'depends_on_past' : False,

    'retries' : 3,

    'email_on_retry' : False,

    'retry_delta' : timedelta(minutes = 5),

    'catchup' : False,

}
dag = DAG('udac_example_dag',

    default_args = default_args,

    description ='Load and transform data in Redshift with Airflow',

    schedule_interval ='0 * * * *'
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_songs_to_redshift = StageToRedshiftOperator(

    task_id='Stage_songs',

    dag=dag,

    table = 'staging_songs',

    redshift_conn_id='redshift',

    aws_credentials_id = 'aws_credentials',

    s3_bucket = 'udacity-dend',

    s3_key = 'song_data',

    json_format = 'AUTO',

    provide_context = True

)

stage_events_to_redshift = StageToRedshiftOperator(

    task_id='Stage_events',

    dag=dag,

    table='staging_events',

    redshift_conn_id="redshift",

    aws_credentials_id = 'aws_credentials',

    s3_bucket = 'udacity-dend',

    s3_key = 'log_data',

    json_format = 's3://udacity-dend/log_json_path.json',

    provide_context = True

)

load_songplays_table = LoadFactOperator(

    task_id='Load_songplays_fact_table',

    dag=dag,

    redshift_conn_id = "redshift",

    dest_table= "songplays",

    sql_to_load = s.songplay_table_insert,

    provide_context = True

)

load_user_dimension_table = LoadDimensionOperator(

    task_id='Load_user_dim_table',

    redshift_conn_id = "redshift",

    dag=dag,

    dest_table = "users",

    sql_to_load = s.user_table_insert,

    operation ='DELETE_INSERT',

    provide_context = True

)

load_song_dimension_table = LoadDimensionOperator(

    task_id='Load_song_dim_table',

    redshift_conn_id = "redshift",

    dag=dag,

    dest_table = "songs",

    sql_to_load = s.song_table_insert,

    operation ='DELETE_INSERT',

    provide_context = True

)

load_artist_dimension_table = LoadDimensionOperator(

    task_id='Load_artist_dim_table',

    redshift_conn_id = "redshift",

    dag=dag,

    dest_table = "artists",

    sql_to_load = s.artist_table_insert,

    operation ='DELETE_INSERT',

    provide_context = True

)

load_time_dimension_table = LoadDimensionOperator(

    task_id='Load_time_dim_table',

    dag=dag,

    redshift_conn_id = "redshift",

    dest_table = "time",

    sql_to_load = s.time_table_insert,

    operation ='DELETE_INSERT',

    provide_context = True

)

run_quality_checks = DataQualityOperator(

    task_id='Run_data_quality_checks',

    dag=dag,

    redshift_conn_id = "redshift",

    table = [ "songplays", "users", "songs", "artists", "time"],

    dq_checks= [

    {'check_sql': "SELECT COUNT(*) FROM users WHERE user_id is null", 'expected_result':0},

    {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result':0},

    {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result':0},

    {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result':0},

    {'check_sql': "SELECT COUNT(*) FROM songplays WHERE userid is null", 'expected_result':0}

    ],

    provide_context = True
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
