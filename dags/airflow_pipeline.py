from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

today = datetime.today()
start_date = today - timedelta(days=1)

default_args = {
    'owner': 'Michel',
    'depends_on_past': False,
    'retries': 3,
    'catchup': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'start_date': start_date
}

with DAG(
    dag_id='data_pipeline_v3',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    template_searchpath="/opt/airflow/scripts",
    schedule_interval='@hourly',
    max_active_runs=1
) as dag:

    start_operator = DummyOperator(task_id='Begin_execution')

    create_all_tables = PostgresOperator(
        task_id="create_all_tables",
        postgres_conn_id="redshift",
        sql="create_tables.sql"
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        s3_bucket="udacity-dend",
        s3_prefix="log_data",
        table_name="staging_events",
        copy_option="{{ params.option }}",
        params={
            "option": "FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
        }
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        s3_bucket="udacity-dend",
        s3_prefix="song_data",
        table_name="staging_songs",
        copy_option="{{ params.option }}",
        params={
            "option": "FORMAT AS JSON 'auto'"
        }
    )

    load_songplays_table = LoadFactOperator(
        task_id='load_songplays',
        redshift_conn_id="redshift",
        table_name="songplays",
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user',
        redshift_conn_id="redshift",
        table_name="users",
        sql_query=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song',
        redshift_conn_id="redshift",
        table_name="songs",
        sql_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist',
        redshift_conn_id="redshift",
        table_name="artists",
        sql_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time',
        redshift_conn_id="redshift",
        table_name="time",
        sql_query=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='data_quality',
        redshift_conn_id="redshift",
        check_dict=SqlQueries.data_quality
    )

    end_operator = DummyOperator(task_id='Stop_execution')

stage = [stage_events_to_redshift, stage_songs_to_redshift]
load_dim_table = [
    load_song_dimension_table, load_artist_dimension_table,
    load_user_dimension_table, load_time_dimension_table
]

start_operator >> create_all_tables >> stage >> load_songplays_table
load_songplays_table >> load_dim_table >> run_quality_checks >> end_operator
