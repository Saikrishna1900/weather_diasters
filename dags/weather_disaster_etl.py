from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG('weather_disaster_etl',
         schedule_interval=None,
         default_args=default_args,
         catchup=False,
         tags=['etl', 'weather', 'spark']) as dag:

    fetch_weather = BashOperator(
        task_id='fetch_weather',
        bash_command='python3 ~/airflow/lib/fetch_weather.py'
    )

    format_weather = BashOperator(
        task_id='format_weather',
        bash_command='source ~/airflow_venv/bin/activate && python3 ~/airflow/lib/format_weather.py'
    )

    fetch_disasters = BashOperator(
        task_id='fetch_disasters',
        bash_command='python3 ~/airflow/lib/fetch_disasters.py'
    )

    format_disasters = BashOperator(
        task_id='format_disasters',
        bash_command='source ~/airflow_venv/bin/activate && python3 ~/airflow/lib/format_disasters.py'
    )

    combine_data = BashOperator(
        task_id='combine_data',
        bash_command='source ~/spark_venv/bin/activate && python3 ~/airflow/lib/combine_data.py'
    )

    index_to_elastic = BashOperator(
        task_id='index_to_elastic',
        bash_command='source ~/spark_venv/bin/activate && python3 ~/airflow/lib/index_to_elastic.py'
    )

    # Task dependencies
    fetch_weather >> format_weather
    fetch_disasters >> format_disasters
    [format_weather, format_disasters] >> combine_data >> index_to_elastic
