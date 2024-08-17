import time
import pendulum
from pathlib import Path
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from lib.vertica import vertica_conn_init, run_sql_file_query



BASE_PATH = Path(__file__).parents[1]

DML_SCRIPT_PATH_DICT = {
    'STV2024071554__DWH.global_metrics': BASE_PATH.joinpath('sql/dml/003_dwh_global_metrics_dml.sql'),
}

@dag(
    schedule_interval='05 13 * * *',
    start_date=pendulum.parse('2022-10-01'),
    end_date=pendulum.parse('2022-11-01'),
    max_active_runs=1,
    concurrency=3
)
def dwh_datamart_update():
    execution_date = '{{ ds }}'
    vertica_connection_id = 'analytics_vertica'

    vertica_conn_info = vertica_conn_init(connection_id=vertica_connection_id)

    sleep_task = PythonOperator(
        task_id='sleep',
        python_callable=lambda: time.sleep(10),
    )

    load_global_metrics = PythonOperator(
        task_id='load_global_metrics',
        python_callable=run_sql_file_query,
        op_kwargs={
            'sql_file_path': DML_SCRIPT_PATH_DICT.get('STV2024071554__DWH.global_metrics'),
            'vertica_conn_info': vertica_conn_info,
            'sql_params': {
                'calc_date': execution_date,
            }
        }
    )

    sleep_task >> [load_global_metrics]

_ = dwh_datamart_update()
