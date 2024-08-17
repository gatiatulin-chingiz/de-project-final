import time
import pendulum
from pathlib import Path
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from lib.vertica import vertica_conn_init, run_sql_file_query



BASE_PATH = Path(__file__).parents[1]

@dag(
    schedule_interval=None,
    start_date=pendulum.parse('2022-10-01'),
    end_date=pendulum.parse('2022-10-31'),
    max_active_runs=1,
    concurrency=3,
)
def vertica_init_tables():
    task_list = []

    connection_id = 'analytics_vertica'
    vertica_conn_info = vertica_conn_init(connection_id=connection_id)
    
    for path in BASE_PATH.joinpath('sql/ddl').glob('*.sql'):
        print(path)
        task_list.append(PythonOperator(
            task_id=f'run_{path.stem}',
            python_callable=run_sql_file_query,
            op_kwargs={
                'sql_file_path': path,
                'vertica_conn_info': vertica_conn_info,
            },
        ))

    sleep_task = PythonOperator(
        task_id='sleep',
        python_callable=lambda: time.sleep(10),
    )

    sleep_task >> task_list

    task_list

_ = vertica_init_tables()