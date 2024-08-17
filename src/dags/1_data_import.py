import time
import pendulum
import contextlib
import numpy as np
import pandas as pd
import vertica_python
from pathlib import Path
from airflow.decorators import dag
from lib.s3_connection import s3_client_init
from typing import Dict, Union, Optional, List
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from lib.vertica import vertica_conn_init, run_sql_file_query



BASE_PATH = Path(__file__).parents[1]

DML_SCRIPT_PATH_DICT = {
    'STV2024071554__STAGING.currencies': BASE_PATH.joinpath('sql/dml/001_staging_currencies_dml.sql'),
    'STV2024071554__STAGING.transactions': BASE_PATH.joinpath('sql/dml/002_staging_transactions_dml.sql')
}

def fetch_s3_file(s3_client, bucket: str, key: str):
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )

def load_dataset_file_to_vertica(
    vertica_conn_info: Dict[str, Union[str, bool]],
    execution_date: str,
    date_name: str,
    dataset_path: Union[List[str], str],
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
    batch_size: int = 1000,
):

    if isinstance(dataset_path, str):
        dataset_path = [dataset_path]

    df = pd.read_csv(dataset_path[0], dtype=type_override)
    df['filter_date'] = df[date_name].apply(lambda x: pendulum.parse(x).date())

    if len(dataset_path) > 1:
        for i in range(1, len(dataset_path)):
            tmp_df = pd.read_csv(dataset_path[i], dtype=type_override)
            tmp_df['filter_date'] = tmp_df[date_name].apply(lambda x: pendulum.parse(x).date())
            df = pd.concat([df, tmp_df], ignore_index=True)
    df = df.loc[df['filter_date'] == pendulum.parse(execution_date).date()]

    df.drop('filter_date', axis=1, inplace=True)

    num_rows = len(df)

    vertica_conn = vertica_python.connect(**vertica_conn_info)

    columns = ', '.join(columns)
    copy_expr = f"""
        COPY {schema}.{table}_inc ({columns})
        FROM STDIN
        DELIMITER ',' direct
        ENCLOSED BY '"'
        REJECTED DATA AS TABLE {schema}.{table}_rej
        ;
    """

    chunk_size = num_rows // batch_size

    with contextlib.closing(vertica_conn.cursor()) as cur:
        cur.execute(f'TRUNCATE TABLE {schema}.{table}_inc')

        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            tmp_df = df.iloc[start:end+1]
            tmp_df = tmp_df.replace(np.nan, None, regex=True)
            cur.copy(copy_expr, tmp_df.to_csv(index=False, header=False))
            print("loaded")
            start += chunk_size + 1

        run_sql_file_query(
            sql_file_path=DML_SCRIPT_PATH_DICT.get(f'{schema}.{table}'),
            vertica_conn_info=vertica_conn_info,
            cur=cur,
        )

    vertica_conn.close()


@dag(
    schedule_interval='05 12 * * *',
    start_date=pendulum.parse('2022-10-01'),
    end_date=pendulum.parse('2022-11-01'),
    max_active_runs=1,
    concurrency=3
)
def staging_data_import():
    execution_date = '{{ ds }}'
    s3_connection_id = 'fintech_s3'
    vertica_connection_id = 'analytics_vertica'
    bucket = 'final-project'

    s3_client = s3_client_init(connection_id=s3_connection_id)
    vertica_conn_info = vertica_conn_init(connection_id=vertica_connection_id)

    transaction_files = [f'transactions_batch_{i}.csv' for i in range(1, 10)]
    s3_files = ['currencies_history.csv', *transaction_files]

    fetch_s3_files_task_list = []

    sleep_task = PythonOperator(
        task_id='sleep',
        python_callable=lambda: time.sleep(10),
    )
    dummy = EmptyOperator(task_id='dummy')

    for f in s3_files:
        temp_task = PythonOperator(
            task_id=f'fetch_{f}',
            python_callable=fetch_s3_file,
            op_kwargs={
                's3_client': s3_client,
                'bucket': bucket,
                'key': f
            },
        )

        fetch_s3_files_task_list.append(temp_task)

    load_currencies = PythonOperator(
            task_id='load_currencies',
            python_callable=load_dataset_file_to_vertica,
            op_kwargs={
                'vertica_conn_info': vertica_conn_info,
                'execution_date': execution_date,
                'date_name': 'date_update',
                'batch_size': 1_000,
                'dataset_path': '/data/currencies_history.csv',
                'schema': 'STV2024071554__STAGING',
                'table': 'currencies',
                'columns': [
                    'currency_code',
                    'currency_code_with',
                    'date_update',
                    'currency_with_div'
                ],
                'type_override': {
                    'currency_code': 'Int64',
                    'currency_code_with': 'Int64',
                    'currency_with_div': 'Float64',
                },
            },
    )

    load_transactions = PythonOperator(
            task_id='load_transactions',
            python_callable=load_dataset_file_to_vertica,
            op_kwargs={
                'vertica_conn_info': vertica_conn_info,
                'execution_date': execution_date,
                'batch_size': 1_000,
                'date_name': 'transaction_dt',
                'dataset_path': [f'/data/{i}' for i in transaction_files],
                'schema': 'STV2024071554__STAGING',
                'table': 'transactions',
                'columns': [
                    'operation_id',
                    'account_number_from',
                    'account_number_to',
                    'currency_code',
                    'country',
                    'status',
                    'transaction_type',
                    'amount',
                    'transaction_dt'
                ],
                'type_override': {
                    'account_number_from': 'Int64',
                    'account_number_to': 'Int64',
                    'currency_code': 'Int64',
                    'amount': 'Int64',
                },
            },
    )


    sleep_task >> fetch_s3_files_task_list >> dummy
    dummy >> [load_currencies, load_transactions]


_ = staging_data_import()
