import contextlib
import vertica_python
from pathlib import Path
from typing import Dict, Union
from airflow.hooks.base import BaseHook


def vertica_conn_init(connection_id: str) -> Dict[str, Union[str, bool]]:
    connection_credentials = BaseHook.get_connection(connection_id)

    return {
        'host': connection_credentials.host,
        'port': connection_credentials.port,
        'user': connection_credentials.login,
        'password': connection_credentials.password,
        'database': connection_credentials.schema,
        'autocommit': True,
    }


def run_sql_file_query(
        sql_file_path: Path,
        vertica_conn_info: Dict[str, Union[str, bool]],
        cur = None,
        sql_params: dict = None,
    ):

    with open(sql_file_path, 'r') as f:
        f_content = f.read()

    if sql_params:
        f_content = f_content.format(**sql_params)

    sql_query_list = [query.rstrip() for query in f_content.split(';')]

    if not cur:
        vertica_conn = vertica_python.connect(**vertica_conn_info)
        with contextlib.closing(vertica_conn.cursor()) as cur:
            for sql_query in sql_query_list:
                cur.execute(sql_query + ';')
    else:
        for sql_query in sql_query_list:
            cur.execute(sql_query + ';')
