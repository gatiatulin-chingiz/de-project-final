import boto3
from airflow.hooks.base import BaseHook

def s3_client_init(connection_id: str):
    connection_credentials = BaseHook.get_connection(connection_id)

    aws_access_key = connection_credentials.extra_dejson['aws_access_key_id']
    aws_secret_access_key = connection_credentials.extra_dejson['aws_secret_access_key']

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
    )

    return s3_client

