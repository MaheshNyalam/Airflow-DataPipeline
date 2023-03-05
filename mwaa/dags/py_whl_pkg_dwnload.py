import pandas as pd
import boto3
import io
import sys
import os
#import cx_Oracle
import pendulum
import logging
import re
from itertools import zip_longest


from airflow import DAG
from airflow.operators.bash import BashOperator

DAG_ID = os.path.basename(__file__).replace(".py", "")

S3_BUCKET = 'aap-dl-dat-fusion-mwaa-test1-dev'
S3_KEY = 'mwaa/backup_whl/' 

with DAG(dag_id=DAG_ID, schedule_interval=None, catchup=False, start_date=pendulum.today('UTC').add(days=-1)) as dag:
    cli_command = BashOperator(
        task_id="bash_command",
        bash_command=f"aws s3 cp /tmp/plugins.zip s3://{S3_BUCKET}/{S3_KEY}"
    )

'''
with DAG(dag_id=DAG_ID, schedule_interval=None, catchup=False, start_date=pendulum.today('UTC').add(days=-1)) as dag:
    cli_command = BashOperator(
        task_id="bash_command",
        bash_command=f"aws s3 cp /tmp/plugins.zip s3://{S3_BUCKET}/{S3_KEY}"
    )
'''
