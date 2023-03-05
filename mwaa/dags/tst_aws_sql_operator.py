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

#from airflow.decorators import dag, task
from airflow import DAG
from airflow import models
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from plugins.transfers.sql_to_s3 import *

from airflow.utils.task_group import TaskGroup
from boto3.dynamodb.conditions import Key, Attr
from sqlalchemy import create_engine
logger = logging.getLogger(__name__)

LIB_DIR = '/usr/local/airflow/plugins/instantclient_18_5'
DAG_ID = os.path.basename(__file__).replace(".py", "")

for _ in ["scripts","config"]:
    sys.path.append(os.path.join(os.path.dirname(__file__), _))

from scripts.common_dat_utils import copys3tos3
from config.vcdb_tables_config import * 


lt = [list(item) for item in list(zip_longest(*[iter(vcdb_tables)]*chunk_size, fillvalue=None))]
lt = [ list(filter(None, _)) for _ in lt ]

dag = DAG(dag_id=DAG_ID, schedule_interval=None, catchup=False, start_date=pendulum.today('UTC').add(days=-1))

start = DummyOperator(task_id='start',dag=dag)
end = DummyOperator(task_id='end',dag=dag)

copy_fls_s3tos3=PythonOperator(task_id='copy_fls_s3tos3',
                    python_callable=copys3tos3,
                    op_kwargs={'stage_bucket':stage_bucket,'stage_prefix':stage_prefix,
                    'target_bucket':target_bucket,'target_prefix': target_prefix,
                    'run_dt':"{{ds_nodash}}"},
                    dag=dag)

for g_id in enumerate(lt, 1):
    tg_id = f'VCDB_TBL_GRP_{g_id[0]}'
    with TaskGroup(group_id=tg_id,dag=dag) as taskgroups:
        for _ in g_id[1]:
            fld= str(_).upper()
            dt_nodash="{{ds_nodash}}"
            dt="{{ds}}"
            sql_table_task = SqlToS3Operator(
                task_id=f"sql_to_s3_task_{_}",
                sql_conn_id="orc_conn",
                query=f"{stg_sql}{_}",
                s3_bucket=stage_bucket,
                s3_key=f"{stage_prefix}/{dt_nodash}/{fld}/{_}_{dt}.txt",
                replace=True,
                pd_kwargs={'index': False, 'header': False,'sep':'|'},dag=dag)
        start >> taskgroups >> copy_fls_s3tos3 >> end

'''
sql_table_tasks = []
for _ in vcdb_tables:
    fld= str(_).upper()
    dt_nodash="{{ds_nodash}}"
    dt="{{ds}}"
    table_task = SqlToS3Operator(
        task_id=f"sql_to_s3_task_{_}",
        sql_conn_id="orc_conn",
        query=f"{stg_sql}{_}",
        s3_bucket=stage_bucket,
        s3_key=f"{stage_prefix}/{dt_nodash}/{fld}/{_}_{dt}.txt",
        replace=True,
        pd_kwargs={'index': False, 'header': False,'sep':'|'},
    )
    sql_table_tasks.append(table_task)
'''

