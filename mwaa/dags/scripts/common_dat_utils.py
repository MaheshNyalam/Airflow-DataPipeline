import pandas as pd
import boto3
import io
import sys
from boto3.dynamodb.conditions import Key, Attr
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.dummy import DummyOperator
from airflow.decorators import dag, task
import logging
from sqlalchemy import create_engine
import re
import pendulum
logger = logging.getLogger(__name__)

def copys3tos3(**kwargs):
    s3 = boto3.resource('s3')
    stage_bucket = kwargs['stage_bucket']
    target_bucket = kwargs['target_bucket']
    run_dt = kwargs['run_dt'].strip()
    stg_prefix = kwargs['stage_prefix']+"/"+run_dt
    s3 = boto3.resource('s3')
    stg_bucket = s3.Bucket(stage_bucket)
    tgt_bucket = s3.Bucket(target_bucket)
    for obj in stg_bucket.objects.filter(Prefix=stg_prefix):
        tgt_prefix = kwargs['target_prefix']+"/"+run_dt
        if 'failure' not in obj.key and obj.key is not None:
            fld=obj.key[len(stg_prefix):].split('_')[0].upper()
            #print('fld: ',fld)
            stg_source = { 'Bucket': stage_bucket,'Key': obj.key}
            tgt_prefix=f'{tgt_prefix}{fld}'
            tgt_key = tgt_prefix + obj.key[len(stg_prefix):]
            tgt_obj = tgt_bucket.Object(tgt_key)
            #msg = f" Copy file from Bucket: {stg_source} to Bucket: {tgt_obj} "
            msg = f" Copy file from Bucket: {stage_bucket}/{obj.key} to Bucket: {target_bucket}/{tgt_key}"
            try :
                tgt_obj.copy(stg_source)
                print(f"{msg} Success")
            except Exception as e:
                print(f"{msg} {e} Failed")
