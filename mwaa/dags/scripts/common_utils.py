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
#import cx_Oracle

def get_sf_table_names(**kwargs):
    db_nm=kwargs['db_nm']
    schema_nm=kwargs['schema_nm']
    sf_table_nm=kwargs['sf_table_nm']
    sf_conn_id=kwargs['sf_conn_id']
    snowflake_conn_id=SnowflakeHook(snowflake_conn_id=sf_conn_id)
    #sql=f"select distinct tablename as tablename from {db_nm}.{schema_nm}.{sf_table_nm} LIMIT 2"
    sql=f"select distinct tablename as tablename from {db_nm}.{schema_nm}.{sf_table_nm} "
    files_to_be_processed_df=snowflake_conn_id.get_pandas_df(sql)
    files_to_be_processed_df.columns= files_to_be_processed_df.columns.str.lower()
    #files_to_be_processed_df = pd.read_sql_query(sql, snowflake_conn_id)
    #print(files_to_be_processed_df.info())
    #print(files_to_be_processed_df)
    tables=files_to_be_processed_df['tablename'].tolist()
    return tables

def get_orcl_table_data(**kwargs):
    dt = pendulum.now().to_date_string()
    #run_dt = dt.replace('-','')
    run_dt=kwargs['run_dt']
    print('run_dt: ', run_dt)
    s="select * from ECL_ACES"
    #select * from ECL_ACES.ASPIRATION
    fl_nm = kwargs['fl_nm']
    stage_bucket=kwargs['stage_bucket']
    stage_prefix=kwargs['stage_prefix']
    header=kwargs['header']
    orcl_conn_id=kwargs['orcl_conn_id']
    sep=kwargs['delimiter']
    fl_extn=kwargs['fl_extn']
    # dwh_hook = SnowflakeHook(snowflake_conn_id="dat_sf_con_nonprod")
    orcl_hook = OracleHook(oracle_conn_id=orcl_conn_id)
    sql=f"{s}.{fl_nm}"
    s3_bucket = boto3.resource("s3").Bucket(stage_bucket)
    msg=f"{fl_nm} file generation"     
    try:
        files_to_be_processed_df = orcl_hook.get_pandas_df(sql)
        #files_to_be_processed_df = pd.read_sql_query(sql, conn) # Testing ..
        files_to_be_processed_df.columns= files_to_be_processed_df.columns.str.lower() #Column lowercase 
        #print(files_to_be_processed_df)
        with io.StringIO() as csv_buffer:
            files_to_be_processed_df.to_csv(csv_buffer, index=False, header=header,sep=sep)
            key=f"{stage_prefix}/{run_dt}/{fl_nm}_{dt}.{fl_extn}"
            print(key)
            s3_bucket.put_object(Key=key, Body=csv_buffer.getvalue())
            print(f"{msg} succesfull ")
    except Exception as e:
        key=f"{stage_prefix}/{run_dt}/failure/{fl_nm}_{dt}.{fl_extn}"
        s3_bucket.put_object(Key=key, Body=None)
        print(f"{msg} failed, error {e} ")


def get_orcl_multitable_data(**kwargs):
    dt = pendulum.now().to_date_string()
    run_dt=kwargs['run_dt'].strip()
    tables = kwargs['tables']
    header=kwargs['header']
    stage_bucket=kwargs['stage_bucket']
    stage_prefix=kwargs['stage_prefix']
    orcl_conn_id=kwargs['orcl_conn_id']
    sep=kwargs['delimiter']
    s="select * from ECL_ACES"
    #fl_nm = kwargs['fl_nm']
    print('run_dt: ', run_dt)
    fl_extn=kwargs['fl_extn']
    orcl_hook = OracleHook(oracle_conn_id=orcl_conn_id)
    for _ in tables:
        fl_nm = _
        sql=f"{s}.{fl_nm}"
        msg=f"{fl_nm} file generation" 
        s3_bucket = boto3.resource("s3").Bucket(stage_bucket)
        try: 
            files_to_be_processed_df = orcl_hook.get_pandas_df(sql)
            #files_to_be_processed_df = pd.read_sql_query(sql, conn) # Testing ..
            files_to_be_processed_df.columns= files_to_be_processed_df.columns.str.lower() #Column lowercase 
            table_count=len(files_to_be_processed_df.index)
            print(f" Table Name:  {fl_nm} Count: {table_count}")
            #print(files_to_be_processed_df)
            with io.StringIO() as csv_buffer:
                files_to_be_processed_df.to_csv(csv_buffer, index=False, header=header,sep=sep)
                key=f"{stage_prefix}/{run_dt}/{fl_nm}_{dt}.{fl_extn}"
                print(key)
                s3_bucket.put_object(Key=key, Body=csv_buffer.getvalue())
                print(f"{msg} succesfull ")
        except Exception as e:
            key=f"{stage_prefix}/{run_dt}/failure/{fl_nm}_{dt}.{fl_extn}"
            s3_bucket.put_object(Key=key, Body=b'')
            print(f"{msg} failed, error {e} ")

# def copys3tos3(**kwargs):
#     s3 = boto3.resource('s3')
#     stg_bucket = s3.Bucket(kwargs['stage_bucket'])
#     tgt_bucket = s3.Bucket(kwargs['target_bucket'])
#     stg_prefix = kwargs['stage_prefix']
#     run_dt = kwargs['run_dt']
#     for obj in stg_bucket.objects.filter(Prefix=stg_prefix):
#         tgt_prefix = kwargs['target_prefix']
#         if 'failure' not in obj.key and obj.key is not None:
#             print('Actual obj.key: ', obj.key)
#             flnm=str(obj.key.split('/')[-1])
#             fld=str(obj.key.split('/')[-1]).split('_')[0].upper()
#             #fld=obj.key[len(stg_prefix):].split('_')[0].upper()
#             #str(obj.key).replace(' ','')
#             #stg_source = {'Bucket': stg_bucket,'Key': str(obj.key).replace(' ','')}
#             stg_source = {'Bucket': stg_bucket,'Key': obj.key}
#             # replace the prefix
#             #new_key = obj.key.replace(old_prefix, new_prefix, 1)
#             print('stg_source: ', stg_source) 
#             #print('Original - tgt_prefix: ' , tgt_prefix)
#             tgt_prefix=f"{tgt_prefix}/{run_dt}/{fld}"
#             #print('After fld - tgt_prefix: ' , tgt_prefix)
#             #print('obj.key: ', obj.key[len(stg_prefix):])
#             #tgt_key = tgt_prefix + obj.key[len(stg_prefix):]
#             tgt_key = f"{tgt_prefix}/{flnm}" 
#             print('Target Key: ' , tgt_key)
#             tgt_obj = tgt_bucket.Object(tgt_key)
#             msg = f" Copy file from Bucket: {stg_source} to Bucket: {kwargs['target_bucket']}/{tgt_key}"
#             try :
#                 tgt_obj.copy(stg_source)
#                 print(f"{msg} Success")
#             except:
#                 print(f"{msg} Failed")
#                 sys.exit(1)

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
