import sys
import os
import io
import pandas as pd
import boto3
import yaml
import time
import pendulum
import logging
from airflow.utils.email import send_email
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
sys.path.append(os.path.dirname(__file__))
from common_functions import get_ctl_table_data,query_metadata_table
logger = logging.getLogger(__name__)

dwh_hook = SnowflakeHook(snowflake_conn_id="dat_sf_con_nonprod")

def get_feed_list_needed_for_processing(**kwargs):
    env = kwargs['env']
    batchnum = kwargs['batchnum']
    s3_bucket = boto3.resource("s3").Bucket("aap-dl-dat-fusion-mwaa-{}".format(env))
    # return_value = True
    # bucket = s3.Bucket('mybucket')
    # s3_client = boto3.client("s3")
    # s3 = boto3.resource('s3')
    files_to_be_processed_df = get_ctl_table_data(env=env)
    files_to_be_processed_df = files_to_be_processed_df[['FEED_NAME']].drop_duplicates()
    # files_to_be_processed_df = pd.DataFrame({'FEED_NAME':['ITEMXREF']})
    print(files_to_be_processed_df)
    # if files_to_be_processed_df.shape[0] == 0:
    #     return_value = False
    # files_to_be_processed_list = files_to_be_processed_df['FEED_NAME'].tolist()
    with io.StringIO() as csv_buffer:
        # if files_to_be_processed_df.shape[0] == 0:
            # files_to_be_processed_df = pd.DataFrame({'FEEDNAME':['DUMMYFEED']})
        files_to_be_processed_df.to_csv(csv_buffer, index=False, header=False)
        s3_bucket.put_object(Key="mwaa/dags/dds_feed_configs/feed_list_{}.txt".format(batchnum), Body=csv_buffer.getvalue())
    

    feedinfo_from_MD_table_df = query_metadata_table()
    feedinfo_from_MD_table_df = feedinfo_from_MD_table_df.rename(columns={'Dataset_Name':'FEEDNAME'})
    df = feedinfo_from_MD_table_df.dropna(how="all",axis=0,subset=['FEEDNAME'])
    feed_loadtype_dict = df.set_index('FEEDNAME').T.to_dict('list')
    yaml.dump_s3 = lambda obj, f: s3_bucket.Object(key=f).put(Body=yaml.dump(obj))
    yaml.dump_s3(feed_loadtype_dict, "mwaa/dags/dds_feed_configs/feed_with_load_types.yaml")
    time.sleep(30)

    # return return_value

def send_summary_report(*op_args):
    # dwh_hook = SnowflakeHook(snowflake_conn_id="dat_sf_con_nonprod")
    env = op_args[0].upper()
    PARENT_DAG_NAME = op_args[1]
    summary_email_dl = op_args[2]
    cycle_date = op_args[3]
    batchnum = op_args[4]
    if batchnum != 'summary':
        sql="""SELECT * FROM AAP_DAT_{}_DB.LOCATION.DAT_AIRFLOW_DAILY_STATUS WHERE CYCLE_DATE='{}' AND BATCHNUMBER='{}'""".format(env,cycle_date,batchnum)
    else:
        sql="""SELECT * FROM AAP_DAT_{}_DB.LOCATION.DAT_AIRFLOW_DAILY_STATUS WHERE CYCLE_DATE='{}'""".format(env,cycle_date)
    df_test = dwh_hook.get_pandas_df(sql)
    df_test.drop(['BATCHNUMBER'],axis=1,inplace=True)
    df_test.sort_values(by=['FEED_NAME','FILE_DATE'],ascending=True,inplace=True)
    df_test.insert(0, 'S_No', range(1, 1 + len(df_test)))
    # df_test.reset_index(drop=True,inplace=True)
    total_count = df_test.shape[0]
    processed_df = df_test[df_test.FILE_STATUS == 'Processed']
    processed = processed_df.shape[0]
    notprocessed_df = df_test[df_test.FILE_STATUS == 'NotProcessed']
    notprocessed = notprocessed_df.shape[0]
    skipped_df = df_test[df_test.FILE_STATUS == 'Skipped']
    skipped = skipped_df.shape[0]
    def highlight(row):
        if row['FILE_STATUS'] == 'Processed':
            return ['background-color: #ADF987'] * len(row)
        elif row['FILE_STATUS'] == 'NotProcessed':
            return ['background-color: #FF5D5D'] * len(row)
        elif row['FILE_STATUS'] == 'Skipped':
            return ['background-color: #F1F13D'] * len(row)
        else:
            return [''] * len(row)

    df = df_test.style.apply(highlight, axis=1)
    df_final = df.set_table_styles([
        {'selector': '.col_heading','props': 'background-color: #F5B183; color: black;'},
        {"selector": "", "props": [("border", "1px solid black")]},
        {"selector": "tbody td", "props": [("border", "1px solid black")]},
        {"selector": "th", "props": [("border", "1px solid black")]}
        ]).hide_index()
    if batchnum != 'summary':
        subject = f"DDS Jobs Summary Report from {env} Airflow DAG - {PARENT_DAG_NAME} for the date - {cycle_date}"
    else:
        subject = f"{env} DDS Jobs final summary report for the date - {cycle_date}"
    
    html = """\
	<html>
	<head></head>
	<body>
        <br>
        Succeeded Tasks - {0}
        <br>
        Failed Tasks - {1}
        <br>
        Skipped Tasks - {2}
        <br>
        Total Tasks - {3}
        <br>
        <br>
		{4}
	</body>
	</html>
	""".format(processed,notprocessed,skipped,total_count,df_final.to_html())

    send_email(to=summary_email_dl, subject=subject , html_content=html)

def truncate_summary_table(*op_args):
    env = op_args[0].upper()
    batchnum = op_args[1]
    if batchnum == '4':    
        sql = """DELETE FROM AAP_DAT_{}_DB.LOCATION.DAT_AIRFLOW_DAILY_STATUS WHERE 1=1""".format(env)
    else:
        sql = """DELETE FROM AAP_DAT_{}_DB.LOCATION.DAT_AIRFLOW_DAILY_STATUS WHERE BATCHNUMBER='{}'""".format(env,batchnum)
    engine = dwh_hook.get_sqlalchemy_engine()
    with engine.connect().execution_options(autocommit=False) as connection:
        try:
            connection.execute("BEGIN")
            connection.execute(sql)
            connection.execute("COMMIT")
        except:
            connection.execute("ROLLBACK")
        finally:
            connection.close()
    engine.dispose()

def get_cycle_load_date(*op_args):
    env = op_args[0].upper()
    sql="""SELECT DATEADD(day,1,MAX(LOAD_DATE)) AS LOAD_DATE FROM AAP_DAT_{}_DB.PUBLIC.DAT_LOAD_CYCLE_DATES WHERE LOAD_STATUS IS NOT NULL""".format(env)
    # dwh_hook = SnowflakeHook(snowflake_conn_id="dat_sf_con_nonprod")
    df_dates = dwh_hook.get_pandas_df(sql)
    cycle_date = df_dates.iloc[0]['LOAD_DATE']
    # cycle_date = cycle_date.add(days=1)
    cycle_date = str(cycle_date)

    return cycle_date

def insert_next_cycle_date(*op_args):
    env = op_args[0].upper()
    cycle_date = op_args[1]
    logger.info(f"cycle_date - {cycle_date}")
    cycle_date = pendulum.from_format(cycle_date,'YYYY-MM-DD')
    cycle_date = cycle_date.format('YYYY-MM-DD')
    logger.info(f"Date formatted cycle_date - {cycle_date}")

    sql = """INSERT INTO AAP_DAT_{}_DB.PUBLIC.DAT_LOAD_CYCLE_DATES (LOAD_DATE,LOAD_STATUS) VALUES ('{}','Completed')""".format(env,cycle_date)
    # print(sql)
    # dwh_hook = SnowflakeHook(snowflake_conn_id="dat_sf_con_nonprod")
    engine = dwh_hook.get_sqlalchemy_engine()
    with engine.connect().execution_options(autocommit=False) as connection:
        try:
            connection.execute("BEGIN")
            connection.execute(sql)
            connection.execute("COMMIT")
        except:
            connection.execute("ROLLBACK")
            logger.error("Failed to insert next days cycle date into DAT_LOAD_CYCLE_DATES table")
            raise
        finally:
            connection.close()
    engine.dispose()
