import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key, Attr
# import datetime
import pendulum
from pytz import timezone
import time
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.email import send_email
import logging
from sqlalchemy import create_engine
import re
logger = logging.getLogger(__name__)


dwh_hook = SnowflakeHook(snowflake_conn_id="dat_sf_con_nonprod")
client = boto3.resource('dynamodb')
engine = dwh_hook.get_sqlalchemy_engine()

def get_date_value(file_name,s3_obj=False):
    if s3_obj:
        file_name = file_name.split("/")[-1]
    dateformatRegex1 = re.compile(r'^[A-Za-z0-9_-]*(-|_)[0-1]{1}\d{1}-[0-3]{1}\d{1}-\d{2}')
    dateformatRegex2 = re.compile(r'^[A-Za-z0-9_-]*(-|_)[0-1]{1}\d{1}_[0-3]{1}\d{1}_\d{2}')
    value = dateformatRegex1.search("{}".format(file_name))
    print("value -->",value)
    if value:
        file_and_date_pattern = value.group()
        date_pattern = file_and_date_pattern[-8:]
        logger.info("extracted date pattern for filename -{} is {}".format(file_name,date_pattern))
        pattern = '%m-%d-%y'
        # date_value = pd.to_datetime(date_pattern,format='%m-%d-%y')
    else:
        value = dateformatRegex2.search("{}".format(file_name))
        if value:
            file_and_date_pattern = value.group()
            date_pattern = file_and_date_pattern[-8:]
            logger.info("extracted date pattern for filename -{} is {}".format(file_name,date_pattern))
            pattern = '%m_%d_%y'
            # date_value = pd.to_datetime(date_pattern,format='%m_%d_%y')
        else:
            logger.info("Error fetching the date pattern for file - {}. Hence setting some standard pattern".format(file_name))
            pattern = '%m_%d_%y'
            # date_value = pd.to_datetime("01_01_22",format='%m_%d_%y')
    try:
        date_value = pd.to_datetime(date_pattern,format='{}'.format(pattern))
        logger.info("date for filename -{} is {}".format(file_name,date_value))
    except:
        date_value = pd.to_datetime("01_01_22",format='{}'.format(pattern))
        logger.info("date for filename -{} is {}".format(file_name,date_value))

    return date_value

# def get_date_value(date_pattern):
# if re.search('[0-9]{2}-[0-9]{2}-[0-9]{2}', date_pattern):
#     return 
# elif re.search('[0-9]{2}_[0-9]{2}_[0-9]{2}', date_pattern):
#     return pd.to_datetime(date_pattern,format='%m_%d_%y')
# else:
#     return date(2000,1,1)

def convert_to_standard_ts(event_time):
    logger.info(f"event_time - {event_time}")
    time_stamp_value = pendulum.from_format(event_time.split('.')[0],'YYYY-MM-DD HH:mm:ss')
    time_stamp_value = time_stamp_value.format('YYYY-MM-DD HH:mm:ss')
    return time_stamp_value

		
def get_ctl_table_data(env="dev",feedname=None):
    # dwh_hook = SnowflakeHook(snowflake_conn_id="dat_sf_con_nonprod")
    sql="""SELECT * FROM AAP_DAT_{}_DB.PUBLIC.DAT_AIRFLOW_SNS_EVENT_AUDIT_TABLE WHERE LOAD_TYPE IN ('FULL_LOAD','INCR_LOAD_WITHOUT_PARTITION','INCR_LOAD_WITH_PARTITION') AND PROCESS_TRACKER_STATUS IS NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY FILE_NAME ORDER BY ID DESC) = 1""".format(env)
    try:
        files_to_be_processed_df = dwh_hook.get_pandas_df(sql)
        logger.info("Successfully fetched the data from Airflow SNS table")
    except:
        logger.error("Failed to fetch the data from Airflow SNS table")
        raise
    if feedname:
        # files_to_be_processed_df['FEEDNAME'] = files_to_be_processed_df['FILE_NAME'].str.slice(0, -24)
        print(files_to_be_processed_df)
        filter = (
            files_to_be_processed_df["FEED_NAME"] == feedname
        )
        files_to_be_processed_df.where(filter, inplace=True)
        files_to_be_processed_df.dropna(how="all",axis=0,subset=['FILE_NAME'],inplace=True)
        print(files_to_be_processed_df)
        # print(files_to_be_processed_df['FILE_NAME'].str.slice(-23, -15))
        files_to_be_processed_df['FILE_DATE'] =  files_to_be_processed_df['FILE_NAME'].apply(get_date_value)
        # files_to_be_processed_df['EVENT_TIME'] =  files_to_be_processed_df['EVENT_TIME'].apply(convert_to_standard_ts)
    else:
        files_to_be_processed_df.dropna(how="all",axis=0,subset=['FILE_NAME'],inplace=True)
        # files_to_be_processed_df['FEEDNAME'] = files_to_be_processed_df['FILE_NAME'].str.slice(0, -24)
    return files_to_be_processed_df

def query_metadata_table(feedname=None):
    # client = boto3.resource('dynamodb') 
    load_types = ['FULL_LOAD','INCR_LOAD_WITHOUT_PARTITION','INCR_LOAD_WITH_PARTITION']
    if feedname:
        try:
            response = client.Table('Metadata_Table').scan(FilterExpression=Attr('Project_Name').eq('DAT') & Attr('Dataset_Name').eq(feedname))
            logger.info("Successfully fetched data from Metadata_Table DynamoDB Table")
        except:
            logger.error("Failed to fetch data from Metadata_Table DynamoDB Table")
            raise
        df1 = pd.DataFrame(response['Items'])
        df2 = df1[df1.Load_Type.isin(load_types)]
        df3 = df2[["Dataset_Name"]]
    else:
        try:
            response = client.Table('Metadata_Table').scan(FilterExpression=Attr('Project_Name').eq('DAT'))
            logger.info("Successfully fetched data from Metadata_Table DynamoDB Table")
        except:
            logger.error("Failed to fetch data from Metadata_Table DynamoDB Table")
            raise
        df1 = pd.DataFrame(response['Items'])
        df2 = df1[df1.Load_Type.isin(load_types)]
        df3 = df2[["Dataset_Name","Load_Type"]]
    return df3

def query_process_tracker(feedname_for_PT=None):
    # client = boto3.resource('dynamodb')
    try:
        response1 = client.Table('DatasetProcessTracker').query(KeyConditionExpression=Key('DatasetName').eq(feedname_for_PT))
        logger.info("Successfully fetched data from DataProcessTracker DynamoDB Table")
    except:
        logger.error("Failed to fetch data from DataProcessTracker DynamoDB Table")
        raise
    # response = client.Table('DatasetProcessTracker').scan(FilterExpression=Attr('DatasetName').eq(feedname_for_PT) & Attr('FileName').eq(s3_keyname))
    process_tracker_feed_status_df1 = pd.DataFrame(response1['Items'])
    try:
        response2 = client.Table('DatasetProcessTracker_Bkp').query(KeyConditionExpression=Key('DatasetName').eq(feedname_for_PT))
        logger.info("Successfully fetched data from DatasetProcessTracker_Bkp DynamoDB Table")
    except:
        logger.error("Failed to fetch data from DatasetProcessTracker_Bkp DynamoDB Table")
        raise
    process_tracker_feed_status_df2 = pd.DataFrame(response2['Items'])
    process_tracker_feed_status_df = pd.concat([process_tracker_feed_status_df1,process_tracker_feed_status_df2]).drop_duplicates()
    if process_tracker_feed_status_df.shape[0] != 0:
        process_tracker_feed_status_df.sort_values(by=['EndTime'],ascending=False,inplace=True)
        process_tracker_feed_status_df = process_tracker_feed_status_df.iloc[:5]
        process_tracker_feed_status_df['PROCESSED_FILE_DATES'] = process_tracker_feed_status_df['FileName'].apply(get_date_value,s3_obj=True)
        last_fl_status, max_date_value = process_tracker_feed_status_df.sort_values(by=['PROCESSED_FILE_DATES'],ascending=False).iloc[0][['Status','PROCESSED_FILE_DATES']]
    else:
        logger.info("As there are no records for this feed in the process tracker, setting some default values to continue the processing")
        last_fl_status = 'Processed'
        max_date_value = pendulum.datetime(2022,1,1).format('YYYY-MM-DD')
        
    return last_fl_status, max_date_value

def fetch_values_from_feed_dataframe(df,i,feedname):
    bucket_name = df.iloc[i]['RAW_BUCKET_NAME']
    logger.info(f"bucket_name --> {bucket_name}")
    file_name = df.iloc[i]['FILE_NAME']
    logger.info(f"file_name --> {file_name}")
    s3_pt_object=f"s3://{bucket_name}/DAT/{feedname}/{file_name}"
    logger.info(f"s3 target object with full path --> {s3_pt_object}")
    s3_stg_object = df.iloc[i]['S3_PATH']
    logger.info(f"s3 source object with full path --> {s3_stg_object}")
    current_fl_date= df.iloc[i]['FILE_DATE']
    logger.info(f"current_fl_date --> {current_fl_date}")
    current_fl_datetime = df.iloc[i]['EVENT_TIME']
    logger.info(f"current_file_datetime --> {current_fl_datetime}")
    file_info_dict = {'bucket_name':bucket_name,'file_name':file_name,'s3_pt_object':s3_pt_object,'s3_stg_object':s3_stg_object,'current_fl_date':current_fl_date,'current_fl_datetime':current_fl_datetime}
    return file_info_dict


def check_status_from_PT(feedname_for_PT,s3key=None):
    # client = boto3.resource('dynamodb')
    # response1 = client.Table('DatasetProcessTracker').scan(FilterExpression=Attr('FileName').eq(s3key))
    try:
        response1 = client.Table('DatasetProcessTracker').query(KeyConditionExpression=Key('DatasetName').eq(feedname_for_PT))
        logger.info("Successfully fetched data from DatasetProcessTracker DynamoDB Table")
    except:
        logger.error("Failed to fetch data from DatasetProcessTracker DynamoDB Table")
        raise
    process_tracker_file_status_df1 = pd.DataFrame(response1['Items'])
    # print("process_tracker_file_status_df1 keys -->",process_tracker_file_status_df1.keys())
    # process_tracker_file_status_df1 = process_tracker_file_status_df1[process_tracker_file_status_df1.FileName == s3key]
    if process_tracker_file_status_df1.shape[0] != 0:
        print("process_tracker_file_status_df1 keys -->",process_tracker_file_status_df1.keys())
        process_tracker_file_status_df1 = process_tracker_file_status_df1[process_tracker_file_status_df1.FileName == s3key]
        if process_tracker_file_status_df1.shape[0] != 0:
            file_status = process_tracker_file_status_df1.iloc[0]['Status']
        else:
            # response2 = client.Table('DatasetProcessTracker_Bkp').scan(FilterExpression=Attr('FileName').eq(s3key))
            try:
                response2 = client.Table('DatasetProcessTracker_Bkp').query(KeyConditionExpression=Key('DatasetName').eq(feedname_for_PT))
                logger.info("Successfully fetched data from DatasetProcessTracker_Bkp DynamoDB Table")
            except:
                logger.error("Failed to fetch data from DatasetProcessTracker_Bkp DynamoDB Table")
                raise
            process_tracker_file_status_df2 = pd.DataFrame(response2['Items'])
            if process_tracker_file_status_df2.shape[0] != 0:
                print("process_tracker_file_status_df2 keys -->",process_tracker_file_status_df2.keys())
                process_tracker_file_status_df2 = process_tracker_file_status_df2[process_tracker_file_status_df2.FileName == s3key]
                print("process_tracker_file_status DF from BKP table")
                print(process_tracker_file_status_df2)
                if process_tracker_file_status_df2.shape[0] != 0:
                    # process_tracker_file_status_df2 = process_tracker_file_status_df2[process_tracker_file_status_df2.FileName == s3key]
                    file_status = process_tracker_file_status_df2.iloc[0]['Status']
                else:
                    file_status = 'continue'
            else:
                file_status = 'continue'
    else:
        try:
            response2 = client.Table('DatasetProcessTracker_Bkp').query(KeyConditionExpression=Key('DatasetName').eq(feedname_for_PT))
            logger.info("Successfully fetched data from DatasetProcessTracker_Bkp DynamoDB Table")
        except:
            logger.error("Failed to fetch data from DatasetProcessTracker_Bkp DynamoDB Table")
            raise
        process_tracker_file_status_df2 = pd.DataFrame(response2['Items'])
        if process_tracker_file_status_df2.shape[0] != 0:
            print("process_tracker_file_status_df2 keys -->",process_tracker_file_status_df2.keys())
            process_tracker_file_status_df2 = process_tracker_file_status_df2[process_tracker_file_status_df2.FileName == s3key]
            print("process_tracker_file_status DF from BKP table")
            print(process_tracker_file_status_df2)
            if process_tracker_file_status_df2.shape[0] != 0:
                # process_tracker_file_status_df2 = process_tracker_file_status_df2[process_tracker_file_status_df2.FileName == s3key]
                file_status = process_tracker_file_status_df2.iloc[0]['Status']
            else:
                file_status = 'continue'
        else:
            file_status = 'continue'

    return file_status


def update_sf_table(env,file_name,current_fl_datetime):
    filename_to_update = file_name
    # dwh_hook = SnowflakeHook(snowflake_conn_id="dat_sf_con_nonprod")
    sql="UPDATE AAP_DAT_{}_DB.PUBLIC.DAT_AIRFLOW_SNS_EVENT_AUDIT_TABLE SET AIRFLOW_FLAG='Y', PROCESS_TRACKER_STATUS='InProcess' WHERE FILE_NAME='{}' and EVENT_TIME='{}'".format(env,filename_to_update,current_fl_datetime)
    # engine = dwh_hook.get_sqlalchemy_engine()
    with engine.connect().execution_options(autocommit=False) as connection:
        try:
            connection.execute("BEGIN")
            connection.execute(sql)
            connection.execute("COMMIT")
        except:
            connection.execute("ROLLBACK")
            logger.error("Failed to update the Flag to Y on Airflow SNS table")
            raise
        finally:
            connection.close()
    engine.dispose()


def upsert_data_audit_log_table(env="dev",feedname=None):
    # dwh_hook = SnowflakeHook(snowflake_conn_id="dat_sf_con_nonprod")
    sql=f"SELECT * FROM AAP_DAT_{env}_DB.LOCATION.AAP_DAT_INTERFACE_FREQ WHERE AAP_FILENAME={feedname}"
    try:
        feed_info_df = dwh_hook.get_pandas_df(sql)
        logger.info("Successfully fetched the data from DAT_INTERFACE_FREQ table")
    except:
        logger.error("Failed to fetch the data DAT_INTERFACE_FREQ table")
        raise
    if feedname:
        # files_to_be_processed_df['FEEDNAME'] = files_to_be_processed_df['FILE_NAME'].str.slice(0, -24)
        print(files_to_be_processed_df)
        filter = (
            files_to_be_processed_df["FEED_NAME"] == feedname
        )
        files_to_be_processed_df.where(filter, inplace=True)
        files_to_be_processed_df.dropna(how="all",axis=0,subset=['FILE_NAME'],inplace=True)
        print(files_to_be_processed_df)
        # print(files_to_be_processed_df['FILE_NAME'].str.slice(-23, -15))
        files_to_be_processed_df['FILE_DATE'] =  files_to_be_processed_df['FILE_NAME'].apply(get_date_value)
        # files_to_be_processed_df['EVENT_TIME'] =  files_to_be_processed_df['EVENT_TIME'].apply(convert_to_standard_ts)
    else:
        files_to_be_processed_df.dropna(how="all",axis=0,subset=['FILE_NAME'],inplace=True)
        # files_to_be_processed_df['FEEDNAME'] = files_to_be_processed_df['FILE_NAME'].str.slice(0, -24)
    return files_to_be_processed_df

def feed_load_wait_loop(feedname,s3_pt_object,file_name):
    file_status = 'dummy'
    x=0
    process_tracker_info = "file_status from process tracker for the file {} --> {}"
    while file_status not in ('Processed','NotProcessed'):
        #wait for some time to process the current file 
        # #wait time should be dependent on the feed?
        time.sleep(60)
        x=x+1
        logger.info(f"running wait loop --> {x}")
        feedname_for_PT = f"DAT/{feedname}"
        file_status = check_status_from_PT(feedname_for_PT,s3key=s3_pt_object)
        logger.info(f"{process_tracker_info}".format(file_name,file_status))
    return file_status

        
def send_email_by_status(env,status,feedname,file_name,batchnum,email_alert_dl="mahesh.nyalam@advance-auto.com",task_group_name=None):
    if status == 'fail':
        body = "The processing of file - {} is not successfull in {}. Please check Talend Logs for more details."
        subject = (f"ERROR!! {env} DAG - dds_files_copy_by_load_batch_{batchnum}.{task_group_name}: File Processing failed for the feed - {feedname}")
    elif status == 'try_reprocess':
        body = "The file - {} got processed already in {} in the last batch run. In case you want to reprocess the file then first update the status for this file in ProcessTrackerTable to NotProcessed and then proceed."
        subject = (f"WARNING!! {env} DAG - dds_files_copy_by_load_batch_{batchnum}.{task_group_name}: File Processed already for the feed - {feedname}")
    elif status == 'skip':
        body = "The processing of the file - {} has been skipped in {} as it is older than latest processed file."
        subject = (f"WARNING!! {env} DAG - dds_files_copy_by_load_batch_{batchnum}.{task_group_name}: File Processing skipped for the feed - {feedname}")
    elif status == 'already_in_processing':
        body = "The processing of the file - {} has been skipped in {} as the same file got triggered already in the last batch and is still under processing. Check Talend Job for more details."
        subject = (f"WARNING!! {env} DAG - dds_files_copy_by_load_batch_{batchnum}.{task_group_name}: File Processing skipped for the feed - {feedname}")
    else:
        if status == 'old_failure':
            body = "Processing of previous latest file Failed/Not Processed, hence cannot process the current file - {} in {}. In case you want to process the current file then first process the old file which is in failed state (or) mark the status of that old file to Processed in ProcessTrackerStatus table."
            subject = (f"ERROR!! {env} DAG - dds_files_copy_by_load_batch_{batchnum}.{task_group_name}: File Processing haulted for the feed - {feedname}")
        elif status == 'start_reprocess':
            body = "The file - {} which got failed in {} in the last batch run is starting to get reprocessed. Ignore this mail incase you already knew about this."
            subject = (f"WARNING!! {env} DAG - dds_files_copy_by_load_batch_{batchnum}.{task_group_name}: File reprocessing has started for the feed - {feedname}")
        else:
            body = "The file - {} is no more valid in {} as the files later to this date's file are NotProcessed/Failed. In case you want to reprocess the file(which is rare in the case of INCR_WITHOUT_PARTITION load for this scenario), remove the entries of files(entries latest to the current file and also current file entry) from ProcessTrackerTable."
            subject = (f"ERROR!! {env} DAG - dds_files_copy_by_load_batch_{batchnum}.{task_group_name}: File Processing haulted for the feed - {feedname}")
    msg = f"Hi,<br><br>{body} <br><br>Thanks,<br>Airflow".format(file_name,env)
    subject_p = f"{subject}"
    try:
        send_email(to=email_alert_dl, subject=subject_p, html_content=msg)
        logger.info("successfully sent the mail")
    except:
        logger.error("failed to sent the mail")
        raise
    if status in ['try_reprocess','skip','start_reprocess']:
        logger.info(f"{body}".format(file_name,env))
    else:
        logger.error(f"{body}".format(file_name,env))
        raise

def update_dds_load_status(env,file_status,file_name,current_fl_datetime, completed_time):
    filename_to_update = file_name
    # dwh_hook = SnowflakeHook(snowflake_conn_id="dat_sf_con_nonprod")
    sql=f"UPDATE AAP_DAT_{env}_DB.PUBLIC.DAT_AIRFLOW_SNS_EVENT_AUDIT_TABLE SET PROCESS_TRACKER_STATUS='{file_status}', PROCESSED_DTTM='{completed_time}' WHERE FILE_NAME='{filename_to_update}' AND EVENT_TIME='{current_fl_datetime}'"
    logger.info(sql)
    # engine = dwh_hook.get_sqlalchemy_engine()
    with engine.connect().execution_options(autocommit=False) as connection:
        try:
            connection.execute("BEGIN")
            connection.execute(sql)
            connection.execute("COMMIT")
        except:
            connection.execute("ROLLBACK")
            logger.error("Failed to Update the Airflow SNS table")
            raise
        finally:
            connection.close()
    engine.dispose()

def move_s3stage_to_s3raw(s3_filepath,s3_feed_name,s3_file_name):
    source_bucket, source_key = s3_filepath.replace("s3://", "").split("/",maxsplit=1)
    target_bucket = source_bucket
    s3_resource = boto3.resource('s3')
    tgt_bucket_obj = s3_resource.Bucket('{}'.format(target_bucket))
    # source_bucket = 'mwaa-aap-audit-report-dev'
    copy_source = {
        'Bucket': source_bucket,
        'Key': source_key}
    # bucket = s3.Bucket('{}'.format(target_bucket))
    target_key = 'DAT/{0}/{1}'.format(s3_feed_name,s3_file_name)
    try:
        tgt_bucket_obj.copy(copy_source, target_key)
        logger.info("copy for the file --> {} is successful".format(s3_file_name))
        copied_time_stamp = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD HH:mm:ss')
        try:
            s3_resource.Object(source_bucket,source_key).delete()
            logger.info("deleted the file --> {} is successful".format(s3_file_name))
        except:
            logger.error("deleting the file --> {} is not successful".format(s3_file_name))
    except:
        logger.error("copy for the file --> {} failed".format(s3_file_name))
        raise
    return copied_time_stamp


def insert_into_daily_airflow_status_table(env,feedname,batchnum,load_type,file_status,completed_time,file_info_dict,copied_time_stamp,cycle_date):
    bucket_name = file_info_dict['bucket_name']
    file_name = file_info_dict['file_name']
    current_fl_date = file_info_dict['current_fl_date']
    event_time = file_info_dict['current_fl_datetime']  
    current_fl_datetime = convert_to_standard_ts(event_time)
    cycle_date = pendulum.from_format(cycle_date,'YYYY-MM-DD')
    cycle_date = cycle_date.format('YYYY-MM-DD')
    sql=f"INSERT INTO AAP_DAT_{env}_DB.LOCATION.DAT_AIRFLOW_DAILY_STATUS VALUES ('{cycle_date}','{batchnum}','{current_fl_date}','{file_name}','{feedname}','{bucket_name}','{load_type}','{file_status}','{current_fl_datetime}','{copied_time_stamp}','{completed_time}')"
    logger.info(sql)
    engine = dwh_hook.get_sqlalchemy_engine()
    with engine.connect().execution_options(autocommit=False) as connection:
        try:
            connection.execute("BEGIN")
            connection.execute(sql)
            connection.execute("COMMIT")
        except:
            connection.execute("ROLLBACK")
            logger.error("Failed to insert record into DAT_AIRFLOW_DAILY_STATUS table")
            raise
        finally:
            connection.close()
    engine.dispose()


def common_copy_and_wait_function(env,feedname,batchnum,load_type,max_date_value,file_info_dict,cycle_date):
    # bucket_name = file_info_dict['bucket_name']
    file_name = file_info_dict['file_name']
    s3_pt_object = file_info_dict['s3_pt_object']
    s3_stg_object = file_info_dict['s3_stg_object']
    current_fl_date = file_info_dict['current_fl_date']
    current_fl_datetime = file_info_dict['current_fl_datetime']

    upd_airflow_flag_info = "update the airflow-flag to Y for the records of filename --> {}"
    copy_raw_fl_info = "copying the current file --> {} to final raw location"
    # success_body = "The processing of file - {} is successfull."
    process_time_info = "The process completed time for file - {} --> {}"
    # process_tracker_info = "file_status from process tracker for the file {} --> {}"

    if current_fl_date > max_date_value:
        logger.info(f"{upd_airflow_flag_info}".format(file_name))
        update_sf_table(env,file_name,current_fl_datetime)
        logger.info(f"{copy_raw_fl_info}".format(file_name))
        copied_time_stamp = move_s3stage_to_s3raw(s3_stg_object,feedname,file_name)
        file_status = feed_load_wait_loop(feedname,s3_pt_object,file_name)
        if file_status == 'Processed':
            info_status = 'success'
        else:
            info_status = 'fail'

    elif current_fl_date == max_date_value:
        file_status = 'Skipped'
        info_status = 'try_reprocess'
        copied_time_stamp = ''
    else:
        file_status = 'Skipped'
        info_status = 'skip'
        copied_time_stamp = ''
    completed_time = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD HH:mm:ss')
    logger.info(f"{process_time_info}".format(file_name,completed_time))
    update_dds_load_status(env,file_status,file_name,current_fl_datetime, completed_time)
    insert_into_daily_airflow_status_table(env,feedname,batchnum,load_type,file_status,completed_time,file_info_dict,copied_time_stamp,cycle_date)
    
    return file_status, info_status