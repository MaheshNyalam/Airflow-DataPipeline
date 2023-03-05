import pandas as pd
# import datetime
import pendulum
import time
from pytz import timezone
import logging
from common_functions import *
logger = logging.getLogger(__name__)

def incr_load_files(env="dev", feedname=None, df=None, batchnum=3, email_alert_dl="mahesh.nyalam@advance-auto.com",task_group_name=None,cycle_date=None):
    feedname_for_PT = f"DAT/{feedname}"
    load_type='INCR_WITHOUT_PARTITION'
    upd_airflow_flag_info = "update the airflow-flag to Y for the records of filename --> {}"
    copy_raw_fl_info = "copying the current file --> {} to final raw location"
    success_body = "The processing of file - {} is successfull."
    process_time_info = "The process completed time for file - {} --> {}"
    # process_tracker_info = "file_status from process tracker for the file {} --> {}"
    df.sort_values(by=['FILE_DATE','EVENT_TIME'],ascending=True,inplace=True)
    df = df.reset_index(drop=True)
    for i,j in df.iterrows():
        # bucket_name, file_name, s3_pt_object, s3_stg_object, current_fl_date, current_fl_datetime = fetch_values_from_feed_dataframe(df,i,feedname)
        file_info_dict = fetch_values_from_feed_dataframe(df,i,feedname)
        file_name = file_info_dict['file_name']
        s3_pt_object = file_info_dict['s3_pt_object']
        s3_stg_object = file_info_dict['s3_stg_object']
        current_fl_date = file_info_dict['current_fl_date']
        current_fl_datetime = file_info_dict['current_fl_datetime']
        last_fl_status , max_date_value = query_process_tracker(feedname_for_PT)
        logger.info(f"current_fl_date - {current_fl_date}, max_date_value - {max_date_value}, last_fl_status - {last_fl_status}")
        if last_fl_status == 'Processed':
            file_status, info_status = common_copy_and_wait_function(env,feedname,batchnum,load_type,max_date_value,file_info_dict,cycle_date)
            if ((file_status == 'Processed') or (info_status == 'success')):
                logger.info(f"{success_body}".format(file_name))
            else:
                send_email_by_status(env,info_status,feedname,file_name,batchnum,email_alert_dl,task_group_name)
        # If the last processed file status is Pending/InProcess, wait for some time and recheck
        elif last_fl_status in ('InProcess','Pending'):
            while last_fl_status not in ('Processed','NotProcessed'):
                #wait for some time to process the previous file 
                #wait time should be dependent on the feed?
                time.sleep(60)
                last_fl_status , max_date_value = query_process_tracker(feedname_for_PT)
            if last_fl_status == 'Processed':  
                file_status, info_status = common_copy_and_wait_function(env,feedname,batchnum,load_type,max_date_value,file_info_dict)
            else:
                file_status = 'Skipped'
                info_status = 'old_failure'
                copied_time_stamp = ''
                # completed_time = datetime.datetime.now(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S")
                completed_time = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD HH:mm:ss')
                update_dds_load_status(env,file_status,file_name,current_fl_datetime,completed_time)
                insert_into_daily_airflow_status_table(env,feedname,batchnum,load_type,file_status,completed_time,file_info_dict,copied_time_stamp,cycle_date)
                # send_email_by_status(env,"old_failure",feedname,file_name,batchnum,email_alert_dl)
            if ((file_status == 'Processed') or (info_status == 'success')):
                logger.info(f"{success_body}".format(file_name))
            else:
                send_email_by_status(env,info_status,feedname,file_name,batchnum,email_alert_dl,task_group_name)
            
        # If the last processed file status is Failed/NotProcessed then do not proceed with the current file. Also, notify Ops team
        else:
            if current_fl_date > max_date_value:
                file_status = 'Skipped'
                info_status = "old_failure"
                copied_time_stamp = ''
                # send_email_by_status(env,"old_failure",feedname,file_name,batchnum,email_alert_dl)
            elif current_fl_date == max_date_value:
                info_status = "start_reprocess"
                send_email_by_status(env,info_status,feedname,file_name,batchnum,email_alert_dl,task_group_name)
                logger.info(f"{upd_airflow_flag_info}".format(file_name))
                update_sf_table(env,file_name,current_fl_datetime)
                logger.info(f"{copy_raw_fl_info}".format(file_name))
                copied_time_stamp = move_s3stage_to_s3raw(s3_stg_object,feedname,file_name)
                file_status = feed_load_wait_loop(feedname,s3_pt_object,file_name)
                if file_status == 'Processed':
                    info_status = 'success'
                else:
                    info_status = 'fail'
            else:
                file_status = 'Skipped'
                info_status = "future_failure"
                copied_time_stamp = ''
            #    send_email_by_status(env,"future_failure",feedname,file_name,batchnum,email_alert_dl)
            # completed_time = datetime.datetime.now(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S")
            completed_time = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD HH:mm:ss')
            logger.info(f"{process_time_info}".format(file_name,completed_time))
            update_dds_load_status(env,file_status,file_name,current_fl_datetime,completed_time)
            insert_into_daily_airflow_status_table(env,feedname,batchnum,load_type,file_status,completed_time,file_info_dict,copied_time_stamp,cycle_date)
            if ((file_status == 'Processed') or (info_status == 'success')):
                logger.info(f"{success_body}".format(file_name))
            else:
                send_email_by_status(env,info_status,feedname,file_name,batchnum,email_alert_dl,task_group_name)


def full_load_files(env="dev",feedname=None, df=None, batchnum=3, email_alert_dl="mahesh.nyalam@advance-auto.com",task_group_name=None,cycle_date=None):
    load_type='FULL_LOAD'
    feedname_for_PT = f"DAT/{feedname}"
    upd_airflow_flag_info = "update the airflow-flag to Y for the records of filename --> {}"
    copy_raw_fl_info = "copying the current file --> {} to final raw location"
    success_body = "The processing of file - {} is successfull."
    process_time_info = "The process completed time for file - {} --> {}"
    # process_tracker_info = "file_status from process tracker for the file {} --> {}"
    # bucket_name, file_name, s3_pt_object, s3_stg_object, current_fl_date, current_fl_datetime = fetch_values_from_feed_dataframe(df,0,feedname)
    file_info_dict = fetch_values_from_feed_dataframe(df,0,feedname)
    file_name = file_info_dict['file_name']
    s3_pt_object = file_info_dict['s3_pt_object']
    s3_stg_object = file_info_dict['s3_stg_object']
    current_fl_date = file_info_dict['current_fl_date']
    current_fl_datetime = file_info_dict['current_fl_datetime']
    last_fl_status , max_date_value = query_process_tracker(feedname_for_PT)
    if current_fl_date >= max_date_value:
        logger.info(f"{upd_airflow_flag_info}".format(file_name))
        update_sf_table(env,file_name,current_fl_datetime)
        logger.info(f"{copy_raw_fl_info}".format(file_name))
        copied_time_stamp = move_s3stage_to_s3raw(s3_stg_object,feedname,file_name)
        file_status = feed_load_wait_loop(feedname,s3_pt_object,file_name)
        # completed_time = datetime.datetime.now(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S")
        completed_time = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD HH:mm:ss')
        logger.info(f"{process_time_info}".format(file_name,completed_time))
        update_dds_load_status(env,file_status,file_name,current_fl_datetime,completed_time)
        insert_into_daily_airflow_status_table(env,feedname,batchnum,load_type,file_status,completed_time,file_info_dict,copied_time_stamp,cycle_date)
        if file_status == 'Processed':
            logger.info(f"{success_body}".format(file_name))
        else:
            info_status="fail"
            send_email_by_status(env,info_status,feedname,file_name,batchnum,email_alert_dl,task_group_name)
    else:
        file_status = "Skipped"
        info_status="skip"
        copied_time_stamp = ''
        # completed_time = datetime.datetime.now(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S")
        completed_time = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD HH:mm:ss')
        update_dds_load_status(env,file_status,file_name,current_fl_datetime,completed_time)
        insert_into_daily_airflow_status_table(env,feedname,batchnum,load_type,file_status,completed_time,file_info_dict,copied_time_stamp,cycle_date)
        send_email_by_status(env,info_status,feedname,file_name,batchnum,email_alert_dl,task_group_name)
        

def partition_load_files(env="dev",feedname=None, df=None, batchnum=3, email_alert_dl="mahesh.nyalam@advance-auto.com",task_group_name=None,cycle_date=None):
    load_type = 'INCR_WITH_PARTITION'
    # feedname_for_PT = f"DAT/{feedname}"
    upd_airflow_flag_info = "update the airflow-flag to Y for the records of filename --> {}"
    copy_raw_fl_info = "copying the current file --> {} to final raw location"
    success_body = "The processing of file - {} is successfull."
    process_time_info = "The process completed time for file - {} --> {}"
    process_tracker_info = "file_status from process tracker for the file {} --> {}"
    df.sort_values(by=['FILE_DATE','EVENT_TIME'],ascending=True,inplace=True)
    df = df.reset_index(drop=True)
    for i,j in df.iterrows():
        # bucket_name, file_name, s3_pt_object, s3_stg_object, current_fl_date, current_fl_datetime = fetch_values_from_feed_dataframe(df,i,feedname)
        file_info_dict = fetch_values_from_feed_dataframe(df,i,feedname)
        file_name = file_info_dict['file_name']
        s3_pt_object = file_info_dict['s3_pt_object']
        s3_stg_object = file_info_dict['s3_stg_object']
        # current_fl_date = file_info_dict['s3_stg_object']
        current_fl_datetime = file_info_dict['current_fl_datetime']
        feedname_for_PT = f"DAT/{feedname}"
        file_status = check_status_from_PT(feedname_for_PT,s3key=s3_pt_object)
        logger.info(f"{process_tracker_info}".format(file_name,file_status))
        # last_fl_status , max_date_value = query_process_tracker(feedname_for_PT)
        if file_status == 'Processed':
            file_status = "Skipped"
            info_status="try_reprocess"
            copied_time_stamp = ''
            # completed_time = datetime.datetime.now(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S")
            completed_time = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD HH:mm:ss')
            update_dds_load_status(env,file_status,file_name,current_fl_datetime,completed_time)
            insert_into_daily_airflow_status_table(env,feedname,batchnum,load_type,file_status,completed_time,file_info_dict,copied_time_stamp,cycle_date)
            send_email_by_status(env,info_status,feedname,file_name,batchnum,email_alert_dl,task_group_name)
        elif file_status in ('InProcess','Pending'):
            logger.info(f"The file - {file_name} is still in processing which might have been triggered in the last batch. Hence skipping the file processing in this batch")
            # file_status = feed_load_wait_loop(s3_pt_object,file_name,logger_info_dict)
            # completed_time = datetime.now(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S")
            # logger.info(f"{logger_info_dict['process_time_info']}".format(file_name,completed_time))
            # update_dds_load_status(env,file_status,file_name,current_fl_datetime,completed_time)
            file_status = "Skipped"
            info_status = "already_in_processing"
            copied_time_stamp = ''
            # completed_time = datetime.datetime.now(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S")
            completed_time = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD HH:mm:ss')
            update_dds_load_status(env,file_status,file_name,current_fl_datetime,completed_time)
            insert_into_daily_airflow_status_table(env,feedname,batchnum,load_type,file_status,completed_time,file_info_dict,copied_time_stamp,cycle_date)
            # if file_status == 'Processed':
            #     logger.info(f"{logger_info_dict['success_body']}".format(file_name))
            # else:
            #     info_status="fail"
            send_email_by_status(env,info_status,feedname,file_name,batchnum,email_alert_dl,task_group_name)
        else:
            if file_status != 'continue':
                info_status = "start_reprocess"
                send_email_by_status(env,info_status,feedname,file_name,batchnum,email_alert_dl,task_group_name)
            logger.info(f"{upd_airflow_flag_info}".format(file_name))
            update_sf_table(env,file_name,current_fl_datetime)
            logger.info(f"{copy_raw_fl_info}".format(file_name))
            copied_time_stamp = move_s3stage_to_s3raw(s3_stg_object,feedname,file_name)
            file_status = feed_load_wait_loop(feedname,s3_pt_object,file_name)
            # completed_time = datetime.datetime.now(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S")
            completed_time = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD HH:mm:ss')
            logger.info(f"{process_time_info}".format(file_name,completed_time))
            update_dds_load_status(env,file_status,file_name,current_fl_datetime,completed_time)
            insert_into_daily_airflow_status_table(env,feedname,batchnum,load_type,file_status,completed_time,file_info_dict,copied_time_stamp,cycle_date)
            if file_status == 'Processed':
                logger.info(f"{success_body}".format(file_name))
            else:
                info_status="fail"
                send_email_by_status(env,info_status,feedname,file_name,batchnum,email_alert_dl,task_group_name)