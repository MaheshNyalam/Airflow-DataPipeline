import sys
import os
import pandas as pd
from airflow.utils.email import send_email
sys.path.append(os.path.dirname(__file__))
from common_functions import *
from copy_by_load_functions import *
import logging
logger = logging.getLogger(__name__)


def main(*op_args):
    env = op_args[0].upper()
    feedname = op_args[1]
    load_type = op_args[2]
    batchnum = op_args[3]
    email_alert_dl = op_args[4]
    task_group_name = op_args[5]
    cycle_date = op_args[6]
    # logger_info_dict = {"upd_airflow_flag_info" : "update the airflow-flag to Y for the records of filename --> {}",
    # "copy_raw_fl_info" : "copying the current file --> {} to final raw location",
    # "success_body" : "The processing of file - {} is successfull.",
    # "process_time_info" : "The process completed time for file - {} --> {}",
    # "process_tracker_info" : "file_status from process tracker for the file {} --> {}"}
    if ((feedname != 'DUMMY_FEED') or (load_type != 'DUMMY')):
        files_to_be_processed_df = get_ctl_table_data(env,feedname)
        print("files_to_be_processed_df keys --> ",files_to_be_processed_df.keys())
        print(files_to_be_processed_df)
        feedinfo_from_MD_table_df = query_metadata_table(feedname)
        print("feedinfo_from_MD_table_df keys --> ",feedinfo_from_MD_table_df.keys())
        print(feedinfo_from_MD_table_df)
        df = pd.merge(files_to_be_processed_df, feedinfo_from_MD_table_df, how="inner",left_on=['FEED_NAME'],right_on=['Dataset_Name'])
        df = df.reset_index(drop=True)
        print("MERGED df --> ",df.keys())
        if df.shape[0] != 0:
            df=df[['FEED_NAME','FILE_NAME','FILE_DATE','EVENT_TIME','LOAD_TYPE','S3_PATH','RAW_BUCKET_NAME','AIRFLOW_FLAG','PROCESS_TRACKER_STATUS','PROCESSED_DTTM']]
            df.dropna(how="all",axis=0,subset=['FILE_NAME'],inplace=True)
            df.sort_values(by=['FILE_DATE','EVENT_TIME'],ascending=False,inplace=True)
            # print("renamed columns of df --> ",df.keys())
            print(df)
            if load_type == 'INCR_LOAD_WITHOUT_PARTITION':
                incr_load_files(env,feedname,df,batchnum,email_alert_dl,task_group_name,cycle_date)
            elif load_type == 'FULL_LOAD':
                full_load_files(env,feedname,df,batchnum,email_alert_dl,task_group_name,cycle_date)
            elif load_type == 'INCR_LOAD_WITH_PARTITION':
                partition_load_files(env,feedname,df,batchnum,email_alert_dl,task_group_name,cycle_date)
            else:
                logger.error("invalid load_type - {} for the feed - {}".format(load_type,feedname))
                raise
        else:
            body = f"There are no entries in AIRFLOW SNS table that needs to be processed for the feed - {feedname}"
            logger.error(f"{body}")
            msg = f"Hi,<br><br>{body} <br><br>Thanks,<br>Airflow"
            subject = (f"ERROR!! FROM {env} DAG - dds_files_copy_by_load_{batchnum}.{task_group_name}: File Processing not initiated for the feed - {feedname}")
            send_email(to=email_alert_dl, subject=subject, html_content=msg)
            raise
    else:
        logger.info("This is a Dummy task created for not breaking the DAG. Please ignore!!!")


def write_out_success(*op_args):
    feedname = op_args[0]
    load_type = op_args[1]
    print("copy of files for feed- {} , load_type- {} completed successfully".format(feedname,load_type))

def write_out_failures(*op_args):
    feedname = op_args[0]
    load_type = op_args[1]
    print("copy of files for feed- {} , load_type- {} failed".format(feedname,load_type))


        
        
              

# if __name__ == '__main__':
#     query_DatasetName = "DAT/EXP-Sali"
#     print(f"Records for {query_DatasetName}")
#     datasets = query_process_tracker(query_DatasetName)
#     for dataset in datasets:
#         print(dataset['DatasetName'], ":", dataset['FileName'])
