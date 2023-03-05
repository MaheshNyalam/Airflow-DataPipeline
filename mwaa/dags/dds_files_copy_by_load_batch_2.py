import os
import sys
import yaml
import pendulum
import json
# import datetime
# from pytz import timezone
from itertools import zip_longest
from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator,ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
# from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.email import send_email
# from airflow.decorators import dag, task
import logging
sys.path.append(os.path.join(os.path.dirname(__file__),"scripts"))
from main_driver_code import *
from dag_helpers import *

logger = logging.getLogger(__name__)

PARENT_DAG_NAME = os.path.basename(__file__).replace(".py", "")
batchnum = PARENT_DAG_NAME.split("_")[-1]
batchname = PARENT_DAG_NAME.split("_")[-2].upper() + batchnum

env = Variable.get("env_lev")
# todays_date = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD')
# FEED_CONFIG_HOME = "/usr/local/airflow/dags/dds_feed_configs/"
FEED_CONFIG_HOME = Variable.get("FEED_CONFIG_HOME")
# RUN_AS_USER='airflow'
RUN_AS_USER =  Variable.get("RUN_AS_USER")
# dwh_hook = SnowflakeHook(snowflake_conn_id="dat_sf_con_nonprod")
# SUCCESS_EMAIL = ["mahesh.nyalam@advance-auto.com","sadhu.venkateswarlu@advance-auto.com","vidyakrishna.mudumba@advance-auto.com","ramana.gunti@advance-auto.com"]
# FAILURE_EMAIL = ["mahesh.nyalam@advance-auto.com","sadhu.venkateswarlu@advance-auto.com","vidyakrishna.mudumba@advance-auto.com","ramana.gunti@advance-auto.com"]
# summary_email_dl = "mahesh.nyalam@advance-auto.com,sadhu.venkateswarlu@advance-auto.com,vidyakrishna.mudumba@advance-auto.com,ramana.gunti@advance-auto.com"
summary_email_dl = Variable.get("summary_email_dl")
ADHOC_DDS_LIST = Variable.get('ADHOC_DDS_DICT',deserialize_json=True)["FEEDNAMES"]

def failure_function_wrapper(task_reference):
    def failure_function(context):
        # task_refer = "{task_reference}".format(task_reference=task_reference)
        environment = env.upper()
        task_name = context["task"].task_id
        end_datetime = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD HH:mm:ss')
        # run_dt = context["ts"]
        print("Task Failed timestamp is {}".format(end_datetime))
        exception = context.get("exception")
        msg = "Hi,<br><br>Failure in the task {}. <br><br>error : {} <br><br>Thanks".format(task_name, exception)
        # subject = ("ERROR!! {} - DAT Fusion - PI - WeeklyCQBasetable DAG on {} EST".format(env, end_date))
        subject = (f"ERROR!! FROM {environment} DAG - {PARENT_DAG_NAME}.{task_name} at {end_datetime}")
        send_email(to=summary_email_dl, subject=subject, html_content=msg)
    return failure_function

# def get_cycle_load_date():
#     cycle_date = pendulum.now(tz="US/Eastern").format('YYYY-MM-DD')
#     return cycle_date

dag = DAG(dag_id = PARENT_DAG_NAME,
          schedule_interval = '00 22 * * *',
          catchup=False,
          start_date= pendulum.today('EST').add(days=-1))

ts1 = DummyOperator( # dummy operator for starting dag
    task_id='start',
    dag=dag)

ts11 = PythonOperator(
        task_id='get_cycle_load_date',
        python_callable = get_cycle_load_date,
        op_args=[env],
        do_xcom_push=True,
        on_failure_callback=failure_function_wrapper('get_cycle_load_date'),
        dag=dag)

ts2 = PythonOperator(
        task_id='feed_list_needed_for_processing',
        python_callable = get_feed_list_needed_for_processing,
        op_kwargs={'env': env,'batchnum':batchnum},
        execution_timeout=pendulum.duration(minutes=10),
        on_failure_callback=failure_function_wrapper('feed_list_needed_for_processing'),
        do_xcom_push=True,
        dag=dag)

ts3 = PythonOperator(
        task_id='send_summary_report',
        python_callable = send_summary_report,
        op_args=[env,PARENT_DAG_NAME,summary_email_dl,"{{ task_instance.xcom_pull(task_ids='get_cycle_load_date') }}",batchnum],
        execution_timeout=pendulum.duration(minutes=10),
        on_failure_callback=failure_function_wrapper('send_summary_report'),
        dag=dag,
        trigger_rule="all_done")

# ts4 = ShortCircuitOperator(
#         task_id='truncate_summary_table',
#         python_callable = truncate_summary_table,
#         op_args=[env,batchnum],
#         on_failure_callback=failure_function_wrapper('truncate_summary_table'),
#         dag=dag)

ts5 = DummyOperator( # dummy operator for ending
    task_id='end',
    dag=dag)

ts1 >> ts11 >> ts2

# Pull feed_list from feed_list_for_processing task.
# pulled_value = "'{{ task_instance.xcom_pull(task_ids='feed_list_for_processing',key='return_value') }}'"

# feed_list = []
# for i in pulled_value[2:-2].split(", "):
#     j = i[1:-1]
#     feed_list.append(j)

# Read in the feed list file which was dynamically created in the feed_list_needed_for_processing task
if os.path.exists('{}feed_list_{}.txt'.format(FEED_CONFIG_HOME,batchnum)):
    with open('{}feed_list_{}.txt'.format(FEED_CONFIG_HOME,batchnum)) as f:
        feed_list = f.read().splitlines()
    
# Read in the feed load_types list which was dynamically created in the feed_list_needed_for_processing task
with open('{}feed_with_load_types.yaml'.format(FEED_CONFIG_HOME)) as fp:
    feed_load_types_list = yaml.safe_load(fp)

with open('{}feed_dependencies.yaml'.format(FEED_CONFIG_HOME)) as fd:
    feed_dependencies = yaml.safe_load(fd)

# create batches for feeds based on their loadtype
HEAVY_LOAD_FEEDS = feed_dependencies['HEAVY_LOAD_FEEDS']
FULL_LOAD_IND_LIST = [x for x in feed_list if x in feed_load_types_list.keys() and feed_load_types_list[x] == ['FULL_LOAD'] and x in feed_dependencies['INDEPENDENT'] and x in feed_dependencies[batchname]] # INDEPENDENT FULL LOAD list
INCR_LOAD_IND_LIST = [x for x in feed_list if x in feed_load_types_list.keys() and feed_load_types_list[x] == ['INCR_LOAD_WITHOUT_PARTITION'] and x in feed_dependencies['INDEPENDENT'] and x in feed_dependencies[batchname]] # INDEPENDENT INCR_LOAD_WITHOUT_PARTITION list
PARTITION_LOAD_IND_LIST = [x for x in feed_list if x in feed_load_types_list.keys() and feed_load_types_list[x] == ['INCR_LOAD_WITH_PARTITION'] and x in feed_dependencies['INDEPENDENT'] and x in feed_dependencies[batchname]] # INDEPENDENT INCR_LOAD_WITH_PARTITION list
# MASTER_LIST = [x for x in feed_dependencies['DEPENDENT']['SUB_LIST_LEVEL1'].keys()] # DEPENDENT list
# DEPENDENT_LIST = []
# for i in feed_dependencies['DEPENDENT']['SUB_LIST_LEVEL1'].values():
#     DEPENDENT_LIST = DEPENDENT_LIST + i
#     DEPENDENT_LIST_FINAL1 = list(set(DEPENDENT_LIST))

# DEPENDENT_LIST_FINAL = list(set(feed_list) & set(DEPENDENT_LIST_FINAL1))

COMPLETE_LIST_TMP = FULL_LOAD_IND_LIST + INCR_LOAD_IND_LIST + PARTITION_LOAD_IND_LIST
COMPLETE_LIST = [item for item in COMPLETE_LIST_TMP if item not in ADHOC_DDS_LIST]
INDEPENDENT_LIST_SMALL = [item for item in COMPLETE_LIST if item not in HEAVY_LOAD_FEEDS]
INDEPENDENT_LIST_LARGE = [item for item in COMPLETE_LIST if item in HEAVY_LOAD_FEEDS]
# COMPLETE_LIST = [item for item in COMPLETE_LIST_TMP if item not in ADHOC_DDS_LIST]
if len(INDEPENDENT_LIST_SMALL) == 0:
    INDEPENDENT_LIST_SMALL.append('DUMMY_FEED')
# ind_tasks_dict = dict()
# master_tasks_dict = dict()
# ind_task_feed_list = []
# ind_tasks = []
# master_task_feed_list = []
# master_tasks = []
# downstream_task_feed_list = []
# downstream_tasks = []
TaskGroupsListSmall =[]
TaskGroupsListLarge=[]
ind_lt_small = [list(item) for item in list(zip_longest(*[iter(INDEPENDENT_LIST_SMALL)]*7, fillvalue=None))]
ind_lt_small = [ list(filter(None, _)) for _ in ind_lt_small ]

for g_id in enumerate(ind_lt_small, 1):
    tg_id = 'INDEPENDENT_SMALL_DDS_GROUP_{}'.format(g_id[0])
    if g_id[0]==1:
        trigger_condition="all_success"
    else:
        trigger_condition="all_done"
    with TaskGroup(group_id=tg_id,dag=dag) as taskgroup1:
        task_group_name = str(tg_id)
        for feedname in g_id[1]:
            if feedname in FULL_LOAD_IND_LIST:
                load_type = 'FULL_LOAD'
            elif feedname in INCR_LOAD_IND_LIST:
                load_type = 'INCR_LOAD_WITHOUT_PARTITION'
            elif feedname in PARTITION_LOAD_IND_LIST:
                load_type = 'INCR_LOAD_WITH_PARTITION'
            elif feedname == 'DUMMY_FEED':
                load_type = 'DUMMY'
            else:
                logger.error('Invalid load type has been parsed')
            # fld= str(_).upper()
            # dt="{{ds}}") 
            si1 = PythonOperator(
                    task_id='s3copy_job_for_{}'.format(feedname),
                    python_callable = main,
                    op_args=[env, feedname, load_type, batchnum, summary_email_dl, task_group_name, "{{ task_instance.xcom_pull(task_ids='get_cycle_load_date') }}"],
                    execution_timeout=pendulum.duration(minutes=90),
                    on_failure_callback=failure_function_wrapper('s3copy_job_for_{}'.format(feedname)),
                    trigger_rule=trigger_condition,
                    dag=dag)
        # ts2 >> taskgroups1 >> ts3 >> ts5                         
        TaskGroupsListSmall.append(taskgroup1)
ts2 >> TaskGroupsListSmall[0]

if len(TaskGroupsListSmall) > 1:
    for i in range(0,len(TaskGroupsListSmall)):
        if len(TaskGroupsListSmall)-1 != i:
            TaskGroupsListSmall[i] >> TaskGroupsListSmall[i+1]
        else:
            continue      

TaskGroupsListSmall[-1] >> ts3

if len(INDEPENDENT_LIST_LARGE)!=0:
    ind_lt_large = [list(item) for item in list(zip_longest(*[iter(INDEPENDENT_LIST_LARGE)]*2, fillvalue=None))]
    ind_lt_large = [ list(filter(None, _)) for _ in ind_lt_large ]
    for g_id in enumerate(ind_lt_large, 1):
        tg_id = 'INDEPENDENT_LARGE_DDS_GROUP_{}'.format(g_id[0])
        if g_id[0]==1:
            trigger_condition="all_success"
        else:
            trigger_condition="all_done"
        with TaskGroup(group_id=tg_id,dag=dag) as taskgroup2:
            task_group_name = str(tg_id)
            for feedname in g_id[1]:
                if feedname in FULL_LOAD_IND_LIST:
                    load_type = 'FULL_LOAD'
                elif feedname in INCR_LOAD_IND_LIST:
                    load_type = 'INCR_LOAD_WITHOUT_PARTITION'
                elif feedname in PARTITION_LOAD_IND_LIST:
                    load_type = 'INCR_LOAD_WITH_PARTITION'
                elif feedname == 'DUMMY_FEED':
                    load_type = 'DUMMY'
                else:
                    logger.error('Invalid load type has been parsed')
                # fld= str(_).upper()
                # dt="{{ds}}") 
                si1 = PythonOperator(
                        task_id='s3copy_job_for_{}'.format(feedname),
                        python_callable = main,
                        op_args=[env, feedname, load_type, batchnum, summary_email_dl,task_group_name,"{{ task_instance.xcom_pull(task_ids='get_cycle_load_date') }}"],
                        execution_timeout=pendulum.duration(minutes=120),
                        on_failure_callback=failure_function_wrapper('s3copy_job_for_{}'.format(feedname)),
                        trigger_rule=trigger_condition,
                        dag=dag)
            TaskGroupsListLarge.append(taskgroup2)
    ts2 >> TaskGroupsListLarge[0]
        
    if len(TaskGroupsListLarge) > 1:
        for i in range(0,len(TaskGroupsListLarge)):
            if len(TaskGroupsListLarge)-1 != i:
                TaskGroupsListLarge[i] >> TaskGroupsListLarge[i+1]
            else:
                continue
    TaskGroupsListLarge[-1] >> ts3
ts3 >> ts5