import os
import sys
import yaml
import pendulum
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
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.email import send_email
# from airflow.decorators import dag, task
import logging
sys.path.append(os.path.join(os.path.dirname(__file__),"scripts"))
from dag_helpers import *

logger = logging.getLogger(__name__)

PARENT_DAG_NAME = os.path.basename(__file__).replace(".py", "")
# batchnum = PARENT_DAG_NAME.split("_")[-1]
# batchname = PARENT_DAG_NAME.split("_")[-2].upper() + batchnum
# batchname = 'BATCH1'
# batchnum = 1
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
# summary_email_dl = "mahesh.nyalam@advance-auto.com"

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
          schedule_interval = None,
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


ts3 = PythonOperator(
        task_id='send_summary_report',
        python_callable = send_summary_report,
        op_args=[env,PARENT_DAG_NAME,summary_email_dl,"{{ task_instance.xcom_pull(task_ids='get_cycle_load_date') }}","summary"],
        on_failure_callback=failure_function_wrapper('send_summary_report'),
        dag=dag,
        trigger_rule="all_done")

# ts4 = ShortCircuitOperator(
#         task_id='insert_next_cycle_date',
#         python_callable = insert_next_cycle_date,
#         op_args=[env,"{{ task_instance.xcom_pull(task_ids='get_cycle_load_date') }}"],
#         on_failure_callback=failure_function_wrapper('insert_next_cycle_date'),
#         dag=dag)

ts5 = DummyOperator( # dummy operator for ending
    task_id='end',
    dag=dag)

ts1 >> ts11 >> ts3 >> ts5