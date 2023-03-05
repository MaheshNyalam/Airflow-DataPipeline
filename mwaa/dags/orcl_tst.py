from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.utils.dates import days_ago
import os
import cx_Oracle
import pendulum

LIB_DIR = '/usr/local/airflow/plugins/instantclient_18_5'
DAG_ID = os.path.basename(__file__).replace(".py", "")

def testHook(**kwargs):
    cx_Oracle.init_oracle_client(lib_dir=LIB_DIR)
    version = cx_Oracle.clientversion()
    print("cx_Oracle.clientversion",version)
    return version

with DAG(dag_id=DAG_ID, schedule_interval=None, catchup=False, start_date=pendulum.today('UTC').add(days=-1)) as dag:
    hook_test = PythonOperator(
        task_id="hook_test",
        python_callable=testHook,
    )

    opr_sql = OracleOperator(
    task_id='task_sql',
    oracle_conn_id='orc_conn',
    sql= 'SELECT CURRENT_TIMESTAMP FROM dual',
    autocommit =True)

    pluginlist = BashOperator(
        task_id="lib_list",
        bash_command="ls /usr/local/airflow/plugins/instantclient_18_5/",
    )

    folderlist = BashOperator(
        task_id="folder_list",
        bash_command="ls -ltr /usr/local/airflow/"
    )

    airflowconfiglist = BashOperator(
        task_id="airflow_config_list",
        bash_command="cat /usr/local/airflow/airflow.cfg"
    )

    pluginslist = BashOperator(
        task_id="plugins_list",
        bash_command="ls -ltr /usr/local/airflow/plugins/aws_mwaa/templates"
    )

    reqlist = BashOperator(
        task_id="req_list",
        bash_command="cat /usr/local/airflow/requirements/requirements.txt"
    )

    config_folder_list = BashOperator(
        task_id="config_folder_list",
        bash_command="ls -ltr /usr/local/airflow/config"
    )

    all_folder_list = BashOperator(
        task_id="all_folder_list",
        bash_command="ls -l -R /usr"
    )

    python_pip_test = BashOperator(
        task_id="python_pip_test",
        bash_command="pip list",
    )



'''
    hook_test = BashOperator(
        task_id="hook_test",
        bash_command="pip list",
    )
'''    

'''
    hook_test = PythonOperator(
        task_id="hook_test",
        python_callable=testHook,
    )
'''

