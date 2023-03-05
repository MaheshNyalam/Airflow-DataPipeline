from airflow import DAG
from airflow.operators.bash import BashOperator
import os 
import pendulum

DAG_ID = os.path.basename(__file__).replace(".py", "")

common_var_path = "/usr/local/airflow/dags/config/requirements_dl.txt"

S3_BUCKET = '************'
S3_KEY = 'mwaa/bkup_whl/dl_plugins_whl_22062022.zip' 

with DAG(dag_id=DAG_ID, schedule_interval=None, catchup=False, start_date=pendulum.today('UTC').add(days=-1)) as dag:
    ls_command = BashOperator(
        task_id="ls_command",
        bash_command=f"ls -ltr /usr/local/airflow/;ls -ltr /usr/local/airflow/requirements/;cat /usr/local/airflow/requirements/requirements.txt"
    )

    pip_list = BashOperator(
        task_id="pip_list_command",
        bash_command=f"pip list"
    )

    cat_command = BashOperator(
        task_id="cat_command",
        bash_command=f"cat /usr/local/airflow/dags/config/requirements_dl.txt"
    )

    download_command = BashOperator(
        task_id="download_command",
        bash_command=f"mkdir /tmp/req_whls;pip3 download -r /usr/local/airflow/dags/config/requirements_dl.txt -d /tmp/req_whls;zip -j /tmp/plugins.zip /tmp/req_whls/*;aws s3 cp /tmp/plugins.zip s3://{S3_BUCKET}/{S3_KEY}"
    )

