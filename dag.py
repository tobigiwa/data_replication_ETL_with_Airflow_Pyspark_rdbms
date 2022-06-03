import logging
from pathlib import Path

path = Path.cwd()

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'oluwatobitobias',
    'description': 'a workflow of our ELT process',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
    }

def logger():
    logger =  logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger_handler = logging.FileHandler(f'./logs/dag.log')
    logger_formatter = logging.Formatter('%(asctime)s -%(name)s %(levelname)s - %(message)s \n')
    logger_handler.setFormatter(logger_formatter)
    logger.addHandler(logger_handler)
    logger.info('Logs is instatiated')


with DAG('Data_Replication_Workflow',
        default_args=default_args,
        catchup=False,
        schedule_interval=None
        ) as dag:

        log = PythonOperator(task_id='dag_log',
                            python_callable=logger)

        sourcing_and_setting_up_db =  BashOperator(task_id='setting_up_db.py',
                                        bash_command=f'python {path}/sourcing_and_setting_up_db.py')

        ETL_EtLT =  BashOperator(task_id='ETL_EtLT.py',
                             bash_command=f'python {path}/ETL_EtLT.py')

        log >> sourcing_and_setting_up_db >> ETL_EtLT

        
