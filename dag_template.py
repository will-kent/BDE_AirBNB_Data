import os
from datetime import datetime, timedelta

#########################################################
#
#   Load Environment Variables
#
#########################################################

# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


########################################################
#
#   DAG Settings
#
#########################################################

from airflow import DAG

dag_default_args = {
    'owner': 'your name',
    'start_date': datetime(2021, 4, 25, hour=10, minute=0),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='dag_name',
    default_args=dag_default_args,
    schedule_interval=timedelta(hours=24),
    catchup=False,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################


def do_something_1_func(**kwargs):
    pass


def do_something_2_func(**kwargs):
    pass


#########################################################
#
#   DAG Operator Setup
#
#########################################################

from airflow.operators.python_operator import PythonOperator

do_something_1_task = PythonOperator(
    task_id='do_something_1_taskid',
    python_callable=do_something_1_func,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=dag
)

do_something_2_task = PythonOperator(
    task_id='do_something_2_taskid',
    python_callable=do_something_2_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

do_something_1_task >> do_something_2_task