import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


sys.path.append(
    '/Users/KLoGic/Public/Developer/playground/3rd-project/airflow')

from sales_order_init import insertRandomSalesOrder


default_args = {
    'owner': 'klogic',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 1),
    'email': ['klogic@hotmail.co.th'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_insert_sales_order', default_args=default_args, schedule_interval="*/1 * * * *")

process_dag = PythonOperator(
    task_id='dag_insert_sales_order',
    python_callable=insertRandomSalesOrder,
    dag=dag)
