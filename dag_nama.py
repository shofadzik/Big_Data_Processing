from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

dag = DAG('shofa', description='Hello World DAG',schedule_interval='0 12 * * *',start_date=datetime(2022, 7, 16), catchup=False)

orders = BashOperator(
    task_id='orders',
    bash_command="curl host.docker.internal:5000/orders/shofa ", 
    dag=dag
)

order_details = BashOperator(
    task_id='order_details',
    bash_command="curl host.docker.internal:5000/order_details/shofa ",
    dag=dag
)

categories = BashOperator(
    task_id='categories',
    bash_command="curl host.docker.internal:5000/categories/shofa",
    dag=dag
)

orders_joined = BashOperator(
    task_id='orders_joined',
    bash_command="curl host.docker.internal:5000/orders_joined/shofa",
    dag=dag
)

orders >> order_details >> categories
order_details >> orders_joined

#sebelum panah namanya dagsessor, 
#setelah panah suksessor
#order detail nge-emit 2 yaitu categories dan orders join