from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

import datetime as dt
from datetime import datetime


def print_query(**kwargs):
    query2 = kwargs['ti'].xcom_pull(task_ids='mysql_query')
    print(query2)


# default_args = {
#     'owner': 'me',
#     'start_date': dt.datetime(2019, 8, 15),
#     'retries': 1,
#     'retry_delay': dt.timedelta(minutes=5),
# }

with DAG(
    dag_id='01_dag_test',
    start_date=datetime(2022, 5, 28),
    schedule_interval=None
)as dag:

    python = PythonOperator(
        task_id='print',
        provide_context=True,
        python_callable=print_query,
        dag=dag)

    query = MySqlOperator(
        task_id='mysql_query',
        sql=r"""SELECT sj_celula, año, semana, venta
                        FROM ventapesos
                        group BY sj_celula = 255;""",
        mysql_conn_id='mysql_default',
        dag=dag)

query >> python
