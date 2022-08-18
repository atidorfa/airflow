from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks import mysql_hook

default_arg = {'owner': 'airflow', 'start_date': '2020-02-28'}

with DAG(
        '03_mysql_dag',
        default_args=default_arg,
        schedule_interval=None
) as dag:

    mysql_task = MySqlOperator(
        mysql_conn_id='mysql_default',
        task_id='mysql_task',
        sql=r"""INSERT INTO airflow_test (air_celula, air_ano, air_semana, air_venta)
        VALUES (2, 2023, 03, 200);""",
        params={'test_user_id': -99},
        dag=dag)

mysql_task
