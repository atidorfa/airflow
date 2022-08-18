from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

from datetime import datetime

with DAG(
        dag_id='02_mysql_dag',
        start_date=datetime(2022, 5, 28),
        schedule_interval=None
) as dag:
    start_task = EmptyOperator(
        task_id='00_start'
    )

    get_data_mysql_task = MySqlOperator(
        mysql_conn_id='mysql_default',
        task_id='01_get_data_mysql',
        sql=r"""SELECT sj_celula, año, semana, venta
                FROM ventapesos
                group BY sj_celula = 255;""",
        dag=dag
    )

    insert_table_mysql_task = MySqlOperator(
        mysql_conn_id='mysql_default',
        task_id='02_insert_in_table_mysql',
        sql=r"""INSERT INTO airflow_test (air_celula, air_ano, air_semana, air_venta)
                VALUES (2, 2022, 03, 1);""",
        dag=dag
    )

    end_task = EmptyOperator(
        task_id='XX_end'
    )

start_task >> get_data_mysql_task
get_data_mysql_task >> insert_table_mysql_task
insert_table_mysql_task >> end_task
