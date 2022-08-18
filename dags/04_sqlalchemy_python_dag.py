from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from airflow.hooks.mysql_hook import MySqlHook

from datetime import datetime
import json


def get_session(conn_id: str) -> Session:
    hook = MySqlHook(mysql_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    return sessionmaker(bind=engine)()


def get_data_from_mysql():
    sql = text("SELECT sj_celula, año, semana, venta  \
                FROM ventapesos;")

    with get_session('airflow_mysql_mexico') as db:
        result = db.execute(sql).fetchall()
        if result is None:
            return []
        # res = json.dumps(dict(result))
    print(result)


with DAG(
        dag_id='04_sqla_python_dag',
        start_date=datetime(2022, 5, 28),
        schedule_interval=None
) as dag:
    start_task = EmptyOperator(
        task_id='start'
    )

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data_from_mysql,
        op_kwargs={"x": "Apache Airflow"},
        dag=dag,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> get_data >> end_task
