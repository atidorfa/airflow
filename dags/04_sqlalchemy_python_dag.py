from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from airflow.hooks.mysql_hook import MySqlHook

from datetime import datetime


def get_session(conn_id: str) -> Session:
    hook = MySqlHook(mysql_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    return sessionmaker(bind=engine)()


def get_data_from_mysql():
    print('Getting data from: airflow_mysql_mexico.ventapesos: ')
    sql = text("SELECT sj_celula, año, semana, venta \
                FROM ventapesos;")

    with get_session('airflow_mysql_mexico') as db:
        result = db.execute(sql).fetchall()
        if result is None:
            return []
    return result

def insert_data_to_mysql(mysql_data):
    print('Inserting data to: airflow_mysql_mexico.airflow_test')
    sql = text("INSERT INTO airflow_test (air_celula, air_ano, air_semana, air_venta) \
                VALUES (:air_celula, :air_ano, :air_semana, :air_venta);")

    with get_session('airflow_mysql_mexico') as db:
        for data in mysql_data:
            print(f"Inserting {data[0]}, {data[1]}, {data[2]}, {data[3]} ")
            db.execute(sql, {'air_celula': data[0], 'air_ano': data[1], 'air_semana': data[2], 'air_venta': data[3]})
            db.commit()

def copy_table():
    data = get_data_from_mysql()
    insert_data_to_mysql(data)



with DAG(
        dag_id='04_sqla_python_dag',
        start_date=datetime(2022, 5, 28),
        schedule_interval=None
) as dag:
    start_task = EmptyOperator(
        task_id='start'
    )

    copy_data = PythonOperator(
        task_id='get_data',
        python_callable=copy_table,
        op_kwargs={"x": "Apache Airflow"},
        dag=dag,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> copy_data >> end_task
