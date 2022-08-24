import logging
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from airflow.hooks.mysql_hook import MySqlHook

from datetime import datetime


def delete_inactive_registers(db_session):
    sql = text("delete from app_cli_incorporadas_airflow where aci_registro_activo > 1")
    db_session.execute(sql)
    db_session.commit()


def load_incorporated_tree_data(db_session):
    sql = text("insert into app_cli_incorporadas_airflow(aci_clave, aci_child, aci_nivel_ant, aci_upline, aci_registro_activo) "
               "select BRK_SPONSOR as clave, clave as child, nivel_ant, upline, 0 "
               "from app_performance "
               "where BRK_SPONSOR != 0 and registro_activo = 1;")
    db_session.execute(sql)
    db_session.commit()


def update_register_status(db_session):
    sql = text(
        "UPDATE app_cli_incorporadas_airflow SET aci_registro_activo = IF(aci_registro_activo >= 1, aci_registro_activo + 1, 1) "
        "order by aci_registro_activo desc, aci_clave asc, aci_child asc")
    db_session.execute(sql)
    db_session.commit()


def get_session(conn_id: str) -> Session:
    hook = MySqlHook(mysql_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    return sessionmaker(bind=engine)()


with get_session('airflow_mysql_mexico') as db_session:
    def depurate():
        logging.info("Comenzando borrado de datos actuales...")
        delete_inactive_registers(db_session=db_session)
        logging.info("Borrado exitoso [OK]")

    def loader():
        logging.info("Comenzando carga de nuevos datos...")
        load_incorporated_tree_data(db_session=db_session)
        update_register_status(db_session=db_session)
        logging.info("Fin de carga correcto [OK]")


with DAG(
        dag_id='03_IncorporatedMainStrategy',
        start_date=datetime(2022, 5, 28),
        schedule_interval=None
) as dag:
    start_task = EmptyOperator(
        task_id='start'
    )

    depurate_table = PythonOperator(
        task_id='depurate_table',
        python_callable=depurate,
        op_kwargs={"x": "Apache Airflow"},
        dag=dag,
    )

    incorporate_loader = PythonOperator(
        task_id='incorporate_loader',
        python_callable=loader,
        op_kwargs={"x": "Apache Airflow"},
        dag=dag,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> depurate_table >> incorporate_loader >> end_task
