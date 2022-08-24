import logging
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from airflow.hooks.mysql_hook import MySqlHook

from datetime import datetime


def delete_inactive_registers(db_session):
    sql = text("delete from app_cli_grupo_airflow where acg_registro_activo > 1")
    db_session.execute(sql)
    db_session.commit()


def load_groups_tree_data(db_session):
    sql = text("insert into app_cli_grupo_airflow (acg_clave, acg_child, acg_registro_activo) "
               "select grupo as clave, clave as child, 0 "
               "from app_ventahistorica av "
               "where PERIODO = (select max(periodo) from app_ventahistorica) ")
    db_session.execute(sql)
    db_session.commit()


def update_register_status(db_session):
    sql = text(
        "UPDATE app_cli_grupo_airflow SET acg_registro_activo = IF(acg_registro_activo >= 1, acg_registro_activo + 1, 1) "
        "order by acg_registro_activo desc, acg_clave asc, acg_child asc")
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
        load_groups_tree_data(db_session=db_session)
        update_register_status(db_session=db_session)
        logging.info("Fin de carga correcto [OK]")


with DAG(
        dag_id='02_GroupsMainStrategy',
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

    group_loader = PythonOperator(
        task_id='group_loader',
        python_callable=loader,
        op_kwargs={"x": "Apache Airflow"},
        dag=dag,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> depurate_table >> group_loader >> end_task