import logging
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from airflow.hooks.mysql_hook import MySqlHook

from datetime import datetime


def delete_inactive_registers(db_session):
    sql = text("delete from app_cli_familia_airflow where acf_registro_activo > 1")
    db_session.execute(sql)
    db_session.commit()


def load_family_tree_data(db_session):
    sql = text("insert into app_cli_familia_airflow(acf_clave, acf_child, acf_registro_activo) "
               "select c_cur.clave as clave, c_ret.clave as child, 0 as registro_activo "
               "from app_performance c_ret force index(idx_app_performance_clave), app_performance c_cur force index(idx_app_performance_clave) "
               "where c_ret.registro_activo = 1 "
               "and c_cur.registro_activo = 1 "
               "and c_ret.armadonum between c_cur.armadoini  and c_cur.armadofin "
               "and c_ret.clave != c_cur.clave "
               "and c_ret.clave not in ("
               "    select distinct c2.clave as clave "
               "    from app_performance c1 force index(idx_app_performance_clave), app_performance c2 force index(idx_app_performance_clave) "
               "    where c2.armadonum between c1.armadoini and c1.armadofin and c1.registro_activo = 1 "
               "    and c2.registro_activo = 1 "
               "    and c1.clave in ("
               "        select c3.clave from app_performance c3, app_performance c4 "
               "        where c3.armadonum between c4.armadoini and c4.armadofin "
               "        and c4.registro_activo = 1 "
               "        and	c3.registro_activo = 1 "
               "        and c4.clave = c_cur.clave "
               "        and c3.nivel_ant >= c_cur.nivel_ant "
               "        and c3.clave != c4.clave"
               "    )"
               ")")
    db_session.execute(sql)
    db_session.commit()


def update_register_status(db_session):
    sql = text(
        "UPDATE app_cli_familia_airflow SET acf_registro_activo = IF(acf_registro_activo >= 1, acf_registro_activo + 1, 1) "
        "order by acf_registro_activo desc, acf_clave asc, acf_child asc")
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
        load_family_tree_data(db_session=db_session)
        update_register_status(db_session=db_session)
        logging.info("Fin de carga correcto [OK]")


with DAG(
        dag_id='01_FamilyMainStrategy',
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

    family_loader = PythonOperator(
        task_id='comision_loader',
        python_callable=loader,
        op_kwargs={"x": "Apache Airflow"},
        dag=dag,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> depurate_table >> family_loader >> end_task