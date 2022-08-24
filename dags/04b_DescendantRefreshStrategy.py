import logging
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from airflow.hooks.mysql_hook import MySqlHook

from datetime import datetime


def get_descendant_news(db_session, actual_period):
    sql = text("select sj_celula as clave, clave as child "
               "from clientes "
               "where BAJA = 0 and BRK_SPONSOR <> 0 and periodo = :actual_period")
    return db_session.execute(sql, {'actual_period': actual_period}).all()


def load_new_customer_descendant_data(db_session, clave, child, generation):
    sql = text("insert into app_cli_desc_airflow(acd_clave, acd_child, acd_generation, acd_registro_activo) "
               "values (:clave, :child, :generation, 1) ON DUPLICATE KEY update acd_registro_activo = 1")
    db_session.execute(sql, {'clave': clave, 'child': child, 'generation': generation})
    db_session.commit()


def get_session(conn_id: str) -> Session:
    hook = MySqlHook(mysql_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    return sessionmaker(bind=engine)()

with get_session('airflow_mysql_mexico') as db_session:
    def refresher():
        logging.info("Comenzando incorporate refresh...")
        for new_descendant in get_descendant_news(db_session=db_session, actual_period=CalendarAPI().get_period_data().currentYearPeriod):
            load_new_customer_descendant_data(db_session=db_session,
                                              clave=new_descendant['clave'],
                                              child=new_descendant['child'],
                                              generation=0)
        logging.info("Fin de carga correcto [OK]")


with DAG(
        dag_id='04b_DescendantRefreshStrategy',
        start_date=datetime(2022, 5, 28),
        schedule_interval=None
) as dag:
    start_task = EmptyOperator(
        task_id='start'
    )

    descendant_refresh = PythonOperator(
        task_id='descendant_refresh',
        python_callable=refresher,
        op_kwargs={"x": "Apache Airflow"},
        dag=dag,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> descendant_refresh >> end_task