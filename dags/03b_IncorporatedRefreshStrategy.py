import logging
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from airflow.hooks.mysql_hook import MySqlHook

from datetime import datetime



def get_incorporated_news(db_session):
    sql = text("select brk_sponsor as clave, clave as child, upline "
               "from clientes "
               "where ARMADOINI = 0 and ARMADOFIN = 0 and ARMADONUM = 0 and BAJA = 0 and INGRESO is not null and BRK_SPONSOR <> 0")
    return db_session.execute(sql).all()


def load_new_customer_incorporated_data(db_session, clave, child, upline):
    sql = text("insert into app_cli_incorporadas_airflow(aci_clave, aci_child, aci_nivel_ant, aci_upline, aci_registro_activo) "
               "values (:clave, :child, 0, :upline, 1) ON DUPLICATE KEY update aci_registro_activo = 1")
    db_session.execute(sql, {'clave': clave, 'child': child, 'upline': upline})
    db_session.commit()



def get_session(conn_id: str) -> Session:
    hook = MySqlHook(mysql_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    return sessionmaker(bind=engine)()

with get_session('airflow_mysql_mexico') as db_session:
    def refresher():
        logging.info("Comenzando incorporate refresh...")
        for new_customer in get_incorporated_news(db_session=db_session):
            load_new_customer_incorporated_data(db_session=db_session,
                                                clave=new_customer['clave'],
                                                child=new_customer['child'],
                                                upline=new_customer['upline'])
        logging.info("Fin de carga correcto [OK]")


with DAG(
        dag_id='03b_IncorporatedRefreshStrategy',
        start_date=datetime(2022, 5, 28),
        schedule_interval=None
) as dag:
    start_task = EmptyOperator(
        task_id='start'
    )

    incorporate_refresh = PythonOperator(
        task_id='incorporate_refresh',
        python_callable=refresher,
        op_kwargs={"x": "Apache Airflow"},
        dag=dag,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> incorporate_refresh >> end_task