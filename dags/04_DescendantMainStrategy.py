import logging
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from airflow.hooks.mysql_hook import MySqlHook

from datetime import datetime

def delete_inactive_registers(db_session):
    sql = text("delete from app_cli_desc_airflow where acd_registro_activo > 1")
    db_session.execute(sql)
    db_session.commit()


def load_descendant_tree_data(db_session):
    sql = text("insert into app_cli_desc_airflow(acd_clave, acd_child, acd_generation, acd_registro_activo) "
               "select c_cur.clave as clave, c_ret.clave as child, 0 as gen, 0 as registro_activo "
               "from app_performance c_ret force index(idx_app_performance_clave), "
               "app_performance c_cur force index(idx_app_performance_clave) "
               "where c_ret.registro_activo = 1 "
               "and c_cur.registro_activo = 1 "
               "and c_ret.armadonum between c_cur.armadoini and c_cur.armadofin "
               "and c_cur.clave in (select t.clave from app_performance t where t.nivel_ant > 1) "
               "and c_ret.clave != c_cur.clave")
    db_session.execute(sql)
    db_session.commit()


def get_generations_tree(db_session, generation: int, period):
    generation_dict = {1: 'POUT1',
                       2: 'POUT2',
                       3: 'POUT3'}
    sql = text("select destino as clave, fuente as child from comisiondetallenuevo c "
               "where PERIODO = :period and CODIGO = :code")
    return db_session.execute(sql, {'period': period, 'code': generation_dict[generation]})


def update_child_generation(db_session, clave, child, generation):
    sql = text("update app_cli_desc_airflow set acd_generation = :generation "
               "where acd_clave = :clave "
               "and acd_child = :child "
               "and acd_registro_activo = 1")
    db_session.execute(sql, {'clave': clave, 'child': child, 'generation': generation})


def update_register_status(db_session):
    sql = text("UPDATE app_cli_desc_airflow SET acd_registro_activo = IF(acd_registro_activo >= 1, acd_registro_activo + 1, 1) "
               "order by acd_registro_activo desc, acd_clave asc, acd_child asc")
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
        load_descendant_tree_data(db_session=db_session)
        update_register_status(db_session=db_session)

        # 3. Cargar gens
        for gen in [1, 2, 3]:
            for reg in get_generations_tree(db_session=db_session, period=last_period, generation=gen):
                update_child_generation(db_session=db_session,
                                        clave=reg['clave'],
                                        child=reg['child'],
                                        generation=gen)
            db_session.commit()
        logging.info("Fin de carga correcto [OK]")



with DAG(
        dag_id='04_DescendantMainStrategy',
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

    descendant_loader = PythonOperator(
        task_id='descendant_loader',
        python_callable=loader,
        op_kwargs={"x": "Apache Airflow"},
        dag=dag,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> depurate_table >> descendant_loader >> end_task