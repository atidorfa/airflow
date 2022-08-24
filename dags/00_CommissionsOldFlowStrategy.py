import logging
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from airflow.hooks.mysql_hook import MySqlHook
# from crud.App_performance_CRUD import delete_inactives, copy_base_info_from_clients, \
#     get_qualified_leaders_count, update_qualified_leaders, get_active_customers, update_active_customers, update_status

from datetime import datetime


def delete_inactives(db_session):
    sql = text("delete from app_performance_airflow where registro_activo = 2")
    db_session.execute(sql)
    db_session.commit()


def copy_base_info_from_clients(db_session):
    sql = text("INSERT INTO app_performance_airflow (clave, upline, brk_sponsor, armadoini, armadofin, armadonum, armadodeep, "
               "nivel, nivel_ant, nivel_ant_2, vta_personal, vta_grupal, vta_descendencia, saldo, grupo, region, "
               "justcoins, registro_activo) "
               "SELECT clave, upline, brk_sponsor, armadoini, armadofin, armadonum, armadodeep, nivelred, "
               "plan_nivpag1, plan_nivpag2, p_puntos, g_puntos, plan_pghasta7, saldo, sj_celula, sj_region, "
               "saldo_puntos, 0 "
               "FROM clientes "
               "WHERE baja = 0 and clave <> 1")
    db_session.execute(sql)
    db_session.commit()


def get_qualified_leaders_count(db_session):
    sql = text("select ap1.CLAVE as clave, count(ap2.clave) as cant_lideres_calif "
               "from app_performance_airflow ap1, app_performance_airflow ap2 "
               "where ap2.ARMADONUM between (ap1.ARMADOINI + 1) and ap1.ARMADOFIN and ap1.registro_activo = 0 "
               "and ap2.registro_activo = 0 and (ap2.ARMADODEEP - ap1.ARMADODEEP) >= 0 "
               "and (ap2.ARMADODEEP - ap1.ARMADODEEP) < 4 and ap2.NIVEL >= 2 group by ap1.clave;")
    return db_session.execute(sql).all()


def update_qualified_leaders(qualified_leaders_counter, client_id, db_session):
    sql = text(
        "update app_performance_airflow set lideres_calificadas = :qualified_leaders_counter where clave = :client_id and registro_activo = 0 ")
    db_session.execute(sql, {'qualified_leaders_counter': qualified_leaders_counter, 'client_id': client_id})


def get_active_customers(db_session):
    sql = text("select brk_sponsor, count(clave) as cantidad, sum(VTA_PERSONAL) as vta_incorporadas "
               "from app_performance_airflow ac where VTA_PERSONAL <> 0 and registro_activo = 0 group by BRK_SPONSOR")
    return db_session.execute(sql).all()


def update_active_customers(active_incorporated_count, incorporated_sales, client_id, db_session):
    sql = text("update app_performance_airflow set incorporadas_activas = :active_incorporated_count, "
               "vta_incorporadas = :incorporated_sales where clave = :client_id and registro_activo = 0")
    db_session.execute(sql, {'active_incorporated_count': active_incorporated_count,
                             'incorporated_sales': incorporated_sales, 'client_id': client_id})


def update_status(db_session, old_status, new_status):
    sql = text("update app_performance_airflow set registro_activo = :new_status where registro_activo = :old_status")
    db_session.execute(sql, {'old_status': old_status, 'new_status': new_status})



def get_session(conn_id: str) -> Session:
    hook = MySqlHook(mysql_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    return sessionmaker(bind=engine)()


with get_session('airflow_mysql_mexico') as db_session:
    def depurate():
        logging.info("Comenzando borrado de datos actuales...")
        delete_inactives(db_session=db_session)
        logging.info("Borrado exitoso [OK]")

    def comisiones_loader():
        logging.info("Ejecutando ComisionesLoader para carga de nuevos datos...")
        copy_base_info_from_clients(db_session=db_session)
        logging.info("Carga inicial [OK]")

        # calculo de lideres_calificadas
        lista_lideres = get_qualified_leaders_count(db_session=db_session)

        for cliente in lista_lideres:
            logging.info(f"Updating qualified leader: {cliente}")
            update_qualified_leaders(db_session=db_session,
                                     client_id=cliente['clave'],
                                     qualified_leaders_counter=cliente['cant_lideres_calif'])
        db_session.commit()
        logging.info("Actualizacion lideres calificadas [OK]")

        # Calculo de activos
        logging.info("Comenzando calculo de activos...")
        lista_activos = get_active_customers(db_session=db_session)

        for cliente in lista_activos:
            logging.info(f"Updating active customers: {cliente}")
            update_active_customers(client_id=cliente['brk_sponsor'],
                                    active_incorporated_count=cliente['cantidad'],
                                    incorporated_sales=cliente['vta_incorporadas'],
                                    db_session=db_session)
        db_session.commit()

        # Cambiar los estados de los registros
        logging.info("Cambiando estados de registros...")
        update_status(db_session=db_session, old_status=1, new_status=2)
        update_status(db_session=db_session, old_status=0, new_status=1)
        db_session.commit()


with DAG(
        dag_id='00_CommissionsOldFlowStrategy',
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

    comision_loader = PythonOperator(
        task_id='comision_loader',
        python_callable=comisiones_loader,
        op_kwargs={"x": "Apache Airflow"},
        dag=dag,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> depurate_table >> comision_loader >> end_task
