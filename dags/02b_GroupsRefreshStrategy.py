# import logging
# from airflow import DAG
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python_operator import PythonOperator
# from sqlalchemy import text
# from sqlalchemy.orm import sessionmaker, Session
# from airflow.hooks.mysql_hook import MySqlHook
#
# from datetime import datetime
#
#
# class CalendarAPI:
#     def __init__(self):w
#         self.url = get_settings().URL_CALENDAR_API
#         logging.debug("URL calendar srv: " + self.url)
#
#     def get_risk_date(self):
#         try:
#             #entry_id = 'c-rd' + get_locale_date_YYYYMMDD()
#             #cache_entry = get_cache_entry(entry_id=entry_id)
#             #if cache_entry is not None:
#             #    return cache_entry
#
#             response = requests.get(self.url + get_settings().URL_CALENDAR_API_RISK_PREFIX)
#             response_dict = response.json()
#             response_object = DateRiskResponse(**response_dict)
#             #set_cache_entry(entry_id=entry_id, entry=response_object, time_expiration=get_daily_expiration())
#             return response_object
#         except Exception as e:
#             logging.error("Error al conectar al servicio interno CalendarAPI: " + str(e))
#             raise ConnectionError("Error al conectar con servicio de Calendar Risk Date")
#
#     def get_period_data(self):
#         try:
#             #entry_id = 'c-pd'+get_locale_date_YYYYMMDD()
#             #cache_entry = get_cache_entry(entry_id=entry_id)
#             #if cache_entry is not None:
#             #    return cache_entry
#
#             response = requests.get(self.url + get_settings().URL_CALENDAR_API_PERIOD_DATA)
#             response_dict = response.json()
#             response_object = PeriodDataResponse(**response_dict)
#             #set_cache_entry(entry_id=entry_id, entry=response_object, time_expiration=get_daily_expiration())
#             return response_object
#         except Exception as e:
#             logging.error("Error al conectar al servicio interno CalendarAPI: " + str(e))
#             raise ConnectionError("Error al conectar con servicio de Calendar Period-data")
#
#     def get_last_period_data(self):
#         try:
#             #entry_id = 'c-lp' + get_locale_date_YYYYMMDD()
#             #cache_entry = get_cache_entry(entry_id=entry_id)
#             #if cache_entry is not None:
#             #    return cache_entry
#
#             response = requests.get(self.url + get_settings().URL_CALENDAR_API_LAST_PERIOD_DATA)
#             response_dict = response.json()
#             response_object = PeriodDataResponse(**response_dict)
#             #set_cache_entry(entry_id=entry_id, entry=response_object, time_expiration=get_daily_expiration())
#             return response_object
#         except Exception as e:
#             logging.error("Error al conectar al servicio interno CalendarAPI: " + str(e))
#             logging.exception("message")
#             raise ConnectionError("Error al conectar con servicio de Calendar Last-period-data")
#
#
# class DateRiskResponse:
#     def __init__(self, risk_date: bool):
#         self.risk_date = risk_date
#
#
# class PeriodDataResponse:
#     def __init__(self, currentWeek, periodStartDate, periodEndDate, periodProcessGap, currentPeriod, currentYearPeriod, periodWeekCount, periodWeeks, **kwrgs):
#         self.currentWeek = currentWeek
#         self.periodStartDate = periodStartDate
#         self.periodEndDate = periodEndDate
#         self.periodProcessGap = periodProcessGap
#         self.currentPeriod = currentPeriod
#         self.currentYearPeriod = currentYearPeriod
#         self.periodWeekCount = periodWeekCount
#         self.periodWeeks = periodWeeks
#
#
#
# def get_groups_news(db_session, actual_period):
#     sql = text("select sj_celula as clave, clave as child "
#                "from clientes "
#                "where BAJA = 0 and BRK_SPONSOR <> 0 and periodo = :actual_period")
#     return db_session.execute(sql, {'actual_period': actual_period}).all()
#
#
# def load_new_customer_group_data(db_session, clave, child):
#     sql = text("insert into app_cli_grupo_airflow(acg_clave, acg_child, acg_registro_activo) "
#                "values (:clave, :child, 1) ON DUPLICATE KEY update acg_registro_activo = 1")
#     db_session.execute(sql, {'clave': clave, 'child': child})
#     db_session.commit()
#
#
# def get_session(conn_id: str) -> Session:
#     hook = MySqlHook(mysql_conn_id=conn_id)
#     engine = hook.get_sqlalchemy_engine()
#     return sessionmaker(bind=engine)()
#
# with get_session('airflow_mysql_mexico') as db_session:
#     def refresher():
#         logging.info("Comenzando group refresh...")
#         actual_period = CalendarAPI().get_period_data().currentYearPeriod
#         for new_customer in get_groups_news(db_session=db_session,
#                                             actual_period=actual_period):
#             load_new_customer_group_data(db_session=db_session,
#                                          clave=new_customer['clave'],
#                                          child=new_customer['child'])
#         logging.info("Fin de carga correcto [OK]")
#
#
# with DAG(
#         dag_id='02b_GroupsRefreshStrategy',
#         start_date=datetime(2022, 5, 28),
#         schedule_interval=None
# ) as dag:
#     start_task = EmptyOperator(
#         task_id='start'
#     )
#
#     group_refresh = PythonOperator(
#         task_id='group_refresh',
#         python_callable=refresher,
#         op_kwargs={"x": "Apache Airflow"},
#         dag=dag,
#     )
#
#     end_task = EmptyOperator(
#         task_id='end'
#     )
#
# start_task >> group_refresh >> end_task