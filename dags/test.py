# class SQLAlchemyOperator(PythonOperator):
#     """
#     PythonOperator with SQLAlchemy session management - creates session for the Python callable
#     and commit/rollback it afterwards.
#
#     Set `conn_id` with you DB connection.
#
#     Pass `session` parameter to the python callable.
#     """
#     @apply_defaults
#     def __init__(
#             self,
#             conn_id: str,
#             *args, **kwargs):
#         self.conn_id = conn_id
#         super().__init__(*args, **kwargs)
#
#     def execute_callable(self):
#         session = get_session(self.conn_id)
#         try:
#             result = self.python_callable(*self.op_args, session=session, **self.op_kwargs)
#         except Exception:
#             session.rollback()
#             raise
#         session.commit()
#         return result
#
#
#
# with DAG(
#     dag_id='SQAlchemyDAG',
#     schedule_interval='0 2 1 * *',  # monthly at 2:00 AM, 1st day of a month
#     start_date=datetime(2020, 8, 1),
# ) as dag:
#
#     def sqlalchemy_task(session: Session, **kwargs):
#         session.query(YourSQLAlchemyModel)
#
#
#     request_count = SQLAlchemyOperator(
#         dag=dag,
#         task_id='sqlalchemy',
#         conn_id='my_db',
#         python_callable=sqlalchemy_task,
#         provide_context=True,
#     )
