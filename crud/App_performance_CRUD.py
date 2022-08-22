# from sqlalchemy import text
#
#
# def delete_inactives(db_session):
#     sql = text("delete from app_performance where registro_activo = 2")
#     db_session.execute(sql)
#     db_session.commit()
#
#
# def copy_base_info_from_clients(db_session):
#     sql = text("INSERT INTO app_performance (clave, upline, brk_sponsor, armadoini, armadofin, armadonum, armadodeep, "
#                "nivel, nivel_ant, nivel_ant_2, vta_personal, vta_grupal, vta_descendencia, saldo, grupo, region, "
#                "justcoins, registro_activo) "
#                "SELECT clave, upline, brk_sponsor, armadoini, armadofin, armadonum, armadodeep, nivelred, "
#                "plan_nivpag1, plan_nivpag2, p_puntos, g_puntos, plan_pghasta7, saldo, sj_celula, sj_region, "
#                "saldo_puntos, 0 "
#                "FROM clientes "
#                "WHERE baja = 0 and clave <> 1")
#     db_session.execute(sql)
#     db_session.commit()
#
#
# def get_qualified_leaders_count(db_session):
#     sql = text("select ap1.CLAVE as clave, count(ap2.clave) as cant_lideres_calif "
#                "from app_performance ap1, app_performance ap2 "
#                "where ap2.ARMADONUM between (ap1.ARMADOINI + 1) and ap1.ARMADOFIN and ap1.registro_activo = 0 "
#                "and ap2.registro_activo = 0 and (ap2.ARMADODEEP - ap1.ARMADODEEP) >= 0 "
#                "and (ap2.ARMADODEEP - ap1.ARMADODEEP) < 4 and ap2.NIVEL >= 2 group by ap1.clave;")
#     return db_session.execute(sql).all()
#
#
# def update_qualified_leaders(qualified_leaders_counter, client_id, db_session):
#     sql = text("update app_performance set lideres_calificadas = :qualified_leaders_counter where clave = :client_id and registro_activo = 0 ")
#     db_session.execute(sql, {'qualified_leaders_counter': qualified_leaders_counter,'client_id': client_id})
#
#
# def get_active_customers(db_session):
#     sql = text("select brk_sponsor, count(clave) as cantidad, sum(VTA_PERSONAL) as vta_incorporadas "
#                "from app_performance ac where VTA_PERSONAL <> 0 and registro_activo = 0 group by BRK_SPONSOR")
#     return db_session.execute(sql).all()
#
#
# def update_active_customers(active_incorporated_count, incorporated_sales, client_id, db_session):
#     sql = text("update app_performance set incorporadas_activas = :active_incorporated_count, "
#                "vta_incorporadas = :incorporated_sales where clave = :client_id and registro_activo = 0")
#     db_session.execute(sql, {'active_incorporated_count': active_incorporated_count, 'incorporated_sales': incorporated_sales, 'client_id': client_id})
#
#
# def update_status(db_session, old_status, new_status):
#     sql = text("update app_performance set registro_activo = :new_status where registro_activo = :old_status")
#     db_session.execute(sql, {'old_status': old_status, 'new_status': new_status})
