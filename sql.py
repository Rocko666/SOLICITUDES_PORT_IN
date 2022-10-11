# -*- coding: utf-8 -*-

from Funciones.funcion import *

# db_desarrollo2021.otc_t_r_om_portin_co
@cargar_consulta
def fun_otc_t_r_om_portin_co(base_des_consultas, otc_t_r_om_portin_co, fecha_ejecucion):
    qry = '''
    SELECT 
        object_id
        , request_status
        , ascp_response
        , fvc
        , donor_account_type
        , created_when
        , ascp_rejection_comment
        , assigned_csr
        , customer_account
        , donor_operator
        , sales_order
    FROM {bdd_consultas}.{tabla_otc_t_r_om_portin_co}
    WHERE pt_fecha = {fecha_eje}
    '''.format(bdd_consultas=base_des_consultas, tabla_otc_t_r_om_portin_co=otc_t_r_om_portin_co, fecha_eje=fecha_ejecucion)
    return qry 

# db_desarrollo2021.otc_t_r_om_portin_co_mpn_sr
@cargar_consulta
def fun_otc_t_r_om_portin_co_mpn_sr(base_des_consultas, otc_t_r_om_portin_co_mpn_sr):
    qry = '''
    SELECT 
        object_id
        , value
    FROM {bdd_consultas}.{tabla_otc_t_r_om_portin_co_mpn_sr}
    '''.format(bdd_consultas=base_des_consultas, tabla_otc_t_r_om_portin_co_mpn_sr=otc_t_r_om_portin_co_mpn_sr)
    return qry 

# db_rdb.otc_t_r_ri_mobile_phone_number
@cargar_consulta
def fun_otc_t_r_ri_mobile_phone_number(base_rdb_consultas, otc_t_r_ri_mobile_phone_number):
    qry = '''
    SELECT 
        name
        , object_id
        , assoc_sim_iccid
    FROM {bdd_consultas}.{tabla_otc_t_r_ri_mobile_phone_number}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_r_ri_mobile_phone_number=otc_t_r_ri_mobile_phone_number)
    return qry 

# db_rdb.otc_t_r_usr_users
@cargar_consulta
def fun_otc_t_r_usr_users(base_rdb_consultas, otc_t_r_usr_users):
    qry = '''
    SELECT 
        name
        , object_id
        , current_location
    FROM {bdd_consultas}.{tabla_otc_t_r_usr_users}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_r_usr_users=otc_t_r_usr_users)
    return qry 

# db_rdb.otc_t_r_pmgt_store
@cargar_consulta
def fun_otc_t_r_pmgt_store(base_rdb_consultas, otc_t_r_pmgt_store):
    qry = '''
    SELECT object_id
    FROM {bdd_consultas}.{tabla_otc_t_r_pmgt_store}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_r_pmgt_store=otc_t_r_pmgt_store)
    return qry 

# db_rdb.otc_t_r_cim_res_cust_acct
@cargar_consulta
def fun_otc_t_r_cim_res_cust_acct(base_rdb_consultas, otc_t_r_cim_res_cust_acct):
    qry = '''
    SELECT 
        cust_acct_number
        , doc_number
        , doc_type
        , object_id
        , cust_category
    FROM {bdd_consultas}.{tabla_otc_t_r_cim_res_cust_acct}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_r_cim_res_cust_acct=otc_t_r_cim_res_cust_acct)
    return qry 

# db_rdb.otc_t_r_cim_bsns_cust_acct
@cargar_consulta
def fun_otc_t_r_cim_bsns_cust_acct(base_rdb_consultas, otc_t_r_cim_bsns_cust_acct):
    qry = '''
    SELECT 
        cust_acct_number
        , doc_number
        , doc_type
        , object_id
        , cust_category
    FROM {bdd_consultas}.{tabla_otc_t_r_cim_bsns_cust_acct}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_r_cim_bsns_cust_acct=otc_t_r_cim_bsns_cust_acct)
    return qry 

# db_rdb.otc_t_r_pim_cust_category
@cargar_consulta
def fun_otc_t_r_pim_cust_category(base_rdb_consultas, otc_t_r_pim_cust_category):
    qry = '''
    SELECT 
        name
        , object_id
    FROM {bdd_consultas}.{tabla_otc_t_r_pim_cust_category}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_r_pim_cust_category=otc_t_r_pim_cust_category)
    return qry 

# db_rdb.otc_t_r_ri_number_owner
@cargar_consulta
def fun_otc_t_r_ri_number_owner(base_rdb_consultas, otc_t_r_ri_number_owner):
    qry = '''
    SELECT 
        name
        , object_id
    FROM {bdd_consultas}.{tabla_otc_t_r_ri_number_owner}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_r_ri_number_owner=otc_t_r_ri_number_owner)
    return qry 

# db_rdb.otc_t_r_am_sim
@cargar_consulta
def fun_otc_t_r_am_sim(base_rdb_consultas, otc_t_r_am_sim):
    qry = '''
    SELECT 
        name 
        , object_id
    FROM  {bdd_consultas}.{tabla_otc_t_r_am_sim}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_r_am_sim=otc_t_r_am_sim)
    return qry 

# db_rdb.otc_t_r_boe_sales_ord
@cargar_consulta
def fun_otc_t_r_boe_sales_ord(base_rdb_consultas, otc_t_r_boe_sales_ord, fecha_ejecucion):
    qry = '''
    SELECT 
        object_id
        , processed_when
        , sales_ord_status
        , submitted_by
    FROM {bdd_consultas}.{tabla_otc_t_r_boe_sales_ord}
    WHERE pt_created_when={fecha_eje}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_r_boe_sales_ord=otc_t_r_boe_sales_ord, fecha_eje=fecha_ejecucion)
    return qry 

# db_rdb.otc_t_r_boe_ord_item
@cargar_consulta
def fun_otc_t_r_boe_ord_item(base_rdb_consultas, otc_t_r_boe_ord_item, fecha_ejecucion):
    qry = '''
    SELECT 
        tariff_plan_name
        , parent_id
        , phone_number
    FROM {bdd_consultas}.{tabla_otc_t_r_boe_ord_item}
    WHERE pt_created_when={fecha_eje}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_r_boe_ord_item=otc_t_r_boe_ord_item, fecha_eje=fecha_ejecucion)
    return qry 

# db_rdb.otc_t_nc_list_values
@cargar_consulta
def fun_otc_t_nc_list_values(base_rdb_consultas, otc_t_nc_list_values):
    qry = '''
    SELECT 
        value
        , list_value_id
    FROM {bdd_consultas}.{tabla_otc_t_nc_list_values}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_nc_list_values=otc_t_nc_list_values)
    return qry 

# db_rdb.otc_t_vw_list_values
@cargar_consulta
def fun_otc_t_vw_list_values(base_rdb_consultas, otc_t_vw_list_values):
    qry = '''
    SELECT 
        list_value_id 
        , localized_value
    FROM {bdd_consultas}.{tabla_otc_t_vw_list_values}
    '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_vw_list_values=otc_t_vw_list_values)
    return qry 
