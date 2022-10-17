# -*- coding: utf-8 -*-
from Configuraciones.configuracion import *
from Querys.sql import *
from Funciones.funcion import *
import pyspark.sql.functions as sql_fun
from pyspark.sql.functions import col, substring, max, min, when, count, sum, lit, unix_timestamp, desc, asc, length, expr, datediff, row_number, coalesce, split, trim, concat, concat_ws, to_date, last_day, to_timestamp
from pyspark.sql.types import StringType, DateType, IntegerType, StructType, TimestampType
from pyspark.sql.window import Window
import pandas as pd

@seguimiento_transformacion
# Funcion Principal 
def func_proceso_principal(sqlContext, val_fecha_ejecucion, val_base_reportes, val_base_temporales, val_base_rdb_consultas, val_base_des_consultas, val_otc_t_r_om_portin_co,val_otc_t_r_om_portin_co_mpn_sr, val_otc_t_r_ri_mobile_phone_number, val_otc_t_r_usr_users, val_otc_t_r_pmgt_store,val_otc_t_r_cim_res_cust_acct, val_otc_t_r_cim_bsns_cust_acct, val_otc_t_r_pim_cust_category, val_otc_t_r_ri_number_owner, val_otc_t_r_am_sim,val_otc_t_r_boe_sales_ord, val_otc_t_r_boe_ord_item, val_otc_t_nc_list_values, val_otc_t_vw_list_values,val_otc_t_solicitudes_port_in,val_se_calcula):
    
    # controlpoint1    
    val_str_controlpoint1, df_otc_t_r_om_portin_co, df_otc_t_r_om_portin_co_mpn_sr, df_otc_t_r_ri_mobile_phone_number, df_otc_t_r_usr_users, df_otc_t_r_pmgt_store, df_otc_t_r_cim_res_cust_acct, df_otc_t_r_cim_bsns_cust_acct, df_otc_t_r_pim_cust_category, df_otc_t_r_ri_number_owner, df_otc_t_r_am_sim, df_otc_t_r_boe_sales_ord, df_otc_t_r_boe_ord_item, df_otc_t_nc_list_values, df_otc_t_vw_list_values = fun_controlpoint1(sqlContext)

    # controlpoint2
    val_str_controlpoint2, df_sol_port_in, = fun_controlpoint2(sqlContext, df_otc_t_r_om_portin_co, df_otc_t_r_om_portin_co_mpn_sr, df_otc_t_r_ri_mobile_phone_number, df_otc_t_r_usr_users, df_otc_t_r_pmgt_store, df_otc_t_r_cim_res_cust_acct, df_otc_t_r_cim_bsns_cust_acct, df_otc_t_r_pim_cust_category, df_otc_t_r_ri_number_owner, df_otc_t_r_am_sim, df_otc_t_r_boe_sales_ord, df_otc_t_r_boe_ord_item, df_otc_t_nc_list_values, df_otc_t_vw_list_values)
    
    # Mensajes
    val_str_resultado = val_str_controlpoint1
    
    val_str_resultado = val_str_resultado + "\n" + val_str_controlpoint2
        
    return val_str_resultado

# FUNCIONES PRINCIPALES DE CONTROL

@seguimiento_transformacion
def fun_controlpoint1(sqlContext):
    
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint1 ==============='))
    print(msg_succ('================================================================'))
    
    df_otc_t_r_om_portin_co, val_str_df_otc_t_r_om_portin_co= fun_cargar_df_otc_t_r_om_portin_co(sqlContext)  
    
    df_otc_t_r_om_portin_co_mpn_sr, val_str_df_otc_t_r_om_portin_co_mpn_sr= fun_cargar_df_otc_t_r_om_portin_co_mpn_sr(sqlContext)
    
    df_otc_t_r_ri_mobile_phone_number, val_str_df_otc_t_r_ri_mobile_phone_number= fun_cargar_df_otc_t_r_ri_mobile_phone_number(sqlContext)
    
    df_otc_t_r_usr_users, val_str_df_otc_t_r_usr_users = fun_cargar_df_otc_t_r_usr_users(sqlContext)
    
    df_otc_t_r_pmgt_store, val_str_df_otc_t_r_pmgt_store = fun_cargar_df_otc_t_r_pmgt_store(sqlContext)
    
    df_otc_t_r_cim_res_cust_acct, val_str_df_otc_t_r_cim_res_cust_acct = fun_cargar_df_otc_t_r_cim_res_cust_acct(sqlContext)
    
    df_otc_t_r_cim_bsns_cust_acct, val_str_df_otc_t_r_cim_bsns_cust_acct = fun_cargar_df_otc_t_r_cim_bsns_cust_acct(sqlContext)
    
    df_otc_t_r_pim_cust_category, val_str_df_otc_t_r_pim_cust_category = fun_cargar_df_otc_t_r_pim_cust_category(sqlContext)
    
    df_otc_t_r_ri_number_owner, val_str_df_otc_t_r_ri_number_owner = fun_cargar_df_otc_t_r_ri_number_owner(sqlContext)
    
    df_otc_t_r_am_sim, val_str_df_otc_t_r_am_sim = fun_cargar_df_otc_t_r_am_sim(sqlContext)
    
    df_otc_t_r_boe_sales_ord, val_str_df_otc_t_r_boe_sales_ord = fun_cargar_df_otc_t_r_boe_sales_ord(sqlContext)
    
    df_otc_t_r_boe_ord_item, val_str_df_otc_t_r_boe_ord_item = fun_cargar_df_otc_t_r_boe_ord_item(sqlContext)
    
    df_otc_t_nc_list_values, val_str_df_otc_t_nc_list_values = fun_cargar_df_otc_t_nc_list_values(sqlContext)
    
    df_otc_t_vw_list_values, val_str_df_otc_t_vw_list_values = fun_cargar_df_otc_t_vw_list_values
    
    val_str_resultado_cp1 = val_str_df_otc_t_r_om_portin_co + "\n" + val_str_df_otc_t_r_om_portin_co_mpn_sr 
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_r_ri_mobile_phone_number
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_r_usr_users
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_r_pmgt_store
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_r_cim_res_cust_acct
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_r_cim_bsns_cust_acct
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_r_pim_cust_category
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_r_ri_number_owner
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_r_am_sim
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_r_boe_sales_ord
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_r_boe_ord_item
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_nc_list_values
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_otc_t_vw_list_values
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n"
    
    return val_str_resultado_cp1, df_otc_t_r_om_portin_co, df_otc_t_r_om_portin_co_mpn_sr, df_otc_t_r_ri_mobile_phone_number, df_otc_t_r_usr_users, df_otc_t_r_pmgt_store, df_otc_t_r_cim_res_cust_acct, df_otc_t_r_cim_bsns_cust_acct, df_otc_t_r_pim_cust_category, df_otc_t_r_ri_number_owner, df_otc_t_r_am_sim, df_otc_t_r_boe_sales_ord, df_otc_t_r_boe_ord_item, df_otc_t_nc_list_values, df_otc_t_vw_list_values
    
    
@seguimiento_transformacion
def fun_controlpoint2(sqlContext, df_otc_t_r_om_portin_co, df_otc_t_r_om_portin_co_mpn_sr, df_otc_t_r_ri_mobile_phone_number, df_otc_t_r_usr_users, df_otc_t_r_pmgt_store, df_otc_t_r_cim_res_cust_acct, df_otc_t_r_cim_bsns_cust_acct, df_otc_t_r_pim_cust_category, df_otc_t_r_ri_number_owner, df_otc_t_r_am_sim, df_otc_t_r_boe_sales_ord, df_otc_t_r_boe_ord_item, df_otc_t_nc_list_values, df_otc_t_vw_list_values, val_base_reportes, 
val_otc_t_solicitudes_port_in):
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint2 ==============='))
    print(msg_succ('================================================================'))
    
    val_str_elim_otc_t_360_ubicacion = fun_eliminar_df_otc_t_360_ubicacion(sqlContext, val_fecha_ejecucion)
    val_str_cargar_otc_t_360_ubicacion = fun_cargar_df_sol_port_in(sqlContext, df_mksharevozdatos_90_final, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion)
    
    val_str_resultado_cp2 = val_str_elim_otc_t_360_ubicacion + "\n" + val_str_cargar_otc_t_360_ubicacion 
    val_str_resultado_cp2 = val_str_resultado_cp2 + "\n"
    
    return val_str_resultado_cp2

# FUNCIONES QUE GENERAN DATAFRAME

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_desarrollo2021.otc_t_r_om_portin_co
def fun_cargar_df_otc_t_r_om_portin_co(sqlContext):
    df_otc_t_r_om_portin_co_tmp = fun_otc_t_r_om_portin_co(sqlContext, val_base_des_consultas, val_otc_t_r_om_portin_co, val_fecha_ejecucion)
    
    df_otc_t_r_om_portin_co = df_otc_t_r_om_portin_co_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_om_portin_co)
    return df_otc_t_r_om_portin_co, "Transformacion => fun_cargar_df_otc_t_r_om_portin_co => df_otc_t_r_om_portin_co" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_desarrollo2021.otc_t_r_om_portin_co_mpn_sr
def fun_cargar_df_otc_t_r_om_portin_co_mpn_sr(sqlContext):
    df_otc_t_r_om_portin_co_mpn_sr_tmp = fun_otc_t_r_om_portin_co_mpn_sr(sqlContext, val_base_des_consultas, val_otc_t_r_om_portin_co_mpn_sr)
    
    df_otc_t_r_om_portin_co_mpn_sr = df_otc_t_r_om_portin_co_mpn_sr_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_om_portin_co_mpn_sr)
    return df_otc_t_r_om_portin_co_mpn_sr, "Transformacion => fun_cargar_df_otc_t_r_om_portin_co_mpn_sr => df_otc_t_r_om_portin_co_mpn_sr" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_r_ri_mobile_phone_number
def fun_cargar_df_otc_t_r_ri_mobile_phone_number(sqlContext):
    df_otc_t_r_ri_mobile_phone_number_tmp = fun_otc_t_r_ri_mobile_phone_number(sqlContext, val_base_rdb_consultas, val_otc_t_r_ri_mobile_phone_number)
    
    df_otc_t_r_ri_mobile_phone_number = df_otc_t_r_ri_mobile_phone_number_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_ri_mobile_phone_number)
    return df_otc_t_r_ri_mobile_phone_number, "Transformacion => fun_cargar_df_otc_t_r_ri_mobile_phone_number => df_otc_t_r_ri_mobile_phone_number" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_r_usr_users
def fun_cargar_df_otc_t_r_usr_users(sqlContext):
    df_otc_t_r_usr_users_tmp = fun_otc_t_r_usr_users(sqlContext, val_base_rdb_consultas, val_otc_t_r_usr_users)
    
    df_otc_t_r_usr_users = df_otc_t_r_usr_users_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_usr_users)
    return df_otc_t_r_usr_users, "Transformacion => fun_cargar_df_otc_t_r_usr_users => df_otc_t_r_usr_users" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_r_pmgt_store
def fun_cargar_df_otc_t_r_pmgt_store(sqlContext):
    df_otc_t_r_pmgt_store_tmp = fun_otc_t_r_pmgt_store(sqlContext, val_base_rdb_consultas, val_otc_t_r_pmgt_store)
    
    df_otc_t_r_pmgt_store = df_otc_t_r_pmgt_store_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_pmgt_store)
    return df_otc_t_r_pmgt_store, "Transformacion => fun_cargar_df_otc_t_r_pmgt_store => df_otc_t_r_pmgt_store" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_r_cim_res_cust_acct
def fun_cargar_df_otc_t_r_cim_res_cust_acct(sqlContext):
    df_otc_t_r_cim_res_cust_acct_tmp = fun_otc_t_r_cim_res_cust_acct(sqlContext, val_base_rdb_consultas, val_otc_t_r_cim_res_cust_acct)
    
    df_otc_t_r_cim_res_cust_acct = df_otc_t_r_cim_res_cust_acct_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_cim_res_cust_acct)
    return df_otc_t_r_cim_res_cust_acct, "Transformacion => fun_cargar_df_otc_t_r_cim_res_cust_acct => df_otc_t_r_cim_res_cust_acct" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_r_cim_bsns_cust_acct
def fun_cargar_df_otc_t_r_cim_bsns_cust_acct(sqlContext):
    df_otc_t_r_cim_bsns_cust_acct_tmp = fun_otc_t_r_cim_bsns_cust_acct(sqlContext, val_base_rdb_consultas, val_otc_t_r_cim_bsns_cust_acct)
    
    df_otc_t_r_cim_bsns_cust_acct = df_otc_t_r_cim_bsns_cust_acct_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_cim_bsns_cust_acct)
    return df_otc_t_r_cim_bsns_cust_acct, "Transformacion => fun_cargar_df_otc_t_r_cim_bsns_cust_acct => df_otc_t_r_cim_bsns_cust_acct" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_r_pim_cust_category
def fun_cargar_df_otc_t_r_pim_cust_category(sqlContext):
    df_otc_t_r_pim_cust_category_tmp = fun_otc_t_r_pim_cust_category(sqlContext, val_base_rdb_consultas, val_otc_t_r_pim_cust_category)
    
    df_otc_t_r_pim_cust_category = df_otc_t_r_pim_cust_category_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_pim_cust_category)
    return df_otc_t_r_pim_cust_category, "Transformacion => fun_cargar_df_otc_t_r_pim_cust_category => df_otc_t_r_pim_cust_category" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_r_ri_number_owner
def fun_cargar_df_otc_t_r_ri_number_owner(sqlContext):
    df_otc_t_r_ri_number_owner_tmp = fun_otc_t_r_ri_number_owner(sqlContext, val_base_rdb_consultas, val_otc_t_r_ri_number_owner)
    
    df_otc_t_r_ri_number_owner = df_otc_t_r_ri_number_owner_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_ri_number_owner)
    return df_otc_t_r_ri_number_owner, "Transformacion => fun_cargar_df_otc_t_r_ri_number_owner => df_otc_t_r_ri_number_owner" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_r_am_sim
def fun_cargar_df_otc_t_r_am_sim(sqlContext):
    df_otc_t_r_am_sim_tmp = fun_otc_t_r_am_sim(sqlContext, val_base_rdb_consultas, val_otc_t_r_am_sim)
    
    df_otc_t_r_am_sim = df_otc_t_r_am_sim_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_am_sim)
    return df_otc_t_r_am_sim, "Transformacion => fun_cargar_df_otc_t_r_am_sim => df_otc_t_r_am_sim" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_r_boe_sales_ord
def fun_cargar_df_otc_t_r_boe_sales_ord(sqlContext):
    df_otc_t_r_boe_sales_ord_tmp = fun_otc_t_r_boe_sales_ord(sqlContext, val_base_rdb_consultas, val_otc_t_r_boe_sales_ord, val_fecha_ejecucion)
    
    df_otc_t_r_boe_sales_ord = df_otc_t_r_boe_sales_ord_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_boe_sales_ord)
    return df_otc_t_r_boe_sales_ord, "Transformacion => fun_cargar_df_otc_t_r_boe_sales_ord => df_otc_t_r_boe_sales_ord" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_r_boe_ord_item
def fun_cargar_df_otc_t_r_boe_ord_item(sqlContext):
    df_otc_t_r_boe_ord_item_tmp = fun_otc_t_r_boe_ord_item(sqlContext, val_base_rdb_consultas, val_otc_t_r_boe_ord_item, val_fecha_ejecucion)
    
    df_otc_t_r_boe_ord_item = df_otc_t_r_boe_ord_item_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_boe_ord_item)
    return df_otc_t_r_boe_ord_item, "Transformacion => fun_cargar_df_otc_t_r_boe_ord_item => df_otc_t_r_boe_ord_item" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_nc_list_values
def fun_cargar_df_otc_t_nc_list_values(sqlContext):
    df_otc_t_nc_list_values_tmp = fun_otc_t_nc_list_values(sqlContext, val_base_rdb_consultas, val_otc_t_nc_list_values)
    
    df_otc_t_nc_list_values = df_otc_t_nc_list_values_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_nc_list_values)
    return df_otc_t_nc_list_values, "Transformacion => fun_cargar_df_otc_t_nc_list_values => df_otc_t_nc_list_values" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_vw_list_values
def fun_cargar_df_otc_t_vw_list_values(sqlContext):
    df_otc_t_vw_list_values_tmp = fun_otc_t_vw_list_values(sqlContext, val_base_rdb_consultas, val_otc_t_vw_list_values)
    
    df_otc_t_vw_list_values = df_otc_t_vw_list_values_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_vw_list_values)
    return df_otc_t_vw_list_values, "Transformacion => fun_cargar_df_otc_t_vw_list_values => df_otc_t_vw_list_values" + str_datos_df

### control point 2
@seguimiento_transformacion
# Generamos un dataframe en base a los datos de las tablas cargadas en los dataframes anteriores 
def fun_cargar_df_sol_port_in(sqlContext, df_otc_t_r_om_portin_co, df_otc_t_r_om_portin_co_mpn_sr, df_otc_t_r_ri_mobile_phone_number, df_otc_t_r_usr_users, df_otc_t_r_pmgt_store, df_otc_t_r_cim_res_cust_acct, df_otc_t_r_cim_bsns_cust_acct, df_otc_t_r_pim_cust_category, df_otc_t_r_ri_number_owner, df_otc_t_r_am_sim, df_otc_t_r_boe_sales_ord, df_otc_t_r_boe_ord_item, df_otc_t_nc_list_values, df_otc_t_vw_list_values):
    
    t_a = df_otc_t_r_om_portin_co.alias('a')
    # print(msg_succ('df_otc_t_r_om_portin_co  %s\n') % (df_otc_t_r_om_portin_co.printSchema()))
    
    t_mpn = df_otc_t_r_om_portin_co_mpn_sr.alias('mpn')
    # print(msg_succ('df_otc_t_r_om_portin_co_mpn_sr  %s\n') % (df_otc_t_r_om_portin_co_mpn_sr.printSchema()))
    
    t_ri = df_otc_t_r_ri_mobile_phone_number.alias('ri')
    # print(msg_succ('df_otc_t_r_ri_mobile_phone_number  %s\n') % (df_otc_t_r_ri_mobile_phone_number.printSchema()))
    
    t_u = df_otc_t_r_usr_users.alias('u')
    # print(msg_succ('df_otc_t_r_usr_users  %s\n') % (df_otc_t_r_usr_users.printSchema()))
    
    t_t = df_otc_t_r_pmgt_store.alias('t')
    # print(msg_succ('df_otc_t_r_pmgt_store  %s\n') % (df_otc_t_r_pmgt_store.printSchema()))
    
    t_cr = df_otc_t_r_cim_res_cust_acct.alias('cr')
    # print(msg_succ('df_otc_t_r_cim_res_cust_acct  %s\n') % (df_otc_t_r_cim_res_cust_acct.printSchema()))
    
    t_cb = df_otc_t_r_cim_bsns_cust_acct.alias('cb')
    # print(msg_succ('df_otc_t_r_cim_bsns_cust_acct  %s\n') % (df_otc_t_r_cim_bsns_cust_acct.printSchema()))
    
    t_ctg = df_otc_t_r_pim_cust_category.alias('ctg')
    # print(msg_succ('df_otc_t_r_pim_cust_category  %s\n') % (df_otc_t_r_pim_cust_category.printSchema()))
    
    t_donor = df_otc_t_r_ri_number_owner.alias('donor')
    # print(msg_succ('df_otc_t_r_ri_number_owner  %s\n') % (df_otc_t_r_ri_number_owner.printSchema()))
    
    t_sim = df_otc_t_r_am_sim.alias('sim')
    # print(msg_succ('df_otc_t_r_am_sim  %s\n') % (df_otc_t_r_am_sim.printSchema()))
    
    t_o = df_otc_t_r_boe_sales_ord.alias('o')
    # print(msg_succ('df_otc_t_r_boe_sales_ord  %s\n') % (df_otc_t_r_boe_sales_ord.printSchema()))
    
    t_oi = df_otc_t_r_boe_ord_item.alias('oi')
    # print(msg_succ('df_otc_t_r_boe_ord_item  %s\n') % (df_otc_t_r_boe_ord_item.printSchema()))
    
    t_nclv = df_otc_t_nc_list_values.alias('nclv')
    # print(msg_succ('df_otc_t_nc_list_values  %s\n') % (df_otc_t_nc_list_values.printSchema()))
    
    t_nclv2 = df_otc_t_nc_list_values.alias('nclv2')
    # print(msg_succ('df_otc_t_nc_list_values  %s\n') % (df_otc_t_nc_list_values.printSchema()))
    
    t_lv = df_otc_t_nc_list_values.alias('lv')
    # print(msg_succ('df_otc_t_nc_list_values  %s\n') % (df_otc_t_nc_list_values.printSchema()))
    
    t_lv3 = df_otc_t_nc_list_values.alias('lv3')
    # print(msg_succ('df_otc_t_nc_list_values  %s\n') % (df_otc_t_nc_list_values.printSchema()))
    
    t_lv4 = df_otc_t_nc_list_values.alias('lv4')
    # print(msg_succ('df_otc_t_nc_list_values  %s\n') % (df_otc_t_nc_list_values.printSchema()))
    
    t_vwlv = df_otc_t_vw_list_values.alias('vwlv')
    # print(msg_succ('df_otc_t_vw_list_values  %s\n') % (df_otc_t_vw_list_values.printSchema()))
        
    
    df_sol_port_in_tmp = t_a.join(t_mpn, expr(" mpn.object_id = a.object_id ") , how='left').\
        join(t_ri, expr(" mpn.value = ri.object_id "), how='left').\
        join(t_u, expr(" a.assigned_csr = u.object_id "), how='left').\
        join(t_t, expr(" t.object_id = u.current_location "), how='left').\
        join(t_cr, expr(" a.customer_account = cr.object_id "), how='left').\
        join(t_cb, expr(" a.customer_account = cb.object_id "), how='left').\
        join(t_ctg, expr(" ctg.object_id = nvl(cb.cust_category, cr.cust_category) "), how='left').\
        join(t_donor, expr(" donor.object_id = a.donor_operator "), how='left').\
        join(t_sim, expr(" sim.object_id = ri.assoc_sim_iccid "), how='left').\
        join(t_o, expr(" o.object_id = a.sales_order "), how='inner').\
        join(t_oi, expr(" oi.parent_id = a.sales_order and oi.phone_number = ri.object_id "), how='left').\
        join(t_nclv, expr(" nclv.list_value_id = cr.doc_type "), how='left').\
        join(t_nclv2, expr(" nclv2.list_value_id = cb.doc_type "), how='left').\
        join(t_lv, expr(" lv.list_value_id = a.request_status "), how='left').\
        join(t_vwlv, expr(" vwlv.list_value_id = a.ascp_response "), how='left').\
        join(t_lv3, expr(" lv3.list_value_id = a.donor_account_type "), how='left').\
        join(t_lv4, expr(" lv4.list_value_id = o.sales_ord_status "), how='left').\
        selectExpr('case when cr.cust_acct_number is null then cb.cust_acct_number else cr.cust_acct_number end as CustomerAccountNumber', 'case when cr.doc_number is null then cb.doc_number else cr.doc_number end doc_number', 'case when nclv2.value is null then nclv.value else nclv2.value end as doc_type','ctg.name as CustomerCategory','to_char( a.object_id) as PortinCommonOrderID','lv.value as RequestStatus','vwlv.localized_value as estado','to_char(a.fvc) as fvc','donor.name as operadora','to_char(a.donor_account_type) as donor_account_type1','lv3.value as LN_Origen','u.name as AssignedCSR','a.created_when as created_when','to_char(o.object_id) as SalesOrderID','lv4.value as SalesOrderStatus','o.processed_when as SalesOrderProcessedDate','ri.name as telefono','substr(sim.name,1,19) as AssociatedSIMICCID','oi.tariff_plan_name as PlanDestino','a.ascp_rejection_comment  as motivo_rechazo')
    
    # print(msg_succ('========================================================================================'))
    # print(msg_succ('=========================== fun_cargar_df_sol_port_in_tmp =========================='))
    # print(msg_succ('========================================================================================'))
    # print(msg_succ('df_sol_port_in_tmp  %s\n => %s\n') % (df_sol_port_in_tmp.printSchema(),str(df_sol_port_in_tmp.count())))
    
    df_sol_port_in = df_sol_port_in_tmp.cache()
    
    str_datos_df = fun_obtener_datos_df(val_se_calcula,df_sol_port_in)
    return df_sol_port_in, "Transformacion => fun_cargar_df_sol_port_in => df_sol_port_in" + str_datos_df
    

@seguimiento_transformacion
#  Cargamos los Datos en la tabla de HIVE => db_desarrollo2021.otc_t_solicitudes_port_in
def fun_cargar_df_otc_t_solicitudes_port_in(sqlContext, df_sol_port_in, val_base_reportes, val_otc_t_solicitudes_port_in): 
    
    # Insercion de Datos
    val_retorno_insercion = fun_cargar_datos_dinamico_tabla_sin_particion(sqlContext, df_sol_port_in, val_base_reportes, val_otc_t_solicitudes_port_in)
    print(msg_succ('======== df_otc_t_solicitudes_port_in  =>  fun_cargar_datos_dinamico ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    str_datos_df = fun_obtener_datos_df(val_se_calcula,df_otc_t_solicitudes_port_in)
    return "Transformacion => fun_cargar_df_otc_t_solicitudes_port_in => df_otc_t_solicitudes_port_in" + str_datos_df
    