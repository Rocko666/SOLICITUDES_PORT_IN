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
def func_proceso_principal(sqlContext, val_fecha_ejecucion, val_base_reportes, val_base_temporales, val_base_rdb_consultas, val_base_des_consultas, val_otc_t_r_om_portin_co,val_otc_t_r_om_portin_co_mpn_sr, val_otc_t_r_ri_mobile_phone_number, val_otc_t_r_usr_users, val_otc_t_r_pmgt_store,val_otc_t_r_cim_res_cust_acct, val_otc_t_r_cim_bsns_cust_acct, val_otc_t_r_pim_cust_category, val_otc_t_r_ri_number_owner, val_otc_t_r_am_sim,val_otc_t_r_boe_sales_ord, val_otc_t_r_boe_ord_item, val_otc_t_nc_list_values, val_otc_t_vw_list_values):
    
    # controlpoint1    
    val_str_controlpoint1, df_mksharevozdatos_90, df_mksharevozdatos_90_max, df_mksharevozdatos_90_final = fun_controlpoint1(sqlContext, val_fecha_ejecucion)

    # controlpoint2
    val_str_controlpoint2 = fun_controlpoint2(sqlContext, df_mksharevozdatos_90_final, val_base_reportes)
    
    # Mensajes
    val_str_resultado = val_str_controlpoint1
    
    val_str_resultado = val_str_resultado + "\n" + val_str_controlpoint2
        
    return val_str_resultado

# FUNCIONES PRINCIPALES DE CONTROL

@seguimiento_transformacion
def fun_controlpoint1(sqlContext, val_fecha_ejecucion,  val_franja_horaria):
    
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint1 ==============='))
    print(msg_succ('================================================================'))
    
    df_mksharevozdatos_90, val_str_df_mksharevozdatos_90= fun_cargar_df_mksharevozdatos_90(sqlContext, val_franja_horaria)    
    df_mksharevozdatos_90_max, val_str_df_mksharevozdatos_90_max= fun_cargar_df_mksharevozdatos_90_max(sqlContext, val_fecha_ejecucion)
    df_mksharevozdatos_90_final, val_str_df_mksharevozdatos_90_final= fun_cargar_df_mksharevozdatos_90_final(sqlContext, val_fecha_ejecucion)
        
    val_str_resultado_cp1 = val_str_df_mksharevozdatos_90 + "\n" + val_str_df_mksharevozdatos_90_max 
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_mksharevozdatos_90_final
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n"
    
    return val_str_resultado_cp1, df_mksharevozdatos_90, df_mksharevozdatos_90_max, df_mksharevozdatos_90_final
    
    
@seguimiento_transformacion
def fun_controlpoint2(sqlContext, df_mksharevozdatos_90_final, val_fecha_ejecucion, val_base_reportes, val_otc_t_360_ubicacion):
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint2 ==============='))
    print(msg_succ('================================================================'))
    
    val_str_elim_otc_t_360_ubicacion = fun_eliminar_df_otc_t_360_ubicacion(sqlContext, val_fecha_ejecucion)
    val_str_cargar_otc_t_360_ubicacion = fun_cargar_df_otc_t_360_ubicacion(sqlContext, df_mksharevozdatos_90_final, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion)
    
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
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_om_portin_co_mpn_sr)
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
    df_otc_t_r_boe_sales_ord_tmp = fun_otc_t_r_boe_sales_ord(sqlContext, val_base_rdb_consultas, val_otc_t_r_boe_sales_ord)
    
    df_otc_t_r_boe_sales_ord = df_otc_t_r_boe_sales_ord_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_r_boe_sales_ord)
    return df_otc_t_r_boe_sales_ord, "Transformacion => fun_cargar_df_otc_t_r_boe_sales_ord => df_otc_t_r_boe_sales_ord" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_rdb.otc_t_r_boe_ord_item
def fun_cargar_df_otc_t_r_boe_ord_item(sqlContext):
    df_otc_t_r_boe_ord_item_tmp = fun_otc_t_r_boe_ord_item(sqlContext, val_base_rdb_consultas, val_otc_t_r_boe_ord_item)
    
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
##### hasta aqui

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PLAN_1
def fun_cargar_df_mksharevozdatos_90_max(sqlContext):
    df_mksharevozdatos_90_max_tmp = fun_mksharevozdatos_90_max(sqlContext, val_base_ipaccess_consultas, val_mksharevozdatos_90, val_fecha_ejecucion)
    
    df_mksharevozdatos_90_max = df_mksharevozdatos_90_max_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_mksharevozdatos_90_max)
    return df_mksharevozdatos_90_max, "Transformacion => fun_cargar_df_mksharevozdatos_90_max => df_mksharevozdatos_90_max" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ACTUAL_CP
def fun_cargar_df_mksharevozdatos_90_final(sqlContext, df_parque_actual_cp_1, df_parque_actual_cp_2):
    t1 = df_mksharevozdatos_90.alias('t1')
    t2 = df_mksharevozdatos_90_max.alias('t2')
    
    df_mksharevozdatos_90_final_tmp =  t1.join(t2, expr("t2.max_fecha = t1.fecha_proceso"), how='inner') \
                                .selectExpr(u"t1.*")
    
    df_mksharevozdatos_90_final = df_mksharevozdatos_90_final_tmp.cache()
    
    str_datos_df = fun_obtener_datos_df(val_se_calcula,df_mksharevozdatos_90_final)
    return df_mksharevozdatos_90_final, "Transformacion => fun_cargar_df_mksharevozdatos_90_final => df_mksharevozdatos_90_final" + str_datos_df

### control point 2
@seguimiento_transformacion
#  Eliminados los Datos en la tabla de HIVE => db_reportes.otc_t_360_ubicacion
def fun_eliminar_df_otc_t_360_ubicacion(sqlContext, val_fecha_ejecucion):
    print(msg_succ('======== Fechas ::: fun_eliminar_df_otc_t_360_ubicacion ==========='))
    print( msg_succ('\n %s\n') % ( str(val_fecha_ejecucion) ) )
    
    val_retorno_insercion = fun_eliminar_particiones(sqlContext, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion, val_fecha_ejecucion, val_fecha_ejecucion)
    print(msg_succ('======== df_otc_t_360_ubicacion =>  fun_eliminar_particiones ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    str_datos_df = str(val_retorno_insercion)
    return "Transformacion => fun_eliminar_df_otc_t_360_ubicacion => df_otc_t_360_ubicacion " + str_datos_df

@seguimiento_transformacion
#  Cargamos los Datos en la tabla de HIVE => db_reportes.otc_t_360_ubicacion
def fun_cargar_df_otc_t_360_ubicacion(sqlContext, df_mksharevozdatos_90_final, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion): 
    
    # Insercion de Datos
    val_retorno_insercion = fun_cargar_datos_dinamico(sqlContext, df_mksharevozdatos_90_final, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion, val_fecha_ejecucion, val_fecha_ejecucion)
    print(msg_succ('======== df_otc_t_360_ubicacion  =>  fun_cargar_datos_dinamico ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    str_datos_df = fun_obtener_datos_df(val_se_calcula,df_otc_t_360_ubicacion)
    return "Transformacion => fun_cargar_df_otc_t_360_ubicacion => df_otc_t_360_ubicacion" + str_datos_df
    