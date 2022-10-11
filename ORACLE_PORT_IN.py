from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext
import argparse

from datetime import datetime, timedelta
from Transformaciones.transformacion import *
from Funciones.funcion import *
from dateutil.relativedelta import *
from pyspark.sql.types import StringType, DateType, IntegerType, StructType

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


# Validamos los parametros de entrada
def entrada(reproceso, formato_fecha, fecha_ejecucion):
    validar_fecha(fecha_ejecucion, formato_fecha)
    
    val_fecha_proc = obtener_fecha_del_proceso(fecha_ejecucion, formato_fecha)
    
    return val_fecha_proc

# Unificamos las funciones que vamos a ejecutar
def proceso(sqlContext, fecha_ejecucion, base_reportes, base_temporales, base_rdb_consultas, base_des_consultas, otc_t_r_om_portin_co,otc_t_r_om_portin_co_mpn_sr, otc_t_r_ri_mobile_phone_number, otc_t_r_usr_users, otc_t_r_pmgt_store,otc_t_r_cim_res_cust_acct, otc_t_r_cim_bsns_cust_acct, otc_t_r_pim_cust_category, otc_t_r_ri_number_owner, otc_t_r_am_sim,otc_t_r_boe_sales_ord, otc_t_r_boe_ord_item, otc_t_nc_list_values, otc_t_vw_list_values):

    valor_str_retorno = func_proceso_principal(sqlContext, fecha_ejecucion, base_reportes, base_temporales, base_rdb_consultas, base_des_consultas, otc_t_r_om_portin_co,otc_t_r_om_portin_co_mpn_sr, otc_t_r_ri_mobile_phone_number, otc_t_r_usr_users, otc_t_r_pmgt_store,otc_t_r_cim_res_cust_acct, otc_t_r_cim_bsns_cust_acct, otc_t_r_pim_cust_category, otc_t_r_ri_number_owner, otc_t_r_am_sim,otc_t_r_boe_sales_ord, otc_t_r_boe_ord_item, otc_t_nc_list_values, otc_t_vw_list_values)
    return valor_str_retorno

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description=msg())
    parser.add_argument("-rps", help="@Reproceso", dest='reproceso', type=int)
    parser.add_argument("-fecha_ejecucion", help="@Fecha_Ejecucion", dest='fecha_ejecucion', type=str)
    parser.add_argument("-nombre_proceso_pyspark", help="@Nombre_Proceso", dest='nombre_proceso', type=str)
    
    # Obtenemos los parametros del shell    
    args = parser.parse_args()
    val_reproceso = args.reproceso
    val_fecha_ejecucion = args.fecha_ejecucion
    val_nombre_proceso = args.nombre_proceso
    
    configuracion = SparkConf().setAppName(val_nombre_proceso). \
        setAll(
        [('spark.speculation', 'false'), ('spark.master', 'yarn'), ('hive.exec.dynamic.partition.mode', 'nonstrict'),
        ('spark.yarn.queue', val_cola_ejecucion), ('hive.exec.dynamic.partition', 'true')])

    sc = SparkContext(conf=configuracion)
    sc.getConf().getAll()
    sc.setLogLevel("ERROR")
    sqlContext = HiveContext(sc)
    
    # Definimos las variables para la ejecucion
    val_error = 0
    val_inicio_ejecucion = time.time()
    val_formato_fecha = '%Y%m%d'

    try:
        val_fecha_proc = entrada(val_reproceso, val_formato_fecha, val_fecha_ejecucion)
        print(msg_succ("\n val_fecha_proc: %s \n" %(val_fecha_proc)))
        val_proceso = proceso(sqlContext, val_fecha_ejecucion, val_base_reportes, val_base_temporales, val_base_rdb_consultas, val_base_des_consultas, val_otc_t_r_om_portin_co,val_otc_t_r_om_portin_co_mpn_sr, val_otc_t_r_ri_mobile_phone_number, val_otc_t_r_usr_users, val_otc_t_r_pmgt_store,val_otc_t_r_cim_res_cust_acct, val_otc_t_r_cim_bsns_cust_acct, val_otc_t_r_pim_cust_category, val_otc_t_r_ri_number_owner, val_otc_t_r_am_sim,val_otc_t_r_boe_sales_ord, val_otc_t_r_boe_ord_item, val_otc_t_nc_list_values, val_otc_t_vw_list_values)
        print(msg_succ("Ejecucion Exitosa: \n %s " % val_proceso))
            
    except Exception as e:
        val_error = 2
        print(msg_error("Error PySpark: \n %s" % e))
    finally:
        sqlContext.clearCache()
        sc.stop()
        print("%s: Tiempo de ejecucion es: %s minutos " % (
        (time.strftime('%Y-%m-%d %H:%M:%S')), str(round(((time.time() - val_inicio_ejecucion) / 60), 2))))
        exit(val_error)
