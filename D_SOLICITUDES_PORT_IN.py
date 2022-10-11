import datetime
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
import argparse

timestart = datetime.datetime.now()

parser = argparse.ArgumentParser()
parser.add_argument("-nombre_proceso_pyspark", help="@NombreProcesoPyspark", dest='nombre_proceso', type=int)
parser.add_argument("-fecha_ejecucion", help="@Fecha_Ejecucion", dest='fecha_ejecucion', type=str)

# Obtenemos los parametros del shell    
args = parser.parse_args()
vApp=args.nombre_proceso
val_fecha_eje=args.fecha_ejecucion

val_nombre_proceso='solicitudes portabilidad in'
val_cola_ejecucion = 'default'
configuracion = SparkConf().setAppName(val_nombre_proceso). \
        setAll(
        [('spark.speculation', 'false'), ('spark.master', 'yarn'), ('hive.exec.dynamic.partition.mode', 'nonstrict'),
        ('spark.yarn.queue', val_cola_ejecucion), ('hive.exec.dynamic.partition', 'true')])
        
sc = SparkContext(conf=configuracion)
    sc.getConf().getAll()
    sc.setLogLevel("ERROR")
    sqlContext = HiveContext(sc)
    
    
spark = SparkSession\
    .builder\
    .appName(vApp)\
    .master("yarn")\
    .enableHiveSupport()\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


# la siguiente sentencia evita una error de spark 2.2... para algunas tablas ORC 
spark.sql("set spark.sql.hive.convertMetastoreOrc=true")

# ESTA TABLA NO ESTA EN HIVE
qry_a = """
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
FROM db_desarrollo2021.otc_t_r_om_portin_co
WHERE pt_fecha = 20220831
"""
# ESTA TABLA NO ESTA EN HIVE
qry_mpn ="""
SELECT 
object_id
, value
FROM db_desarrollo2021.otc_t_r_om_portin_co_mpn_sr
"""
# TABLA EXISTENTE EN HIVE (name as telefono)
qry_ri ="""
SELECT 
name
, object_id
, assoc_sim_iccid
FROM db_rdb.otc_t_r_ri_mobile_phone_number
"""
# TABLA EXISTENTE EN HIVE
qry_u = """
SELECT 
name
, object_id
, current_location
FROM db_rdb.otc_t_r_usr_users
"""

# TABLA EXISTENTE EN HIVE
qry_t = """
SELECT object_id
FROM db_rdb.otc_t_r_pmgt_store
"""
# TABLA EXISTENTE EN HIVE
qry_cr = """
SELECT 
cust_acct_number
, doc_number
, doc_type
, object_id
, cust_category
FROM db_rdb.otc_t_r_cim_res_cust_acct
"""
# TABLA EXISTENTE EN HIVE
qry_cb = """
SELECT 
cust_acct_number
, doc_number
, doc_type
, object_id
, cust_category
FROM db_rdb.otc_t_r_cim_bsns_cust_acct
"""

# TABLA EXISTENTE EN HIVE
qry_ctg = """
SELECT 
name
, object_id
FROM db_rdb.otc_t_r_pim_cust_category
"""

# TABLA EXISTENTE EN HIVE, particionada, se consulta toda
qry_donor = """
SELECT 
name
, object_id
FROM db_rdb.otc_t_r_ri_number_owner
"""

# TABLA EXISTENTE EN HIVE
qry_sim = """
SELECT 
name 
, object_id
FROM  db_rdb.otc_t_r_am_sim
"""


# TABLA EXISTENTE EN HIVE, particionada, se consulta por particion "pt_created_when"
# parametrizar esta tabla pt_created_when={fecha_eje}
qry_o = """
SELECT 
object_id
, processed_when
, sales_ord_status
, submitted_by
FROM db_rdb.otc_t_r_boe_sales_ord
WHERE pt_created_when=20220831
"""

# TABLA EXISTENTE EN HIVE
qry_oi = """
SELECT 
tariff_plan_name
, parent_id
, phone_number
FROM db_rdb.otc_t_r_boe_ord_item
WHERE pt_created_when=20220831
"""

# TABLA EXISTENTE EN HIVE

qry_nclv= """
SELECT 
value
, list_value_id
FROM db_rdb.otc_t_nc_list_values
"""

# TABLA EXISTENTE EN HIVE
## PARTICIONADA PERO SE CONSULTA COMPLETA
qry_vwlv= """
SELECT 
list_value_id 
, localized_value
FROM db_rdb.otc_t_vw_list_values
"""


df_a = spark.sql(qry_a)
df_mpn = spark.sql(qry_mpn)
df_ri = spark.sql(qry_ri)
df_u = spark.sql(qry_u)
df_t = spark.sql(qry_t)
df_cr = spark.sql(qry_cr)
df_cb = spark.sql(qry_cb)
df_ctg = spark.sql(qry_ctg)
df_donor = spark.sql(qry_donor)
df_sim = spark.sql(qry_sim)
# df_err = spark.sql(qry_err)
df_o = spark.sql(qry_o)
df_oi = spark.sql(qry_oi)
df_nclv = spark.sql(qry_nclv)
df_vwlv = spark.sql(qry_vwlv)

#df_a.printSchema()
#df_mpn.printSchema()
#df_ri.printSchema()
#df_u.printSchema()
#df_t.printSchema()
#df_cr.printSchema()
#df_cb.printSchema()
#df_ctg.printSchema()
#df_donor.printSchema()
#df_sim.printSchema()
#df_err.printSchema()
#df_o.printSchema()
#df_oi.printSchema()
#df_nclv.printSchema()
#df_vwlv.printSchema()

#df_a.show(5)
#df_mpn.show(5)
#df_ri.show(5)
#df_u.show(5)
#df_t.show(5)
#df_cr.show(5)
#df_cb.show(5)
#df_ctg.show(5)
#df_donor.show(5)
#df_sim.show(5)
#df_err.show(5)
#df_o.show(5)
#df_oi.show(5)
#df_nclv.show(5)
#df_vwlv.show(5)



t_a = df_a.alias('a')
t_mpn = df_mpn.alias('mpn')
t_ri = df_ri.alias('ri')
t_u = df_u.alias('u')
t_t = df_t.alias('t')
t_cr = df_cr.alias('cr')
t_cb = df_cb.alias('cb')
t_ctg = df_ctg.alias('ctg')
t_donor = df_donor.alias('donor')
t_sim = df_sim.alias('sim')
#t_err = df_err.alias('err')
t_o = df_o.alias('o')
t_oi = df_oi.alias('oi')
t_nclv = df_nclv.alias('nclv')
t_vwlv = df_vwlv.alias('vwlv')

#JOIN a o
df1 = t_a.join(t_o, expr("o.object_id = a.sales_order"), 'inner').selectExpr('a.object_id as portincommonorderid','a.donor_account_type as donor_account_type1','a.request_status','a.ascp_response','a.fvc','a.donor_account_type','a.created_when as created_when','a.ascp_rejection_comment as motivo_rechazo','o.object_id as salesorderid','o.sales_ord_status','o.processed_when as salesorderprocesseddate','a.assigned_csr','a.customer_account','a.donor_operator','a.sales_order')
df1.cache()

# df1.printSchema()
t_1 = df1.alias('t_1')

# LEFT JOIN 
df2 = t_1.join(t_mpn, expr("mpn.object_id = t_1.portincommonorderid"), how='left').selectExpr('mpn.value as mpn_value','t_1.*')
#df2.printSchema()
df2.cache()
t_2 = df2.alias('t_2')

# LEFT JOIN
df3 = t_2.join(t_ri, expr("t_2.mpn_value = ri.object_id"), how='left').selectExpr('ri.name as telefono','ri.assoc_sim_iccid','ri.object_id as ri_object_id','t_2.*')
df3.cache()
#df3.printSchema()
t_3 = df3.alias('t_3')
# df3.show()

# LEFT JOIN
df4 = t_3.join(t_u, expr("t_3.assigned_csr = u.object_id"), how='left').selectExpr('u.name as assignedcsr','u.current_location','t_3.*')
df4.cache()
#df4.printSchema()
t_4 = df4.alias('t_4')

#df5 = t_4.join(t_t, expr("t.object_id = t_4.current_location"), how='left').selectExpr(,,,,,,,)

# LEFT JOIN
df5 = t_4.join(t_cr, expr("t_4.customer_account = cr.object_id"), how='left').selectExpr('cr.cust_acct_number as cr_cust_acct_number','cr.doc_number as cr_doc_number','cr.doc_type as cr_doc_type','cr.cust_category as cr_cust_category','t_4.*')
df5.cache()
#df5.printSchema()
t_5 = df5.alias('t_5')

# LEFT JOIN
df6 = t_5.join(t_cb, expr("t_5.customer_account = cb.object_id"), how='left').selectExpr('cb.cust_acct_number as cb_cust_acct_number','cb.doc_number as cb_doc_number','cb.doc_type as cb_doc_type','cb.cust_category as cb_cust_category','t_5.*')
df6.cache()
#df6.printSchema()
t_6 = df6.alias('t_6')

# LEFT JOIN
df7 = t_6.join(t_ctg, expr("ctg.object_id = nvl(t_6.cb_cust_category, t_6.cr_cust_category)"), how='left').selectExpr('case	when t_6.cr_cust_acct_number is null then t_6.cb_cust_acct_number else t_6.cr_cust_acct_number end as customeraccountnumber','case when t_6.cr_doc_number is null then t_6.cb_doc_number else t_6.cr_doc_number	end as doc_number','ctg.name as customercategory','t_6.*')
df7.cache()
#df7.printSchema()
t_7 = df7.alias('t_7')

# LEFT JOIN
df8 = t_7.join(t_donor, expr("donor.object_id = t_7.donor_operator"), how='left').selectExpr('donor.name as operadora','t_7.*')
df8.cache()
#df8.printSchema()
t_8 = df8.alias('t_8')

# LEFT JOIN
df9 = t_8.join(t_sim, expr("sim.object_id = t_8.assoc_sim_iccid"), how='left').selectExpr('substr(sim.name, 1, 19) as associatedsimiccid','t_8.*')
df9.cache()
#df9.printSchema()
t_9 = df9.alias('t_9')

# LEFT JOIN
#df10 = t_9.join(t_err, expr("err.failed_order = t_9.portincommonorderid"), how='left').selectExpr('t_9.*')
#t_10 = df10.alias('t_10')

# LEFT JOIN
df10 = t_9.join(t_oi, expr("oi.parent_id = t_9.sales_order and oi.phone_number = t_9.ri_object_id"), how='left').selectExpr('oi.tariff_plan_name as plandestino','t_9.*')
df10.cache()
t_10 = df10.alias('t_10')

# LEFT JOIN
df11 = t_10.join(t_nclv, expr("nclv.list_value_id = t_10.request_status"), how='left').selectExpr('nclv.value as requeststatus','t_10.*')
df11.cache()
t_11 = df11.alias('t_11')

# LEFT JOIN
df12 = t_11.join(t_vwlv, expr("vwlv.list_value_id = t_11.ascp_response"), how='left').selectExpr('vwlv.localized_value as estado','t_11.*')
df12.cache()
#df12.printSchema()
t_12 = df12.alias('t_12')

# LEFT JOIN
df13 = t_12.join(t_nclv, expr("nclv.list_value_id = t_12.donor_account_type"), how='left').selectExpr('nclv.value as ln_origen','t_12.*')
df13.cache()
#df13.printSchema()
t_13 = df13.alias('t_13')

# LEFT JOIN
df14 = t_13.join(t_nclv, expr("nclv.list_value_id = t_13.sales_ord_status"), how='left').selectExpr('nclv.value as salesorderstatus','t_13.*')
df14.cache()
#df14.printSchema()
t_14 = df14.alias('t_14')

# LEFT JOIN
df15 = t_14.join(t_nclv, expr("nclv.list_value_id = nvl(t_14.cb_doc_type, t_14.cr_doc_type)"), how='left').selectExpr('nclv.value as doc_type','t_14.*')
df15.cache()
t_15 = df15.alias('t_15')
#df15.printSchema()

df16 = df15.selectExpr('customeraccountnumber','doc_number','doc_type','customercategory','portincommonorderid','requeststatus','estado','fvc','operadora','donor_account_type1','ln_origen','assignedcsr','created_when','salesorderid','salesorderstatus','salesorderprocesseddate','telefono','associatedsimiccid','plandestino','motivo_rechazo')
df16.printSchema()
df16.cache()

df16.repartition(10).write.format('hive').format("parquet").mode("overwrite").saveAsTable('db_desarrollo2021.solicitudes_aux')

spark.stop()
timeend = datetime.datetime.now()
duracion = timeend - timestart
print("Duracion {}".format(duracion))

# /usr/hdp/current/spark2-client/bin/spark-submit --master yarn --executor-memory 2G --num-executors 10 --executor-cores 2 --driver-memory 2G /home/nae108834/Cliente360_RF/D_SOLICITUDES_PORT_IN.py

#/usr/hdp/current/spark2-client/bin/pyspark --master yarn --executor-memory 16G --num-executors 10 --executor-cores 2 --driver-memory 2G