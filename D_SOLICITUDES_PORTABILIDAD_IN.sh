#!/bin/bash
############################################################################
#   Script de carga de datos desde ORACLE para solicitudes_de_portabilidad #
#   hacia HIVE como parte  del requerimiento  EXTRACTOR DE MOVIMIENTOS     #
#--------------------------------------------------------------------------#
#--------------------------------------------------------------------------#
# REFACTORING: CRISTIAN ORTIZ											   #
# MODIFICADO : 27/AGO/2022									    		   #
# COMENTARIO:                                                              #
# "EXTRACTOR DE MOVIMIENTOS" 											   #
############################################################################
ENTIDAD=D_SOLICITUDES_PORT_IN
FECHAEJE=$1
REPROCESO=$2
VAL_RUTA_PROCESO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA_PROCESO';"`
VAL_RUTA_APLICACION=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA_APLICACION';"`
VAL_NOMBRE_PROCESO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOMBRE_PROCESO';"`
VAL_NOMBRE_PROCESO_HIVE_QUERY='D_SOLICITUDES_PORT_IN'

# Seteamos la variable de error en 0, variable que se retorna a control M
error=0

# Validacion de parametros iniciales, nulos y existencia de Rutas
  if [ -z "$ENTIDAD" ] || [ -z "$FECHAEJE" ] || [ -z "$VAL_RUTA_PROCESO" ] || [ -z "$VAL_NOMBRE_PROCESO" ] || [ -z "$VAL_RUTA_APLICACION" ] || [ -z "$REPROCESO" ] || [ -z "$VAL_NOMBRE_PROCESO_HIVE_QUERY" ] ; then
    echo " $TIME [ERROR] $rc unos de los parametros esta vacio o nulo"
    error=3
    exit $error
  fi

  
  # Verificar si existe la ruta del programa Existe, si no generamos el error
  if ! [ -e "$VAL_RUTA_PROCESO" ]; then
    echo "$TIME [ERROR] $rc la ruta de la aplicacion no existe o no se tiene permisos"
    error=3
    exit $error
  fi

#################################################
# Generamos las variables para el nombre del log
#################################################
VAL_HORA=`date '+%Y%m%d%H%M%S'`
VAL_FECHA_LOG=`date '+%Y%m%d%H%M%S'`
VAL_RUTA_LOG=$VAL_RUTA_PROCESO/Logs
VAL_NOMBRE_LOG=$VAL_NOMBRE_PROCESO"_"$VAL_FECHA_LOG.log
VAL_LOG_EJECUCION_PRINCIPAL=$VAL_RUTA_LOG/"LogPrincipal_"$VAL_NOMBRE_LOG
VAL_LOG_EJECUCION_PYTHON=$VAL_RUTA_LOG/$VAL_NOMBRE_LOG


echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
echo " ====================== ... INICIA PROCESO '$ENTIDAD' ... ====================== "`date '+%Y%m%d%H%M%S'` >> $VAL_LOG_EJECUCION_PRINCIPAL
echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
echo "SHELL Variable => FECHAEJE: $FECHAEJE" >> $VAL_LOG_EJECUCION_PRINCIPAL


#################################################
# Ejecucion Proceso SPARK
#################################################

echo " =================================================================================== " >> $VAL_LOG_EJECUCION_PYTHON
echo " ================ ... Se ejecuta Sub-Proceso PYSPARK DE '$ENTIDAD' ... ================ "`date '+%Y%m%d%H%M%S'` >> $VAL_LOG_EJECUCION_PYTHON
echo " =================================================================================== " >> $VAL_LOG_EJECUCION_PYTHON

echo "==================================================================================================================================="


$VAL_RUTA_APLICACION --master yarn --executor-memory 16G --num-executors 10 --executor-cores 2 --driver-memory 2G  $VAL_RUTA_PROCESO/$VAL_NOMBRE_PROCESO.py \
-fecha_ejecucion $FECHAEJE -rps $REPROCESO -nombre_proceso_pyspark $VAL_NOMBRE_PROCESO_HIVE_QUERY &> $VAL_LOG_EJECUCION_PYTHON

# Validamos el LOG de la ejecucion de Python, si encontramos errores finalizamos con error >0
  VAL_ERRORES=`egrep 'FAILED:|Error|Table not found|Table already exists|Vertex' $VAL_LOG_EJECUCION_PYTHON | wc -l`
  if [ $VAL_ERRORES -ne 0 ];then
	error=4
    echo "=== Error en la ejecucion del Sub-Proceso PYSPARK '$ENTIDAD' " >> $VAL_LOG_EJECUCION_PYTHON
  else
	error=0
	echo " === ... FIN Sub-Proceso PYSPARK '$ENTIDAD'  ... === "`date '+%Y%m%d%H%M%S'` >> $VAL_LOG_EJECUCION_PYTHON		
  fi

  cat $VAL_LOG_EJECUCION_PYTHON >> $VAL_LOG_EJECUCION_PRINCIPAL
  
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " ==================== ... FINALIZA PROCESO '$ENTIDAD' ... ====================== "`date '+%Y%m%d%H%M%S'` >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL

exit $error
# sh -x /home/nae108834/D_SOLICITUDES_PORT_IN/Bin/D_SOLICITUDES_PORT_IN.sh 20220731