#!/bin/bash
#################################################
## Run Spark Job                               ##
#################################################


FIRST_LETTER=$(echo $0|cut -c1-1)
if [ $FIRST_LETTER = "/" ] ; then
    APPDIR=$(dirname $0)
else
    APPDIR=$(pwd)/$(dirname $0)
fi

export SPARK_HOME=/usr/local/spark

APP_LIB_DIR=${APPDIR}/lib
APP_CONF_DIR=${APPDIR}/
EXTRA_JARS=`echo ${APP_LIB_DIR}/*.jar|tr ' ' ','`
APP_JAR=/Users/mac/Downloads/erkernel/target/er-kernel-1.0-SNAPSHOT-jar-with-dependencies.jar

APP_MAIN_CLASS=org.wumiguo.erkernel.ErKernelRunner
LOGFILE=./apptesting.log

cluster=local[1]
SPARK_CMD="spark-submit --jars ${EXTRA_JARS} --name ERKernelJob --master ${cluster} \
  --files ${APP_CONF_DIR}/log4j.properties#log4j \
  --num-executors 2 --executor-cores 2 --executor-memory 1G \
  --class ${APP_MAIN_CLASS} ${APP_JAR} "

echo "will execute cmd: $SPARK_CMD "
eval $SPARK_CMD > $LOGFILE 2>&1 &
if [[ "$?" -eq "0" ]] ; then
    echo "Spark job completed"
    exit 0
else
    echo "Spark job failed!!!"
    exit 1
fi
