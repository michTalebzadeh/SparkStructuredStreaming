#!/bin/ksh
#--------------------------------------------------------------------------------
#
# Procedure:    start hadoop daemons.ksh
#
# Description:  starts all hadoop nodes
#
# Parameters:   none
#
#--------------------------------------------------------------------------------
# Vers|  Date  | Who | DA | Description
#-----+--------+-----+----+-----------------------------------------------------
# 1.0 |04/03/16|  MT |    | Initial Version
#--------------------------------------------------------------------------------
#
ENVFILE=/home/hduser/dba/bin/environment.ksh

if [[ -f $ENVFILE ]]
then
        . $ENVFILE
        . ~/spark_1.5.2_bin-hadoop2.6.kshrc
else
        echo "Abort: $0 failed. No environment file ( $ENVFILE ) found"
        exit 1
fi

FILE_NAME=`basename $0 .ksh`
CLASS=`echo ${FILE_NAME}|tr "[:upper:]" "[:lower:]"`
NOW="`date +%Y%m%d_%H%M`"
LOG_FILE=${LOGDIR}/${FILE_NAME}.log
[ -f ${LOG_FILE} ] && rm -f ${LOG_FILE}
print "\n" `date` ", Started $0" | tee -a ${LOG_FILE}
cd ../${FILE_NAME}
print "Compiling ${FILE_NAME}" | tee -a ${LOG_FILE}
sbt package
print "Submiiting the job" | tee -a ${LOG_FILE}
$SPARK_HOME/bin/spark-submit \
	        --class "${FILE_NAME}" \
                 --master spark://50.140.197.217:7077 \
                   target/scala-2.10/${CLASS}_2.10-1.0.jar
print `date` ", Finished $0" | tee -a ${LOG_FILE}
exit
