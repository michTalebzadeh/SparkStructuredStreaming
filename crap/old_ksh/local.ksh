#!/bin/ksh
#--------------------------------------------------------------------------------
#
# Procedure:    generic.ksh
#
# Description:  Compiles and run scala app using sbt or mvn and spark-submit
#
# Parameters:   A -> APPLICATION, T-> TYPE
#
##  Example:	generic.ksh -A ImportCSV -T sbt
##		generic.ksh -A ImportCSV -T mvn

#
#--------------------------------------------------------------------------------
# Vers|  Date  | Who | DA | Description
#-----+--------+-----+----+-----------------------------------------------------
# 1.0 |04/03/15|  MT |    | Initial Version
#--------------------------------------------------------------------------------
#
function F_USAGE
{
   echo "USAGE: ${1##*/} -A '<Application>'"
   echo "USAGE: ${1##*/} -T '<Type>'"
   echo "USAGE: ${1##*/} -P '<SP>'"
   echo "USAGE: ${1##*/} -H '<HELP>' -h '<HELP>'"
   exit 10
}
#
# Main Section
#
if [[ "${1}" = "-h" || "${1}" = "-H" ]]; then
   F_USAGE $0
fi
## MAP INPUT TO VARIABLES
while getopts A:T:P: opt
do
   case $opt in
   (A) APPLICATION="$OPTARG" ;;
   (T) TYPE="$OPTARG" ;;
   (P) SP="$OPTARG" ;;
   (*) F_USAGE $0 ;;
   esac
done

[[ -z ${APPLICATION} ]] && print "You must specify an application value " && F_USAGE $0
[[ -z ${TYPE} ]] && print "You must specify build type sbt, mvn or assembly " && F_USAGE $0

TYPE=`echo ${TYPE}|tr "[:upper:]" "[:lower:]"`
if [[ "${TYPE}" != "sbt" ]] && [[ "${TYPE}" != "mvn" ]] && [[ "${TYPE}" != "assembly" ]]
then
        print "Incorrect value for build type. The build type can only be mvn, sbt or assembly"  && F_USAGE $0
fi
#
if [[ -z ${SP} ]]
then
        SP=55555
fi


ENVFILE=/home/hduser/dba/bin/environment.ksh

if [[ -f $ENVFILE ]]
then
        . $ENVFILE
        . ~/spark-2.0.0-bin-hadoop2.6.kshrc
else
        echo "Abort: $0 failed. No environment file ( $ENVFILE ) found"
        exit 1
fi

#
. ./common_functions.ksh
#
##FILE_NAME=`basename $0 .ksh`
FILE_NAME=${APPLICATION}
CLASS=`echo ${FILE_NAME}|tr "[:upper:]" "[:lower:]"`
NOW="`date +%Y%m%d_%H%M`"
LOG_FILE=${LOGDIR}/${FILE_NAME}.log
[ -f ${LOG_FILE} ] && rm -f ${LOG_FILE}
#
## Build sbt, mvn or assembly file
#
if [[ "${TYPE}" = "sbt" ]]
then
	create_sbt_file
        JAR_FILE="target/scala-2.10/scala_2.10-1.0.jar"
	[ -f ${JAR_FILE} ] && rm -f ${JAR_FILE}
elif [[ "${TYPE}" = "assembly" ]]
then
	create_assembly_sbt_file
        create_build_sbt_file
        JAR_FILE="target/scala-2.10/scala-assembly-1.0.jar"
	[ -f ${JAR_FILE} ] && rm -f ${JAR_FILE}
else
	create_mvn_file
	JAR_FILE="target/scala-1.0.jar"
	[ -f ${JAR_FILE} ] && rm -f ${JAR_FILE}
fi
#
print "\n" `date` ", Started $0 building package with $TYPE" | tee -a ${LOG_FILE}
cd ../${FILE_NAME}
print "Compiling ${FILE_NAME}" | tee -a ${LOG_FILE}
#
if [[ "${TYPE}" = "sbt" ]]
then
  	${TYPE} package
elif [[ "${TYPE}" = "assembly" ]]
then
  	sbt ${TYPE} 
else
  	${TYPE} package
fi
#
print "Submiting the job" | tee -a ${LOG_FILE}
##
## you get a spark executor per yarn container. the spark executor can have multiple cores, yes. this is configurable.
## so the number of partitions that can be processed in parallel is num-executors * executor-cores.
## and for processing a partition the available memory is executor-memory / executor-cores (roughly, cores can of course borrow memory from each other within executor).
##
## The relevant setting for spark-submit are:
## --executor-memory
## --executor-cores
## --num-executors
##
##${SPARK_HOME}/bin/spark-submit \
##                --packages com.databricks:spark-csv_2.11:1.3.0 \
##                --jars /home/hduser/jars/spark-streaming-kafka-assembly_2.10-1.6.1.jar \
##                --class "${FILE_NAME}" \
##                --executor-cores=2 \
##                --master local[2] \
##                --master spark://50.140.197.217:7077 \
##                --executor-memory=4G \
##	          --executor-cores=2 \
##                --num-executors=1 \
##                ${JAR_FILE}

${SPARK_HOME}/bin/spark-submit \
                --packages com.databricks:spark-csv_2.11:1.3.0 \
                --driver-memory 8G \
                --num-executors 1 \
                --executor-memory 8G \
                --master local[12] \
                --conf "spark.scheduler.mode=FAIR" \
                --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
                --jars /home/hduser/jars/spark-streaming-kafka-assembly_2.10-1.6.1.jar \
	        --class "${FILE_NAME}" \
                --conf "spark.ui.port=${SP}" \
                --conf "spark.driver.port=54631" \
                --conf "spark.fileserver.port=54731" \
                --conf "spark.blockManager.port=54832" \
                --conf "spark.kryoserializer.buffer.max=512" \
                ${JAR_FILE} 
                ##${JAR_FILE} >> ${LOG_FILE}
print `date` ", Finished $0" | tee -a ${LOG_FILE}
#
## Do the cleanup
#
clean_up
#
exit
