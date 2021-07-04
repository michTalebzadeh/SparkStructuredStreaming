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
   echo "USAGE: ${1##*/} -M '<Mode>'"
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
while getopts M:A:T:P: opt
do
   case $opt in
   (M) MODE="$OPTARG" ;;
   (A) APPLICATION="$OPTARG" ;;
   (T) TYPE="$OPTARG" ;;
   (P) SP="$OPTARG" ;;
   (*) F_USAGE $0 ;;
   esac
done

[[ -z ${MODE} ]] && print "You must specify a run mode: Local, Standalone or Yarn " && F_USAGE $0
MODE=`echo ${MODE}|tr "[:upper:]" "[:lower:]"`
if [[ "${MODE}" != "local" ]] && [[ "${MODE}" != "standalone" ]] && [[ "${MODE}" != "yarn" ]]
then
        print "Incorrect value for build mode. The run mode can only be local, standalone or yarn"  && F_USAGE $0
fi
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
        export SP=55555
fi

ENVFILE=/home/hduser/dba/bin/environment.ksh

if [[ -f $ENVFILE ]]
then
        . $ENVFILE
	. /home/hduser/spark-2.4.3-bin-hadoop3.0.ksh
else
        echo "Abort: $0 failed. No environment file ( $ENVFILE ) found"
        exit 1
fi
#
. /home/hduser/dba/bin/scala/bin/common_functions.ksh
#
##FILE_NAME=`basename $0 .ksh`
FILE_NAME=${APPLICATION}
CLASS=`echo ${FILE_NAME}|tr "[:upper:]" "[:lower:]"`
NOW="`date +%Y%m%d_%H%M`"
LOG_FILE=${LOGDIR}/${FILE_NAME}.log
[ -f ${LOG_FILE} ] && rm -f ${LOG_FILE}

print `date` ", Calling ./compile.ksh to compile the code" | tee -a ${LOG_FILE}

./compile.ksh -A ${APPLICATION} -T ${TYPE} -P ${SP}

#
default_settings

print `date` ", Running in **${MODE} mode**" | tee -a ${LOG_FILE}

cd ../${FILE_NAME}

#
## specify where jar files are created by ./compile.ksh 

if [[ "${TYPE}" = "sbt" ]]
then
        JAR_FILE="target/scala-2.10/scala_2.10-1.0.jar"
elif [[ "${TYPE}" = "assembly" ]]
then
        JAR_FILE="target/scala-2.11/${APPLICATION}-assembly-1.0.jar"
else
        JAR_FILE="target/scala-1.0.jar"
fi
#

if [[ "${MODE}" = "local" ]]
then
	run_local
elif [[ "${MODE}" = "standalone" ]]
then
	run_standalone
else
	run_yarn
fi

print `date` ", Finished $0" | tee -a ${LOG_FILE}
#
## Do the cleanup
#
##clean_up
#
exit
