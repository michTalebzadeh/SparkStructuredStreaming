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
   echo "USAGE: ${1##*/} -H '<HELP>' -h '<HELP>'"
   exit 10
}
#
function create_sbt_file {
        SBT_FILE=${GEN_APPSDIR}/scala/${APPLICATION}/${FILE_NAME}.sbt
        [ -f ${SBT_FILE} ] && rm -f ${SBT_FILE}
        cat >> $SBT_FILE << !
name := "scala"

version := "1.0"

scalaVersion := "2.10.4"

!
}
#
function clean_up
{
if [[ "${TYPE}" = "sbt" ]]
then
	[ -f ${SBT_FILE} ] && rm -f ${SBT_FILE}
elif [[ "${TYPE}" = "assembly" ]]
then
	[ -f ${BUILD_SBT_FILE} ] && rm -f ${BUILD_SBT_FILE}
	[ -f ${ASSEMBLY_SBT_FILE} ] && rm -f ${ASSEMBLY_SBT_FILE}
else
	[ -f ${MVN_FILE} ] && rm -f ${MVN_FILE}
fi
}
#
# Main Section
#
if [[ "${1}" = "-h" || "${1}" = "-H" ]]; then
   F_USAGE $0
fi
## MAP INPUT TO VARIABLES
while getopts A: opt
do
   case $opt in
   (A) APPLICATION="$OPTARG" ;;
   (*) F_USAGE $0 ;;
   esac
done

[[ -z ${APPLICATION} ]] && print "You must specify an application value " && F_USAGE $0

TYPE="sbt"
if [[ "${TYPE}" != "sbt" ]] 
then
        print "Incorrect value for build type. The build type can only be sbt"  && F_USAGE $0
fi
#

ENVFILE=/home/hduser/dba/bin/environment.ksh

if [[ -f $ENVFILE ]]
then
        . $ENVFILE
        . ~/spark-1.6.1-bin-hadoop2.6.kshrc
else
        echo "Abort: $0 failed. No environment file ( $ENVFILE ) found"
        exit 1
fi

##FILE_NAME=`basename $0 .ksh`
FILE_NAME=${APPLICATION}
CLASS=`echo ${FILE_NAME}|tr "[:upper:]" "[:lower:]"`
NOW="`date +%Y%m%d_%H%M`"
LOG_FILE=${LOGDIR}/${FILE_NAME}.log
[ -f ${LOG_FILE} ] && rm -f ${LOG_FILE}

[ -d ${GEN_APPSDIR}/scala/${APPLICATION}/project ] && rm -rf ${GEN_APPSDIR}/scala/${APPLICATION}/project
[ -d ${GEN_APPSDIR}/scala/${APPLICATION}/target ] && rm -rf ${GEN_APPSDIR}/scala/${APPLICATION}/target

## Build sbt, mvn or assembly file
#
create_sbt_file
JAR_FILE="target/scala-2.10/scala_2.10-1.0.jar"
[ -f ${JAR_FILE} ] && rm -f ${JAR_FILE}
#
cd ../${FILE_NAME}

print "Compiling ${FILE_NAME}" | tee -a ${LOG_FILE}
sbt compile
#
print "Running ${FILE_NAME}" | tee -a ${LOG_FILE}
sbt run

print "\n" `date` ", Started building package with $TYPE" | tee -a ${LOG_FILE}
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
print "\n" `date` ", Executing using the main method from the jar file ${JAR_FILE}" | tee -a ${LOG_FILE}

scala \
               ${JAR_FILE}

print `date` ", Finished $0" | tee -a ${LOG_FILE}
#
## Do the cleanup
#
clean_up
#
exit
