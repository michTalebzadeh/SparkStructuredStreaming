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
function create_build_sbt_file {
        BUILD_SBT_FILE=${GEN_APPSDIR}/scala/${APPLICATION}/build.sbt
        [ -f ${BUILD_SBT_FILE} ] && rm -f ${BUILD_SBT_FILE}
        cat >> $BUILD_SBT_FILE << !
lazy val root = (project in file(".")).
  settings(
    name := "${APPLICATION}",
    version := "1.0",
    scalaVersion := "2.10.4",
    mainClass in Compile := Some("myPackage.${APPLICATION}")        
  )

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}
!
}
#
function create_local_build_sbt_file {
        BUILD_SBT_FILE=${GEN_APPSDIR}/scala/${APPLICATION}/build.sbt
        [ -f ${BUILD_SBT_FILE} ] && rm -f ${BUILD_SBT_FILE}
        cat >> $BUILD_SBT_FILE << !
    organization := "org.local"
    name := "${APPLICATION}",
    version := "1.0.0",
    scalaVersion := "2.10.4",
    packageBin in Compile := file(s"${name.value}_${scalaBinaryVersion.value}.jar")
!
}
#
function create_assembly_sbt_file {
        ASSEMBLY_SBT_FILE=${GEN_APPSDIR}/scala/${APPLICATION}/project/assembly.sbt
        [ -f ${ASSEMBLY_SBT_FILE} ] && rm -f ${ASSEMBLY_SBT_FILE}
        cat >> $ASSEMBLY_SBT_FILE << !
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")
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
        create_build_sbt_file
	create_assembly_sbt_file
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
print `date` ", Finished $0" | tee -a ${LOG_FILE}
#
## Do the cleanup
#
##clean_up
#
exit
