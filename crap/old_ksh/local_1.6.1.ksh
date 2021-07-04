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
function create_sbt_file {
	SBT_FILE=${GEN_APPSDIR}/scala/${APPLICATION}/${FILE_NAME}.sbt
	[ -f ${SBT_FILE} ] && rm -f ${SBT_FILE}
        cat >> $SBT_FILE << !
name := "scala"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.6.1"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"
libraryDependencies += "com.google.code.gson" % "gson" % "2.6.2"
!
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
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("myPackage.${APPLICATION}")        
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.6.1"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"
libraryDependencies += "com.google.code.gson" % "gson" % "2.6.2"

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
function create_assembly_sbt_file {
        ASSEMBLY_SBT_FILE=${GEN_APPSDIR}/scala/${APPLICATION}/project/assembly.sbt
        [ -f ${ASSEMBLY_SBT_FILE} ] && rm -f ${ASSEMBLY_SBT_FILE}
        cat >> $ASSEMBLY_SBT_FILE << !
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")
!
}
#
function create_mvn_file {
        MVN_FILE=${GEN_APPSDIR}/scala/${APPLICATION}/pom.xml
        [ -f ${MVN_FILE} ] && rm -f ${MVN_FILE}
        cat >> $MVN_FILE << !
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
<modelVersion>4.0.0</modelVersion>
<groupId>spark</groupId>
<version>1.0</version>
<name>\${project.artifactId}</name>

<properties>
<maven.compiler.source>1.7</maven.compiler.source>
<maven.compiler.target>1.7</maven.compiler.target>
<encoding>UTF-8</encoding>
<scala.version>2.10.4</scala.version>
<maven-scala-plugin.version>2.15.2</maven-scala-plugin.version>
</properties>

<dependencies>
  <dependency>
    <groupId>org.scala-lang</groupId>
    <artifactId>scala-library</artifactId>
    <version>2.11.8</version>
  </dependency>
<dependency>
<groupId>org.apache.spark</groupId>
<artifactId>spark-core_2.10</artifactId>
<version>1.5.1</version>
</dependency>
<dependency>
<groupId>org.apache.spark</groupId>
<artifactId>spark-sql_2.10</artifactId>
<version>1.5.1</version>
</dependency>
<dependency>
<groupId>org.apache.spark</groupId>
<artifactId>spark-hive_2.10</artifactId>
<version>1.5.0</version>
</dependency>
<dependency>
<groupId>com.databricks</groupId>
<artifactId>spark-csv_2.11</artifactId>
<version>1.3.0</version>
</dependency>
<dependency>
<groupId>org.apache.spark</groupId>
<artifactId>spark-streaming-kafka_2.10</artifactId>
<version>1.5.1</version>
</dependency>
<dependency>
<groupId>org.apache.spark</groupId>
<artifactId>spark-streaming-twitter_2.10</artifactId>
<version>1.6.1</version>
</dependency>
<dependency>
<groupId>org.twitter4j</groupId>
<artifactId>twitter4j-core</artifactId>
<version>2.1.4</version>
</dependency>
<dependency>
    <groupId>com.google.code.gson</groupId>
    <artifactId>gson</artifactId>
    <version>2.6.2</version>
</dependency>
</dependencies>

<build>
<sourceDirectory>src/main/scala</sourceDirectory>
<plugins>
<plugin>
<groupId>org.scala-tools</groupId>
<artifactId>maven-scala-plugin</artifactId>
<version>\${maven-scala-plugin.version}</version>
<executions>
<execution>
<goals>
<goal>compile</goal>
</goals>
</execution>
</executions>
<configuration>
<jvmArgs>
<jvmArg>-Xms64m</jvmArg>
<jvmArg>-Xmx1024m</jvmArg>
</jvmArgs>
</configuration>
</plugin>
<plugin>
<groupId>org.apache.maven.plugins</groupId>
<artifactId>maven-shade-plugin</artifactId>
<version>1.6</version>
<executions>
<execution>
<phase>package</phase>
<goals>
<goal>shade</goal>
</goals>
<configuration>
<filters>
<filter>
<artifact>*:*</artifact>
<excludes>
<exclude>META-INF/*.SF</exclude>
<exclude>META-INF/*.DSA</exclude>
<exclude>META-INF/*.RSA</exclude>
</excludes>
</filter>
</filters>
<transformers>
<transformer
implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
<mainClass>com.group.id.Launcher1</mainClass>
</transformer>
</transformers>
</configuration>
</execution>
</executions>
</plugin>
</plugins>
</build>

<artifactId>scala</artifactId>
</project>
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
