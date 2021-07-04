#!/bin/bash/
NODE=`hostname`; NODE=$LOGNAME"@"$NODE; export NODE
##PS1="$NODE:""${DSQUERY}:"'$PWD'"# "
DSQUERY=": "
PS1="$NODE""${DSQUERY}"'$PWD'"> "
history=10000;export history
set filec
alias a=alias
alias h=history
alias alurt='ls -alurt'
alias ltr='ls -ltra'
alias nedit='~/mich/nedit/nedit'
##a rm="rm -i"
alias ls="ls --color=never"
alias mv="mv -i"
alias cp="cp -i"
alias jl='cd ~/dba/log'
alias jb='cd ~/dba/bin'
alias je='cd ~/dba/env'
alias jt='cd /apps/sybase/dba/bin/ASE15/201202/selectivity'
alias js='cd ~/dba/bin/scala'
alias jr='cd ~/dba/bin/scala/bin'
alias jf='cd ~/dba/bin/flink/bin'
##alias blh='rlwrap beeline -u jdbc:hive2://rhes75:10099/default org.apache.hive.jdbc.HiveDriver -n hduser -p hduser -i /home/hduser/dba/bin/hive_on_spark_init.hql'
alias blh='rlwrap beeline -u jdbc:hive2://rhes75:10099/default org.apache.hive.jdbc.HiveDriver -n hduser -p hduser  -i /home/hduser/dba/bin/add_jars.hql'
alias hbs='rlwrap hbase shell'
alias smc='sqoop job -meta-connect "jdbc:sybase:Tds:rhes75:5200/sqoopdb?user=sqoopuser&password=sqoopuser" '
alias zk='$ZOOKEEPER_HOME/bin/zkCli.sh -server rhes75:2181'
alias sp_sysmon='(echo sp_sysmon 2!*;echo go) | sql'
alias sp_who='(echo sp_who;echo go) | sql'
alias sp__who='(echo sp__who;echo go) | sql'
alias io='(echo sp__io;echo go) | sql'
alias cpu='(echo sp__cpu;echo go) | sql'
alias sp_lock='(echo sp_lock;echo go) | sql'
alias sp__lock='(echo sp__lock;echo go) | sql'
alias lk='sql -e "show locks"'
alias trn='sql -e "show transactions"'
alias l='(echo sp__long;echo go) | sql'
alias s='(echo sp__whoe;echo go) | sql'
alias tn='(echo sp__topn;echo go) | sql'
alias mpa='(echo exec sp__mpa;echo go;echo exec sp__topn;echo go;echo exec sp__objects;echo go) | sql'
alias c='(echo sp__cache;echo go) | sql'
alias t='(echo tempdb..sp__#tables;echo go) | sql'
alias obj='(echo sp__objects;echo go) | sql'
alias co='(echo sp__cached_objects;echo go) | sql'
alias mp='(echo sp__proc_stats;echo go) | sql'
alias sp__cpu='(echo sp__cpu;echo go) | sql'
alias sp__io='(echo sp__io;echo go) | sql'
alias snap='(echo sp_tsnap;echo go) | sql'
alias monspid="~/dba/bin/monspid"
alias topproc='(echo exec sp__topproc;echo go) | sql'
alias dump='cd /apps/sybase//dump'
alias jm='cd ~/mich'
alias jv='cd /var/tmp'
alias use='. ~/.profile'
alias mypc='DISPLAY=166.15.160.46:0.0'
alias stats='(cat /apps/sybase/dba/bin/mda_codes/ase_stats.sql) | sql'
alias cs='(cat /apps/sybase/dba/bin/mda_codes/cs.sql) | sql'
alias blk='(cat /apps/sybase/dba/bin/mda_codes/block.sql) | sql'
alias ap='(cat /apps/sybase/dba/bin/mda_codes/active_processes.sql) | sql'
alias tmp='(echo sp__seg_usage tempdb;echo go) | sql'
alias l5='ls -ltra|tail -5'
alias ys='yarn application -list -appStates all'
alias ks='JMX_PORT=10101  ${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties &'
alias jc='jconsole rhes75:10101 &'
export editor=vi
export PSQL_EDITOR="/Users/username/bin/mate"
export XKEYSYMDB=/usr/share/X11/XKeysymDB
set -o vi
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export JDK_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export PATH=$PATH:$JAVA_HOME/bin
alias syarn='echo UI is on 55555;rlwrap myspark-shell_yarn --driver-class-path /home/hduser/jars/ddhybrid.jar --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar'
alias slocal='echo UI is on 55555;rlwrap myspark-shell_local --driver-class-path /home/hduser/jars/ddhybrid.jar --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar'
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/GCPFirstProject-d75f1b3a9817.json"
alias ssbq='spark-shell --driver-class-path ~/jars/ddhybrid.jar --jars ~/jars/ddhybrid.jar'
alias bq='spark-shell --driver-class-path ~/jars/googlebigquery.jar --jars ~/jars/googlebigquery.jar'
CLASSPATH=/home/hduser/jars/ddhybrid.jar:/home/hduser/jars/spark-bigquery-with-dependencies_2.11-0.17.2.jar:/home/hduser/jars/spark-bigquery_2.11-0.2.7.jar
export JVM_ARGS="-Xmx1024m -XX:MaxPermSize=256m"
SIMBAJARS=""
for i in `ls ${HOME}/Simba/*.jar `
do
    SIMBAJARS=$SIMBAJARS,$i
done
alias gc='/usr/bin/google-chrome &'
alias sshell='rlwrap spark-shell --driver-class-path /home/hduser/jars/ddhybrid.jar --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar --master yarn --deploy-mode client'
alias yarns="rlwrap myspark-shell_yarn --driver-class-path /home/hduser/jars/ddhybrid.jar --jars /home/hduser/jars/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar --packages com.github.samelamin:spark-bigquery_2.11:0.2.6"
alias crap="rlwrap myspark-shell_yarn --driver-class-path /home/hduser/jars/ddhybrid.jar --jars /home/hduser/jars/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar,/home/hduser/jars/com.google.http-client_google-http-client-1.24.1.jar,/home/hduser/jars/com.google.http-client_google-http-client-jackson2-1.24.1.jar,/home/hduser/jars/com.google.cloud.bigdataoss_util-1.9.4.jar,/home/hduser/jars/com.google.api-client_google-api-client-1.24.1.jar,/home/hduser/jars/com.google.oauth-client_google-oauth-client-1.24.1.jar,/home/hduser/jars/com.google.apis_google-api-services-bigquery-v2-rev398-1.24.1.jar,/home/hduser/jars/com.google.cloud.bigdataoss_bigquery-connector-0.13.4-hadoop2.jar,/home/hduser/jars/spark-bigquery_2.11-0.2.6.jar"
alias yarnp='spark-submit --master yarn --deploy-mode client --driver-class-path /home/hduser/jars/ddhybrid.jar --jars /home/hduser/jars/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar --packages com.github.samelamin:spark-bigquery_2.11:0.2.6'
alias crapp="spark-submit --master yarn --deploy-mode client --driver-class-path /home/hduser/jars/ddhybrid.jar --jars /home/hduser/jars/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar,/home/hduser/jars/com.google.http-client_google-http-client-1.24.1.jar,/home/hduser/jars/com.google.http-client_google-http-client-jackson2-1.24.1.jar,/home/hduser/jars/com.google.cloud.bigdataoss_util-1.9.4.jar,/home/hduser/jars/com.google.api-client_google-api-client-1.24.1.jar,/home/hduser/jars/com.google.oauth-client_google-oauth-client-1.24.1.jar,/home/hduser/jars/com.google.apis_google-api-services-bigquery-v2-rev398-1.24.1.jar,/home/hduser/jars/com.google.cloud.bigdataoss_bigquery-connector-0.13.4-hadoop2.jar,/home/hduser/jars/spark-bigquery_2.11-0.2.6.jar"
export CONDA_HOME=/home/hduser/miniconda2
export PATH=$PATH:$CONDA_HOME/bin
cd /usr/src/Python-3.7.9/environments; source virtualenv/bin/activate;cd
(base) (vrtualenv) hduser@dpcluster-m: /home/hduser> scp bashrc rhes75:~/dba/bin/python
ssh: Could not resolve hostname rhes75: Name or service not known
lost connection
(base) (vrtualenv) hduser@dpcluster-m: /home/hduser> cat bashrc
#!/bin/bash/
NODE=`hostname`; NODE=$LOGNAME"@"$NODE; export NODE
##PS1="$NODE:""${DSQUERY}:"'$PWD'"# "
DSQUERY=": "
PS1="$NODE""${DSQUERY}"'$PWD'"> "
history=10000;export history
set filec
alias a=alias
alias h=history
alias alurt='ls -alurt'
alias ltr='ls -ltra'
alias nedit='~/mich/nedit/nedit'
##a rm="rm -i"
alias ls="ls --color=never"
alias mv="mv -i"
alias cp="cp -i"
alias jl='cd ~/dba/log'
alias jb='cd ~/dba/bin'
alias je='cd ~/dba/env'
alias jt='cd /apps/sybase/dba/bin/ASE15/201202/selectivity'
alias js='cd ~/dba/bin/scala'
alias jr='cd ~/dba/bin/scala/bin'
alias jf='cd ~/dba/bin/flink/bin'
##alias blh='rlwrap beeline -u jdbc:hive2://rhes75:10099/default org.apache.hive.jdbc.HiveDriver -n hduser -p hduser -i /home/hduser/dba/bin/hive_on_spark_init.hql'
alias blh='rlwrap beeline -u jdbc:hive2://rhes75:10099/default org.apache.hive.jdbc.HiveDriver -n hduser -p hduser  -i /home/hduser/dba/bin/add_jars.hql'
alias hbs='rlwrap hbase shell'
alias smc='sqoop job -meta-connect "jdbc:sybase:Tds:rhes75:5200/sqoopdb?user=sqoopuser&password=sqoopuser" '
alias zk='$ZOOKEEPER_HOME/bin/zkCli.sh -server rhes75:2181'
alias sp_sysmon='(echo sp_sysmon 2!*;echo go) | sql'
alias sp_who='(echo sp_who;echo go) | sql'
alias sp__who='(echo sp__who;echo go) | sql'
alias io='(echo sp__io;echo go) | sql'
alias cpu='(echo sp__cpu;echo go) | sql'
alias sp_lock='(echo sp_lock;echo go) | sql'
alias sp__lock='(echo sp__lock;echo go) | sql'
alias lk='sql -e "show locks"'
alias trn='sql -e "show transactions"'
alias l='(echo sp__long;echo go) | sql'
alias s='(echo sp__whoe;echo go) | sql'
alias tn='(echo sp__topn;echo go) | sql'
alias mpa='(echo exec sp__mpa;echo go;echo exec sp__topn;echo go;echo exec sp__objects;echo go) | sql'
alias c='(echo sp__cache;echo go) | sql'
alias t='(echo tempdb..sp__#tables;echo go) | sql'
alias obj='(echo sp__objects;echo go) | sql'
alias co='(echo sp__cached_objects;echo go) | sql'
alias mp='(echo sp__proc_stats;echo go) | sql'
alias sp__cpu='(echo sp__cpu;echo go) | sql'
alias sp__io='(echo sp__io;echo go) | sql'
alias snap='(echo sp_tsnap;echo go) | sql'
alias monspid="~/dba/bin/monspid"
alias topproc='(echo exec sp__topproc;echo go) | sql'
alias dump='cd /apps/sybase//dump'
alias jm='cd ~/mich'
alias jv='cd /var/tmp'
alias use='. ~/.profile'
alias mypc='DISPLAY=166.15.160.46:0.0'
alias stats='(cat /apps/sybase/dba/bin/mda_codes/ase_stats.sql) | sql'
alias cs='(cat /apps/sybase/dba/bin/mda_codes/cs.sql) | sql'
alias blk='(cat /apps/sybase/dba/bin/mda_codes/block.sql) | sql'
alias ap='(cat /apps/sybase/dba/bin/mda_codes/active_processes.sql) | sql'
alias tmp='(echo sp__seg_usage tempdb;echo go) | sql'
alias l5='ls -ltra|tail -5'
alias ys='yarn application -list -appStates all'
alias ks='JMX_PORT=10101  ${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties &'
alias jc='jconsole rhes75:10101 &'
export editor=vi
export PSQL_EDITOR="/Users/username/bin/mate"
export XKEYSYMDB=/usr/share/X11/XKeysymDB
set -o vi
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export JDK_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export PATH=$PATH:$JAVA_HOME/bin
alias syarn='echo UI is on 55555;rlwrap myspark-shell_yarn --driver-class-path /home/hduser/jars/ddhybrid.jar --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar'
alias slocal='echo UI is on 55555;rlwrap myspark-shell_local --driver-class-path /home/hduser/jars/ddhybrid.jar --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar'
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/GCPFirstProject-d75f1b3a9817.json"
alias ssbq='spark-shell --driver-class-path ~/jars/ddhybrid.jar --jars ~/jars/ddhybrid.jar'
alias bq='spark-shell --driver-class-path ~/jars/googlebigquery.jar --jars ~/jars/googlebigquery.jar'
CLASSPATH=/home/hduser/jars/ddhybrid.jar:/home/hduser/jars/spark-bigquery-with-dependencies_2.11-0.17.2.jar:/home/hduser/jars/spark-bigquery_2.11-0.2.7.jar
export JVM_ARGS="-Xmx1024m -XX:MaxPermSize=256m"
SIMBAJARS=""
for i in `ls ${HOME}/Simba/*.jar `
do
    SIMBAJARS=$SIMBAJARS,$i
done
alias gc='/usr/bin/google-chrome &'
alias sshell='rlwrap spark-shell --driver-class-path /home/hduser/jars/ddhybrid.jar --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar --master yarn --deploy-mode client'
alias yarns="rlwrap myspark-shell_yarn --driver-class-path /home/hduser/jars/ddhybrid.jar --jars /home/hduser/jars/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar --packages com.github.samelamin:spark-bigquery_2.11:0.2.6"
alias crap="rlwrap myspark-shell_yarn --driver-class-path /home/hduser/jars/ddhybrid.jar --jars /home/hduser/jars/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar,/home/hduser/jars/com.google.http-client_google-http-client-1.24.1.jar,/home/hduser/jars/com.google.http-client_google-http-client-jackson2-1.24.1.jar,/home/hduser/jars/com.google.cloud.bigdataoss_util-1.9.4.jar,/home/hduser/jars/com.google.api-client_google-api-client-1.24.1.jar,/home/hduser/jars/com.google.oauth-client_google-oauth-client-1.24.1.jar,/home/hduser/jars/com.google.apis_google-api-services-bigquery-v2-rev398-1.24.1.jar,/home/hduser/jars/com.google.cloud.bigdataoss_bigquery-connector-0.13.4-hadoop2.jar,/home/hduser/jars/spark-bigquery_2.11-0.2.6.jar"
alias yarnp='spark-submit --master yarn --deploy-mode client --driver-class-path /home/hduser/jars/ddhybrid.jar --jars /home/hduser/jars/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar --packages com.github.samelamin:spark-bigquery_2.11:0.2.6'
alias crapp="spark-submit --master yarn --deploy-mode client --driver-class-path /home/hduser/jars/ddhybrid.jar --jars /home/hduser/jars/spark-bigquery-latest.jar,/home/hduser/jars/ddhybrid.jar,/home/hduser/jars/com.google.http-client_google-http-client-1.24.1.jar,/home/hduser/jars/com.google.http-client_google-http-client-jackson2-1.24.1.jar,/home/hduser/jars/com.google.cloud.bigdataoss_util-1.9.4.jar,/home/hduser/jars/com.google.api-client_google-api-client-1.24.1.jar,/home/hduser/jars/com.google.oauth-client_google-oauth-client-1.24.1.jar,/home/hduser/jars/com.google.apis_google-api-services-bigquery-v2-rev398-1.24.1.jar,/home/hduser/jars/com.google.cloud.bigdataoss_bigquery-connector-0.13.4-hadoop2.jar,/home/hduser/jars/spark-bigquery_2.11-0.2.6.jar"
export CONDA_HOME=/home/hduser/miniconda2
export PATH=$PATH:$CONDA_HOME/bin
cd /usr/src/Python-3.7.9/environments; source virtualenv/bin/activate;cd
