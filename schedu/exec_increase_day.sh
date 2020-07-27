#!/bin/bash
export JAVA_HOME=/opt/app/jdk1.8.0_172
export SPARK_HOME=/opt/app/spark-2.3.1-bin-hadoop2.6
export PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin
yesterday=`date -d last-day +%Y%m%d00`
#echo 'asdas' > '/opt/workspace/sx_graph/logs/day_'$yesterday'.log'
/root/anaconda2/bin/python /opt/workspace/sx_graph/relation_theme_extenddir/exec_increase_sparkjob.py > '/opt/workspace/sx_graph/logs/day_'$yesterday'.log' 2>&1
/opt/app/hadoop-2.6.0/bin/hdfs dfs -rm -r -f /phoebus/_fileservice/users/slmp/shulianmingpin/midfile/call_msg_tmp/*
/bin/bash /opt/workspace/sx_graph/distcp_schedu/increase_detail_data.sh $yesterday
