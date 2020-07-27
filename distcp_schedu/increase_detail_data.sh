#增量入hbase文件
#export JAVA_HOME=/opt/app/jdk1.8.0_172
#export HDFS_HOME=/opt/app/hadoop-2.6.0/
export PATH=$PATH
/opt/app/hadoop-2.6.0/bin/hadoop distcp -m 30 /phoebus/_fileservice/users/slmp/shulianmingpin/incrdir/edge_groupcall_detail/cp=$1 hdfs://24.1.17.4:8020/user/shulian/incrdir/edge_groupcall_detail
/opt/app/hadoop-2.6.0/bin/hadoop distcp -m 30 /phoebus/_fileservice/users/slmp/shulianmingpin/incrdir/edge_groupmsg_detail/cp=$1 hdfs://24.1.17.4:8020/user/shulian/incrdir/edge_groupmsg_detail
/opt/app/hadoop-2.6.0/bin/hadoop distcp -m 30 /phoebus/_fileservice/users/slmp/shulianmingpin/incrdir/edge_person_stay_hotel_detail/cp=$1 hdfs://24.1.17.4:8020/user/shulian/incrdir/edge_person_stay_hotel_detail
/opt/app/hadoop-2.6.0/bin/hadoop distcp -m 30 /phoebus/_fileservice/users/slmp/shulianmingpin/incrdir/edge_person_surfing_internetbar_detail/cp=$1 hdfs://24.1.17.4:8020/user/shulian/incrdir/edge_person_surfing_internetbar_detail
