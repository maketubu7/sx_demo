#!/bin/bash
##全量入hbase, 人物图层相关明细
export PATH=$PATH
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/relation_history_score hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/relation_mid_score hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/edge_person_groupcall_detail hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/edge_person_groupmsg_detail hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/edge_person_grouppost_detail hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/relation_post_month hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/relation_samehotel_month hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/relation_telcall_month hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/relation_telmsg_month hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/relation_withair_month hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/relation_withinter_month hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/relation_withtrain_month hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_person_train_detail/ hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp /phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_person_airline_detail/ hdfs://24.1.17.4:8020/user/shulian/person_relation
hadoop distcp -Ddistcp.bytes.per.map=1073741824 -Ddfs.client.socket-timeout=240000000 -Dipc.client.connect.timeout=40000000 /phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_person_internetbar_detail/ hdfs://24.1.17.4:8020/user/shulian/person_relation