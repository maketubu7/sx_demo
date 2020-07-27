CREATE TABLE IF NOT EXISTS slmp_hive.ods_gov_edu_stud_info(
    coll_time string,
    name string,
    fur_name string,
    cred_num string,
    guar_name string,
    guar_cert_no string,
    father_name string,
    father_cert_no string,
    father_pred_addr_name string,
    mother_name string,
    mother_cert_no string,
    mother_pred_addr_name string)
     PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/ods_gov_edu_stud_info'

CREATE TABLE IF NOT EXISTS slmp_hive.ods_pol_pub_dw_evt(
    capture_time string,
    user_num string,
    user_homloc_ctct_tel_arcod string,
    oppos_num string,
    oppos_homloc_ctct_tel_arcod string,
    user_imsi string,
    user_imei string,
    loc_area_code string,
    bassta_houest_idecod string,
    bassta_lon string,
    bassta_lat string,
    bassta_geohash string,
    norma_bassta_lon string,
    norma_bassta_lat string,
    norma_bassta_geohash string)
     PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/ods_pol_pub_dw_evt';



CREATE external TABLE IF NOT EXISTS slmp_hive.edge_groupcall_detail(
    start_phone string,
    end_phone string,
    start_time bigint,
    end_time bigint,
    call_duration bigint,
    tablename string,
    homearea string,
    relatehomeac string)
     PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_groupcall_detail';


CREATE external TABLE IF NOT EXISTS slmp_hive.edge_groupcall(
    start_phone string,
    end_phone string,
    start_time bigint,
    end_time bigint,
    call_total_duration bigint,
    call_total_times bigint)
     PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_groupcall';

CREATE external TABLE IF NOT EXISTS slmp_hive.bbd_dw_detail_tmp(
    phone string,
    imsi string,
    imei string,
    lon string,
    lat string,
    geohash string,
    event_type string,
    capture_time string)
     PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/bbd_dw_detail_tmp';


CREATE TABLE IF NOT EXISTS slmp_hive.bbd_dw_detail_tmp(
    phone string,
    imsi string,
    imei string,
    lon string,
    lat string,
    event_type string,
    capture_time bigint)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/dw_detail_tmp';

CREATE TABLE IF NOT EXISTS slmp_hive.edge_person_arrived_airline_detail_tmp(
    sfzh string,
    airlineid string,
    hbh string,
    hbrq bigint,
    yjddsj bigint,
    sfd string,
    mdd string,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/edge_person_arrived_airline_detail';

CREATE external TABLE IF NOT EXISTS slmp_hive.edge_person_arrived_airline(
    sfzh string,
    airlineid string,
    hbh string,
    hbrq bigint,
    yjddsj bigint,
    sfd string,
    mdd string,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/edge_person_arrived_airline';


CREATE external TABLE IF NOT EXISTS slmp_hive.edge_person_checkin_airline(
    sfzh string,
    airlineid string,
    hbh string,
    hbrq bigint,
    sfd string,
    mdd string,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/edge_person_checkin_airline';

drop table slmp_hive.edge_person_reserve_airline;
CREATE external TABLE IF NOT EXISTS slmp_hive.edge_person_reserve_airline(
    sfzh string,
    airlineid string,
    hbh string,
    hbrq bigint,
    sfd string,
    mdd string,
    yjqfsj bigint,
    yjddsj bigint,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_person_reserve_airline';



CREATE external TABLE IF NOT EXISTS slmp_hive.vertex_person(
    zjhm string,
    zjlx string,
    gj string,
    xm string,
    ywxm string,
    zym string,
    xb string,
    csrq string,
    mz string,
    jg string,
    whcd string,
    hyzk string,
    zzmm string,
    hkszdxz string,
    sjjzxz string,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/relation_theme_extenddir/vertex_person';


CREATE external TABLE IF NOT EXISTS slmp_hive.edge_person_stay_hotel_detail(
    sfzh string,
    lgdm string,
    zwmc string,
    start_time bigint,
    end_time bigint,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_person_stay_hotel_detail';

CREATE external TABLE IF NOT EXISTS slmp_hive.vertex_hotel(
    lgdm string,
    qiyemc string,
    yyzz string,
    xiangxidizhi string,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/vertex_hotel'


CREATE external TABLE IF NOT EXISTS slmp_hive.edge_person_stay_hotel(
    sfzh string,
    lgdm string,
    start_time bigint,
    end_time bigint,
    num bigint)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_person_stay_hotel';


CREATE external TABLE IF NOT EXISTS slmp_hive.edge_groupmsg_detail(
    start_phone string,
    end_phone string,
    start_time bigint,
    end_time bigint,
    send_time bigint,
    message string,
    homearea string,
    relatehomeac string,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_groupmsg_detail';

CREATE external TABLE IF NOT EXISTS slmp_hive.edge_groupmsg(
    start_phone string,
    end_phone string,
    start_time bigint,
    end_time bigint,
    message_number bigint)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/edge_groupmsg';


CREATE external TABLE IF NOT EXISTS slmp_hive.edge_groupcall_day(
    start_phone string,
    end_phone string,
    start_time bigint,
    end_time bigint,
    call_duration bigint,
    homearea string,
    relatehomeac string,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/incrdir/edge_groupcall_detail';

CREATE external TABLE IF NOT EXISTS slmp_hive.edge_groupcall(
    start_phone string,
    end_phone string,
    start_time bigint,
    end_time bigint,
    call_total_duration bigint,
    call_total_times bigint)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/edge_groupcall';


CREATE external TABLE IF NOT EXISTS slmp_hive.edge_person_open_phone(
    sfzh string,
    phone string,
    start_time bigint,
    end_time bigint,
    is_xiaohu string,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/edge_person_open_phone';


CREATE external TABLE IF NOT EXISTS slmp_hive.edge_person_smz_phone(
    start_perosn string,
    end_phone string,
    start_time bigint,
    end_time bigint)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/edge_person_smz_phone';



CREATE external TABLE IF NOT EXISTS slmp_hive.edge_person_smz_phone_top(
    start_perosn string,
    end_phone string,
    start_time bigint,
    end_time bigint)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/edge_person_smz_phone_top';



CREATE external TABLE IF NOT EXISTS slmp_hive.edge_person_spouse_person(
    sfzhnan string,
    sfzhnv string,
    start_time bigint,
    end_time bigint,
    hqsj bigint,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/edge_person_spouse_person';


CREATE external TABLE IF NOT EXISTS slmp_hive.vertex_imsi(
    imsi string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/vertex_imsi';


CREATE external TABLE IF NOT EXISTS slmp_hive.vertex_imei(
    imei string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/datatmp/vertex_imei';


CREATE external TABLE IF NOT EXISTS slmp_hive.vertex_case(
    asjbh string,
    ajmc string,
    asjfskssj bigint,
    asjfsjssj bigint,
    asjly string,
    ajlb string,
    fxasjsj bigint,
    fxasjdd_dzmc string,
    jyaq string,
    ajbs string,
    larq bigint,
    tablename string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/vertex_case';


CREATE external TABLE IF NOT EXISTS slmp_hive.relation_history_score(
    sfzh1 string,
    sfzh2 string,
    label string,
    zb_score double,
    tb_score double)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/relation_history_score';


CREATE external TABLE IF NOT EXISTS slmp_hive.relation_mid_score(
    sfzh1 string,
    sfzh2 string,
    score double,
    label string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/person_relation_detail/relation_mid_score';


CREATE external TABLE IF NOT EXISTS slmp_hive.relation_score_res(
    sfzh1 string,
    sfzh2 string,
    data string,
    last_tb_score double,
    last_zb_score double)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/relation_score_res';


CREATE external TABLE IF NOT EXISTS slmp_hive.edge_person_train_detail(
    sfzh1 string,
    sfzh2 string,
    trianid string,
    cc string,
    fcrq bigint,
    fz string,
    dz string)
    PARTITIONED BY (
    `cp` bigint)
     ROW FORMAT SERDE
       'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
     STORED AS INPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
     OUTPUTFORMAT
       'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
     LOCATION
        'hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_person_train_detail';