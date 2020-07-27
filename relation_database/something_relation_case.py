# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : company.py
# @Software: PyCharm
# @content : 工商相关信息

import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession
# from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time, copy, re, math
from datetime import datetime, timedelta,date
import json
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')
#warehouse_location = '/user/hive/warehouse/'
conf=SparkConf().set('spark.driver.maxResultSize', '2g')
conf.set('spark.yarn.executor.memoryOverhead', '30g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '2g')
conf.set('spark.executor.instances', 10)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')
#conf.set("spark.sql.warehouse.dir", warehouse_location)


spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commonUtil = CommonUdf(spark)

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/case_link_person'
save_root = 'relation_theme_extenddir'

def something_relate_case():
    # cert_type 证件类型
    # cert_num 证件号码
    # case_no 案件编号
    # case_name 案件名称
    # case_time 立案时间
    # rel_type 关系类型
    # last_dis_place 最后发现地

    ## 案件关联人关系
    # DC01 报案
    # DC02 受害
    # DC03 嫌疑
    # DC04 证人
    init(spark, 'nb_app_dws_per_cas_his_dccase', if_write=False, is_relate=True)
    sql = '''
        select case_no asjbh, cert_num sfzh, cast(case_time as bigint) start_time,
        cast(case_time as bigint) end_time,rel_type type from nb_app_dws_per_cas_his_dccase
        where verify_sfz(cert_num) = 1 and case_no is not null and case_no != ''
    '''
    cols = ['asjbh','sfzh','if(start_time>0,start_time,0) start_time','if(end_time>0,end_time,0) end_time','type']
    source = spark.sql(sql).selectExpr(*cols).dropDuplicates(["sfzh","asjbh","type"])

    source.persist()
    write_orc(source.where('type="DC02"').drop('type'),add_save_path('edge_case_victim_person',root=save_root))
    write_orc(source.where('type="DC01"').drop('type'),add_save_path('edge_case_reportby_person',root=save_root))
    write_orc(source.where('type="DC03"').drop('type'),add_save_path('edge_case_suspectis_person',root=save_root))
    write_orc(source.where('type="DC04"').drop('type'),add_save_path('edge_case_witness_person',root=save_root))
    source.unpersist()

    ## 案件关联机动车
    # case_no 案件编号
    # veh_no 车辆识别代号
    # veh_lic_tcode 机动车号牌种类
    # veh_plate_num 机动车号牌号码
    # dis_place 发现地

    init(spark, 'nb_app_dws_cas_res_his_vehcas', if_write=False, is_relate=True)
    sql = '''
        select case_no asjbh, veh_plate_num autolpn,cast(upd_time as bigint) start_time,
        cast(upd_time as bigint) end_time,veh_lic_tcode type from nb_app_dws_cas_res_his_vehcas
        where case_no is not null and case_no != '' and veh_plate_num is not null and veh_plate_num != ''
    '''

    source = spark.sql(sql)
    df1 = source.drop('type').dropDuplicates(['asjbh','autolpn'])
    write_orc(df1,add_save_path('edge_case_link_autolpn',root=save_root))

    # 车牌号节点
    cols = ['autolpn','mc hpzl']
    df2 = source.selectExpr('autolpn','type')
    hpzl_dm = get_all_dict(spark).where('lb="hpzl_type"')
    v_df = df2.join(hpzl_dm,df2['type']==hpzl_dm['dm'],'left').selectExpr(*cols).dropDuplicates(['autolpn'])
    write_orc(v_df,add_save_path('vertex_autolpn',root=save_root))


    ## 真实案件的人，电话的关联关系
    link_person1 = read_orc(spark,add_save_path('edge_case_link_person_zp',root=save_root))
    link_person2 = read_orc(spark,add_save_path('edge_case_link_person_steal',root=save_root))
    link_phone1 = read_orc(spark,add_save_path('edge_case_link_phone_zp',root=save_root))
    link_phone2 = read_orc(spark,add_save_path('edge_case_link_phone_steal',root=save_root))

    link_person = link_person1.unionAll(link_person2).dropDuplicates(['sfzh','asjbh'])
    link_phone = link_phone1.unionAll(link_phone2).dropDuplicates(['phone','asjbh'])

    write_orc(link_person,add_save_path('edge_case_link_person',root=save_root))
    write_orc(link_phone,add_save_path('edge_case_link_phone',root=save_root))


def edge_phone_call_alert():
    '''
    报告警情
    1、ODS_POL_CRIM_ENFCAS_POL_INFO	警情基本信息
        CP_PER_CERT_NO	报警人_公民身份号码
        POLI_NO	警情序号
        POL_CASE_NO	警情编号
        ACCE_ALARM_TIME	接警时间
    :return:
    '''
    cols = ['phone', 'jqbh', 'bjrsfzh', 'bjrxm', 'jjsj','jjsj start_time','jjsj+3600 end_time']
    sql = '''
        select format_phone(cp_ctct_tel) phone, format_data(pol_case_no) jqbh,
        format_zjhm(cp_per_cert_no) bjrsfzh, format_data(cp_per_name) bjrxm,
        cast(format_timestamp(acce_alarm_time) as bigint) jjsj, 'ods_pol_crim_enfcas_pol_info' tablename 
        from ods_pol_crim_enfcas_pol_info
        where verify_phonenumber(cp_ctct_tel) = 1 and format_data(pol_case_no) != '' 
    '''
    init(spark,'ods_pol_crim_enfcas_pol_info',if_write=False)
    df = spark.sql(sql).drop_duplicates(['phone','jqbh']).selectExpr(*cols)

    write_orc(df,add_save_path('edge_phone_call_alert',root=save_root))
    logger.info('edge_phone_call_alert down')

def edge_alert_link_case():
    '''
    立案 警情-案件
    1、ODS_POL_CRIM_ENFCAS_POL_INFO	警情基本信息
        POL_CASE_NO	警情编号
        CASE_NO	案件编号
        ACCE_ALARM_TIME	接警时间
    2、ODS_POL_CRIM_ENFO_CASE_INFO	案件基本信息
        CASE_NO	案件编号
        POL_CASE_NO	警情编号
        ACCEP_TIME	受理时间
        BRIEF_CASE	简要案情
        REPO_CASE_TIME	报案时间
    :return:
    '''
    cols = ['jqbh', 'asjbh', 'link_time','link_time start_time','0 end_time']
    tmp_sql = '''
        select format_data(pol_case_no) jqbh, format_data(case_no) asjbh,
        {link_time} link_time,'{tablename}' tablename
        from {tablename}
    '''

    sql1 = tmp_sql.format(link_time=''' acce_alarm_time ''',tablename='ods_pol_crim_enfcas_pol_info')
    sql2 = tmp_sql.format(link_time=''' cast(format_timestamp(coalesce_str(accep_time,disc_time)) as bigint) ''',
                          tablename='ods_pol_crim_enfo_case_info')

    init(spark,'ods_pol_crim_enfcas_pol_info',if_write=False)
    init(spark,'ods_pol_crim_enfo_case_info',if_write=False)

    df1 = spark.sql(sql1)
    df2 = spark.sql(sql2)
    df = df1.unionAll(df2)
    df = df.drop_duplicates(['jqbh','asjbh']).selectExpr(*cols)
    write_orc(df,add_save_path('edge_alert_link_case',root=save_root))

    logger.info("edge_alert_link_case down")

def vertex_case():

    # ods_pol_crim_enfo_case_info	案件基本信息
    # case_no	案件编号
    # pol_case_no	警情编号
    # case_name	案件名称
    # cascla_name	案件类别
    # case_iden	案件标识
    # accep_time	受理时间
    # undo_case_date	撤案日期
    # disc_time	发现时间
    # disc_loca_addi_code	发现地点行政区划代码
    # disc_loca_admin_name	发现地点行政区划
    # disc_loca_addr_name	发现地点详址
    # case_addr_addi_code	发案地行政区划代码
    # case_addr_admin_name	发案地行政区划
    # case_addr_addr_name	发案地详址
    # case_loss_prop_brief_cond	案事件损失财物_简要情况
    # brief_case	简要案情
    # case_date	立案日期
    # repo_case_time	报案时间
    # case_attr_code	案件属性

    table_list = [

        {'asjbh': 'case_no', 'ajmc': 'case_name', 'asjfskssj': 'accep_time', 'asjfsjssj': 'undo_case_date',
         'asjly': 'cascla_name','ajlb': 'cascla_name', 'fxasjsj': 'disc_time', 'fxasjdd_dzmc': 'case_addr_addr_name',
         'jyaq': 'brief_case', 'ajbs': 'case_iden', 'larq': 'case_date','city':'case_addr_admin_name','link_app':'',
         'tablename': 'ods_pol_crim_enfo_case_info'}
    ]

    tmp_sql = '''
            select format_data({asjbh}) asjbh, {ajmc} ajmc, cast(format_timestamp({asjfskssj}) as bigint) asjfskssj,
            cast(valid_datetime({asjfsjssj}) as bigint) asjfsjssj, {asjly} asjly, {ajlb} ajlb,
            cast(format_timestamp({fxasjsj}) as bigint) fxasjsj, {fxasjdd_dzmc} fxasjdd_dzmc, {jyaq} jyaq,
            {ajbs} ajbs, cast(valid_datetime({larq}) as bigint) larq, {city} city, '{link_app}' link_app, '{tablename}' tablename
            from {tablename} where format_data({asjbh}) != ''
        '''

    sql = tmp_sql.format(**table_list[0])
    init(spark,table_list[0].get('tablename'),if_write=False)
    df = spark.sql(sql).dropDuplicates(['asjbh'])
    write_orc(df,add_save_path('vertex_case',root=save_root))

def vertex_alert():
    table_list = [
        {'jqbh': 'pol_case_no', 'jqxz': 'pol_natu', 'jqfxdz': 'pol_case_occu_plac_addr_name',
         'jyqk': 'brief_cond', 'jjsj': 'cast(format_timestamp(acce_alarm_time) as bigint)',
         'tablename': 'ods_pol_crim_enfcas_pol_info','table_sort':1},
        {'jqbh': 'pol_case_no', 'jqxz': '""', 'jqfxdz': 'disc_loca_addr_name', 'jyqk': 'brief_case',
         'jjsj': 'cast(format_timestamp(repo_case_time) as bigint)', 'tablename': 'ods_pol_crim_enfo_case_info','table_sort':2},
    ]

    tmp_sql = '''
                select format_data({jqbh}) jqbh, {jqxz} jqxz, {jqfxdz} jqfxdz,
                {jyqk} jyqk,  {jjsj} jjsj, '{tablename}' tablename, {table_sort} table_sort
                from {tablename}
                where format_data({jqbh}) != ''
            '''

    union_df = create_uniondf(spark,table_list,tmp_sql)

    res = union_df.selectExpr('*','row_number() over (partition by jqbh order by table_sort asc) num') \
                        .where('num=1').drop('num').drop('table_sort')

    write_orc(res,add_save_path('vertex_alert',root=save_root))
    logger.info('vertex_alert down')

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    something_relate_case()
    edge_phone_call_alert()
    edge_alert_link_case()
    vertex_case()
    vertex_alert()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

