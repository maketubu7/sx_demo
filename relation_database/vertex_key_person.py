# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : vertex_key_person.py
# @Software: PyCharm
# @content : 重点 前科 吸毒 涉毒人员汇总

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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'


def vertex_key_personnel():

    table_info = [
        #重点人
        {'tablename':'ods_pol_per_keyper_info','sfzh':'cert_no','type_code':'if(key_per_per_type_code !="",concat("K",trim(key_per_per_type_code)),"K1")','cond':'verify_sfz(cert_no) = 1'},
        # 涉毒
        {'tablename':'ods_pol_dct_druper_basinf','sfzh':'cred_num','type_code':'"K2"','cond':'verify_sfz(cred_num) = 1'},
        {'tablename':'ods_pol_pri_drutrpla_pers_info','sfzh':'cred_num','type_code':'"K2"','cond':'comm_cert_code = "111" and verify_sfz(cred_num) = 1'},
        {'tablename':'ods_pol_pri_drutre_outper_info','sfzh':'cert_no','type_code':'"K2"','cond':'verify_sfz(cert_no) = 1'},
        # 前科
        {'tablename':'ods_pol_crim_dethou_detes_info','sfzh':'cred_num','type_code':'"K3"','cond':'comm_cert_code = "111" and verify_sfz(cred_num) = 1'},
        {'tablename':'ods_pol_pri_detesta_outper_info','sfzh':'cert_no','type_code':'"K3"','cond':'verify_sfz(cert_no) = 1'},
        {'tablename':'ods_pol_pri_dethou_detees_info','sfzh':'cred_num','type_code':'"K4"','cond':'comm_cert_code = "111" and verify_sfz(cred_num) = 1 and trim(felon_judge_flag) = "1"'},
        {'tablename':'ods_pol_pri_dethou_detees_info','sfzh':'cred_num','type_code':'"K3"','cond':'comm_cert_code = "111" and verify_sfz(cred_num) = 1 and trim(felon_judge_flag) != "1"'},
        {'tablename':'ods_pol_pri_detsta_outper_info','sfzh':'cert_no','type_code':'"K3"','cond':'verify_sfz(cert_no) = 1'},
    ]

    tmp_sql = '''
            select format_zjhm({sfzh}) sfzh, {type_code} type_code from {tablename} where {cond}
    '''
    dfs = []
    for info in table_info:
        sql = tmp_sql.format(**info)
        init(spark,info.get("tablename"),if_write=False)
        df = spark.sql(sql)
        if df.take(1):
            dfs.append(df)
    res = reduce(lambda a,b:a.union(b),dfs)
    res = res.dropDuplicates(['sfzh','type_code'])
    write_orc(res,add_save_path('vertex_key_person',root=save_root))

def person_stay_kss():
    table_list = [
        {'tablename': 'ods_pol_pri_dethou_detees_info', 'sfzh': 'cred_num', 'jsbh': 'deten_hou_no',
         'ajbh': '""', 'jyaq': 'brief_case', 'start_time': 'ente_stat_date',
         'end_time': 'out_pris_date', 'fjh': 'monroo_no', 'table_sort': 1},
        {'tablename': 'ods_pol_pri_detsta_outper_info', 'sfzh': 'cert_no',
         'jsbh': 'deten_hou_no', 'ajbh': '""', 'jyaq': '""', 'start_time': 0,
         'end_time': 'ackn_out_pris_date', 'fjh': '""',
         'table_sort': 2}
    ]

    tmp_sql = '''
            select format_zjhm({sfzh}) sfzh, format_data({jsbh}) jsbh, trim({ajbh}) ajbh,
            {jyaq} jyaq, valid_datetime({start_time}) start_time, valid_datetime({end_time}) end_time,
            trim({fjh}) fjh, '{tablename}' tablename, {table_sort} table_sort
            from {tablename} where verify_sfz({sfzh}) = 1 and format_data({jsbh}) != ''
        '''

    union_df = create_uniondf(spark,table_list, tmp_sql)
    res = union_df.selectExpr("*", "row_number() over (partition by sfzh,jsbh order by jsbh asc) num") \
        .where('num=1').drop('num').drop('table_sort')

    write_orc(res, add_save_path('edge_person_stay_kss',root=save_root))
    logger.info('edge_person_stay_kss down')


def person_stay_jds():
    table_list = [
        {'tablename': 'ods_pol_pri_drutrpla_pers_info', 'sfzh': 'cred_num', 'jsbh': 'drug_trea_place_no',
        'start_time': 'ente_stat_date',
         'end_time': 'out_pris_date', 'fjh': 'drug_trea_room_no', 'table_sort': 1},
        {'tablename': 'ods_pol_pri_drutre_outper_info', 'sfzh': 'cert_no','jsbh':'priso_no',
         'start_time': 0,
         'end_time': 'OUT_PRIS_DATE', 'fjh': 'out_pris_time_room_no',
         'table_sort': 2},
    ]

    tmp_sql = '''
                select format_zjhm({sfzh}) sfzh, format_data({jsbh}) jsbh, valid_datetime({start_time}) start_time,
                 valid_datetime({end_time}) end_time,trim({fjh}) fjh, '{tablename}' tablename, {table_sort} table_sort
                from {tablename} where verify_sfz({sfzh}) = 1 and format_data({jsbh}) != ''
            '''

    union_df = create_uniondf(spark,table_list, tmp_sql)
    res = union_df.selectExpr("*", "row_number() over (partition by sfzh,jsbh order by jsbh asc) num") \
        .where('num=1').drop('num').drop('table_sort')

    write_orc(res, add_save_path('edge_person_stay_jds',root=save_root))
    logger.info('edge_person_stay_jds down')

def person_stay_jail():
    kss = read_orc(spark,add_save_path('edge_person_stay_kss',root=save_root)).withColumn('type_name',lit('看守所'))
    cols = ['sfzh','jsbh','"" ajbh','"" jyaq','start_time','end_time','fjh','tablename','"戒毒所" type_name']
    jds = read_orc(spark,add_save_path('edge_person_stay_jds',root=save_root)).selectExpr(*cols)
    res = kss.unionAll(jds).dropDuplicates(['sfzh','jsbh','type_name'])
    write_orc(res,add_save_path('edge_person_stay_jail',root=save_root))
    logger.info('edge_person_stay_jail down')



def vertex_jail():
    '''
    监所节点信息 编号，名称
    上述所有表的监所信息提取
    :return:
    '''
    table_list = [
        {'tablename':'ods_pol_pri_drutrpla_pers_info','unitid':'drug_trea_place_no','unitname':'drug_trea_place_desig','unittype':u'戒毒所'},
        {'tablename':'ods_pol_pri_drutre_outper_info','unitid':'priso_no','unitname':'priso_desig','unittype':u'戒毒所'},
        {'tablename':'ods_pol_pri_detsta_outper_info','unitid':'deten_hou_no','unitname':'deten_hou_desig','unittype':u'看守所'},
        {'tablename':'ods_pol_pri_dethou_detees_info','unitid':'deten_hou_no','unitname':'deten_hou_desig','unittype':u'看守所'},
    ]

    tmp_sql = '''
                    select format_data({unitid}) unitid, format_data({unitname}) unitname,
                    '{unittype}' unittype, '{tablename}' tablename
                    from {tablename} where format_data({unitid}) != ''
                '''

    union_df = create_uniondf(spark,table_list,tmp_sql)

    res = union_df.drop_duplicates(['unitid'])

    write_orc(res,add_save_path('vertex_jail',root=save_root))
    logger.info('vertex_jail down')


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    vertex_key_personnel()
    person_stay_kss()
    person_stay_jds()
    person_stay_jail()
    vertex_jail()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))