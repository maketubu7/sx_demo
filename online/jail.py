# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:50
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : jail.py
# @Software: PyCharm
# @content : 监所相关信息
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

conf=SparkConf().set('spark.driver.maxResultSize', '10g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '20g')
conf.set('spark.executor.instances', 20)
conf.set('spark.executor.cores', 8)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()
from common import *
commonUtil = CommonUdf(spark)


path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/jail'

##todo:all
def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix,'parquet/',path))
    return df
def write_parquet(df,path):
    '''写 parquet'''
    df.write.mode("overwrite").parquet(path=os.path.join(path_prefix,'parquet/',path))


def write_orc(df, path,mode='overwrite'):
    df.write.format('orc').mode(mode).save(path)
    logger.info('write success')


def read_orc(path):
    df = spark.read.orc(path)
    return df

def edge_person_stay_jls():
    '''
    1、ODS_POL_CRIM_DETHOU_DETES_INFO	拘留所在押人员信息
        DETSTA_NO	拘留所_编号
        DETSTA_DESIG	拘留所_名称
        ENTE_STAT_NO	入所编号
        CRED_NUM	证件号码
        BRIEF_CASE	简要案情
        ENTE_STAT_DATE	入所日期
        OUT_PRIS_DATE	出所日期
    2、ODS_POL_PRI_DETESTA_OUTPER_INFO	拘留所出所人员信息
        DETSTA_NO	拘留所_编号
        DETSTA_DESIG	拘留所_名称
        PERSON_NO	人员_编号
        CERT_NO	公民身份号码
        OI_PRIS_NO	出入所_编号
        OUT_PRIS_DATE	出所日期
    3、ODS_POL_PRI_PRIENT_HEAEXA	拘留所入所健康检查信息
        DETSTA_NO	拘留所_编号
        DETSTA_DESIG	拘留所_名称
        CRED_NUM	证件号码
        PERSON_NO	人员_编号
        CHECK_TIME	检查日期
    4、ODS_POL_PRI_PRIPER_ROOADJ	拘留所人员监室调整信息
        DETSTA_NO	拘留所_编号
        DETSTA_DESIG	拘留所_名称
        CRED_NUM	证件号码
        PERSON_NO	人员_编号
    :return:
    '''
    table_list = [
        {'tablename':'ods_pol_crim_dethou_detes_info','sfzh':'cred_num','jsbh':'detsta_no',
         'ajbh':'case_no','jyaq':'brief_case','start_time':'ente_stat_date',
         'end_time':'out_pris_date','fjh':'dethou_no','table_sort':1},
        {'tablename': 'ods_pol_pri_detesta_outper_info', 'sfzh': 'cert_no',
         'jsbh': 'detsta_no', 'ajbh': '""', 'jyaq': '""','start_time': 0,
         'end_time': 'out_pris_date', 'fjh': 'out_pris_time_room_no',
         'table_sort': 2},
        {'tablename': 'ods_pol_pri_prient_heaexa', 'sfzh': 'cred_num',
         'jsbh': 'detsta_no', 'ajbh': '""', 'jyaq': '""', 'start_time': 'check_time',
         'end_time': 0, 'fjh': '""','table_sort': 3},
        {'tablename': 'ods_pol_pri_priper_rooadj', 'sfzh': 'cred_num','jsbh': 'detsta_no',
         'ajbh': '""', 'jyaq': '""', 'start_time': 0,'end_time': 0, 'fjh': 'orig_monroo_no',
         'table_sort': 4},
    ]

    tmp_sql = '''
        select format_zjhm({sfzh}) sfzh, format_data({jsbh}) jsbh, trim({ajbh}) ajbh,
        {jyaq} jyaq, valid_datetime({start_time}) start_time, valid_datetime({end_time}) end_time,
        trim({fjh}) fjh, '{tablename}' tablename, {table_sort} table_sort
        from {tablename} where verify_sfz({sfzh}) = 1 and format_data({jsbh}) != ''
    '''

    union_df = create_uniondf(spark,table_list,tmp_sql)
    res = union_df.selectExpr("*","row_number() over (partition by sfzh,jsbh order by jsbh asc) num") \
            .where('num=1').drop('num').drop('table_sort')

    write_orc(res,add_save_path('edge_person_stay_jls'))
    logger.info('edge_person_stay_jls down')

def edge_person_stay_kss():
    '''
    1、ODS_POL_PRI_DETSTA_OUTPER_INFO	看守所出所人员信息
        DETEN_HOU_NO	看守所_编号
        DETEN_HOU_DESIG	看守所_名称
        PERSON_NO	人员_编号
        CERT_NO	公民身份号码
        ACKN_OUT_PRIS_DATE	确认_出所日期
        DECI_DATE	决定日期
    2、ODS_POL_PRI_HIST_DETES_DEARES	看守所历史在押人员处理结果
        DETEN_HOU_NO	看守所_编号
        DETEN_HOU_DESIG	看守所_名称
        PERSON_NO	人员_编号
        CERT_NO	公民身份号码
        PROC_TIME	处理时间
    3、ODS_POL_PRI_JAIENT_HEAEXA	看守所入所健康检查信息
        DETEN_HOU_NO	看守所_编号
        DETEN_HOU_DESIG	看守所_名称
        CRED_NUM	证件号码
        ENTE_STAT_NO	入所编号
        CHECK_TIME	检查日期
    4、ODS_POL_PRI_JAIPER_ROOADJ	看守所人员监室调整信息
        DETEN_HOU_NO	看守所_编号
        DETEN_HOU_DESIG	看守所_名称
        CRED_NUM	证件号码
        PERSON_NO	人员_编号
    5、ODS_POL_PRI_DETHOU_DETEES_INFO	看守所在押人员信息
        DETEN_HOU_NO	看守所_编号
        DETEN_HOU_DESIG	看守所_名称
        DETEN_DATE	拘留日期
        MONROO_NO	监室编号
        DETEN_HOU_PERSON_NO	看守所_人员_编号
        CRED_NUM	证件号码
        ENTE_STAT_DATE	入所日期

    :return:
    '''
    table_list = [
        {'tablename': 'ods_pol_pri_dethou_detees_info', 'sfzh': 'cred_num', 'jsbh': 'deten_hou_no',
         'ajbh': '""', 'jyaq': 'brief_case', 'start_time': 'ente_stat_date',
         'end_time': 0, 'fjh': 'monroo_no', 'table_sort': 1},
        {'tablename': 'ods_pol_pri_detsta_outper_info', 'sfzh': 'cert_no',
         'jsbh': 'deten_hou_no', 'ajbh': '""', 'jyaq': '""', 'start_time': 0,
         'end_time': 'ackn_out_pris_date', 'fjh': '""',
         'table_sort': 2},
        {'tablename': 'ods_pol_pri_hist_detes_deares', 'sfzh': 'cert_no',
         'jsbh': 'deten_hou_no', 'ajbh': '""', 'jyaq': '""', 'start_time': 'sententce_start_date',
         'end_time': 'sententce_due_date', 'fjh': '""', 'table_sort': 3},
        {'tablename': 'ods_pol_pri_jaient_heaexa', 'sfzh': 'cred_num', 'jsbh': 'deten_hou_no',
         'ajbh': '""', 'jyaq': '""', 'start_time': 'check_time', 'end_time': 0, 'fjh': '""',
         'table_sort': 4},
        {'tablename': 'ods_pol_pri_jaiper_rooadj', 'sfzh': 'cred_num', 'jsbh': 'deten_hou_no',
         'ajbh': '""', 'jyaq': '""', 'start_time': 0, 'end_time': 0, 'fjh': 'orig_monroo_no',
         'table_sort': 5},
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

    write_orc(res, add_save_path('edge_person_stay_kss'))
    logger.info('edge_person_stay_kss down')

def edge_person_stay_jds():
    '''
    1、ODS_POL_PRI_DRCENT_HEAEXA	戒毒所入所健康检查信息
        DRUG_TREA_PLACE_NO	戒毒所_编号
        DRUG_TREA_PLACE_DESIG	戒毒所_名称
        CERT_NO	公民身份号码
        CHECK_TIME	检查日期
        PERSON_NO	人员_编号
    2、ODS_POL_PRI_DRCPER_ROOADJ	戒毒所人员监室调整信息
        DRUG_TREA_PLACE_NO	戒毒所_编号
        DRUG_TREA_PLACE_DESIG	戒毒所_名称
        PERSON_NO	人员_编号
        CRED_NUM	证件号码
    3、ODS_POL_PRI_DRUTRE_OUTPER_INFO	戒毒所出所人员信息
        PRISO_NO	监所_编号
        PRISO_DESIG	监所_名称
        OI_PRIS_NO	出入所_编号
        CERT_NO	公民身份号码
        ENSTA_REGI_NO	入所登记_编号
        OUT_PRIS_DATE	出所日期
        LEASTA_TIME	离所时间
    4、ODS_POL_PRI_DRUTRPLA_PERS_INFO	戒毒所人员信息
        DRUG_TREA_PLACE_NO	戒毒所_编号
        DRUG_TREA_PLACE_DESIG	戒毒所_名称
        CRED_NUM	证件号码
        ENTE_STAT_DATE	入所日期
        DRUG_TREA_ROOM_NO	戒毒室_编号
        OUT_PRIS_DATE	出所日期
    :return:
    '''
    table_list = [
        {'tablename': 'ods_pol_pri_drutrpla_pers_info', 'sfzh': 'cred_num', 'jsbh': 'drug_trea_place_no',
        'start_time': 'ente_stat_date',
         'end_time': 'out_pris_date', 'fjh': 'drug_trea_room_no', 'table_sort': 1},
        {'tablename': 'ods_pol_pri_drutre_outper_info', 'sfzh': 'cert_no','jsbh':'priso_no',
         'start_time': 0,
         'end_time': 'OUT_PRIS_DATE', 'fjh': 'out_pris_time_room_no',
         'table_sort': 2},
        {'tablename': 'ods_pol_pri_drcper_rooadj', 'sfzh': 'cred_num',
         'jsbh': 'drug_trea_place_no','start_time': 0,
         'end_time': 0, 'fjh': '""', 'table_sort': 3},
        {'tablename': 'ods_pol_pri_drcent_heaexa', 'sfzh': 'cert_no', 'jsbh': 'drug_trea_place_no',
         'start_time': 'check_time', 'end_time': 0, 'fjh': '""',
         'table_sort': 4},
    ]

    tmp_sql = '''
                select format_zjhm({sfzh}) sfzh, format_data({jsbh}) jsbh, valid_datetime({start_time}) start_time,
                 valid_datetime({end_time}) end_time,trim({fjh}) fjh, '{tablename}' tablename, {table_sort} table_sort
                from {tablename} where verify_sfz({sfzh}) = 1 and format_data({jsbh}) != ''
            '''

    union_df = create_uniondf(spark,table_list, tmp_sql)
    res = union_df.selectExpr("*", "row_number() over (partition by sfzh,jsbh order by jsbh asc) num") \
        .where('num=1').drop('num').drop('table_sort')

    write_orc(res, add_save_path('edge_person_stay_jds'))
    logger.info('edge_person_stay_jds down')

def vertex_jail():
    '''
    监所节点信息 编号，名称
    上述所有表的监所信息提取
    :return:
    '''
    table_list = [
        {'tablename':'ods_pol_pri_priper_rooadj','unitid':'detsta_no','unitname':'detsta_desig','unittype':u'拘留所'},
        {'tablename':'ods_pol_pri_prient_heaexa','unitid':'detsta_no','unitname':'detsta_desig','unittype':u'拘留所'},
        {'tablename':'ods_pol_pri_jaiper_rooadj','unitid':'deten_hou_no','unitname':'deten_hou_desig','unittype':u'看守所'},
        {'tablename':'ods_pol_pri_jaient_heaexa','unitid':'deten_hou_no','unitname':'deten_hou_desig','unittype':u'看守所'},
        {'tablename':'ods_pol_pri_hist_detes_deares','unitid':'deten_hou_no','unitname':'deten_hou_desig','unittype':u'看守所'},
        {'tablename':'ods_pol_pri_drutrpla_pers_info','unitid':'drug_trea_place_no','unitname':'drug_trea_place_desig','unittype':u'戒毒所'},
        {'tablename':'ods_pol_pri_drutre_outper_info','unitid':'priso_no','unitname':'priso_desig','unittype':u'戒毒所'},
        {'tablename':'ods_pol_pri_drcper_rooadj','unitid':'drug_trea_place_no','unitname':'drug_trea_place_desig','unittype':u'戒毒所'},
        {'tablename':'ods_pol_pri_drcent_heaexa','unitid':'drug_trea_place_no','unitname':'drug_trea_place_desig','unittype':u'戒毒所'},
        {'tablename':'ods_pol_pri_detsta_outper_info','unitid':'deten_hou_no','unitname':'deten_hou_desig','unittype':u'看守所'},
        {'tablename':'ods_pol_pri_detesta_outper_info','unitid':'detsta_no','unitname':'detsta_desig','unittype':u'拘留所'},
        {'tablename':'ods_pol_crim_dethou_detes_info','unitid':'detsta_no','unitname':'detsta_desig','unittype':u'拘留所'},
        {'tablename':'ods_pol_pri_dethou_detees_info','unitid':'deten_hou_no','unitname':'deten_hou_desig','unittype':u'看守所'},
    ]

    tmp_sql = '''
                    select format_data({unitid}) unitid, format_data({unitname}) unitname,
                    '{unittype}' unittype, '{tablename}' tablename
                    from {tablename} where format_data({unitid}) != ''
                '''

    union_df = create_uniondf(spark,table_list,tmp_sql)

    res = union_df.drop_duplicates(['unitid'])

    write_orc(res,add_save_path('vertex_jail'))
    logger.info('vertex_jail down')


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    edge_person_stay_jds()
    edge_person_stay_jls()
    edge_person_stay_kss()
    vertex_jail()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))