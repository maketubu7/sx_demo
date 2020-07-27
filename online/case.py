# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 14:17
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : case.py
# @Software: PyCharm
# @content : 警情案件相关信息

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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

warehouse_location = '/user/hive/warehouse/'
conf=SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '20g')
conf.set('spark.executor.instances', 30)
conf.set('spark.executor.cores', 8)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')
conf.set("spark.sql.warehouse.dir", warehouse_location)

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commonUtil = CommonUdf(spark)
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/call_msg'

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

def add_save_path(tablename, cp=''):
    ## hive外表保存地址
    if cp:
        tmp_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/{}/cp={}'
        return tmp_path.format(tablename.lower(), cp)
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/{}/cp=2020'.format(tablename.lower())

def create_tmpview_table(tablename,cp=''):
    '''
    读取所有内容
    :param tablename:
    :param cp: cp='' 读取所有内容
    :return:
    '''
    tmp_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/{}/*'
    if not cp:
        df = read_orc(tmp_path.format(tablename))
        df.createOrReplaceTempView(tablename)
    else:
        df = read_orc(add_save_path(tablename, cp))
        df.createOrReplaceTempView(tablename)


def create_uniondf(table_list,tmp_sql):
    dfs = []
    for info in table_list:
        sql = tmp_sql.format(**info)
        init(spark, info['tablename'], if_write=False)
        df = spark.sql(sql)
        dfs.append(df)
    union_df = reduce(lambda x,y:x.unionAll(y),dfs)
    return union_df
##todo:all 升级没得数据

def vertex_alert():
    '''
    警情节点
    1、ODS_POL_CRIM_ENFCAS_POL_INFO	警情基本信息
        POL_CASE_NO	警情编号
        CASE_NO	案件编号
        POL_CASE_TCODE	警情类别代码
        POL_CASE_TNAME	警情类别名称
        POL_NATU	警情性质
        BRIEF_COND	简要情况
        ACCE_ALARM_NO	接警编号
        NEW_ACCE_ALARM_NO	新_接警编号
        ACCE_ALARM_PERS_POL_NUM	接警人_警号
        ACCE_ALARM_TIME	接警时间
        PRPOL_RESU_BRIEF_COND	处警结果_简要情况
    2、ODS_POL_CRIM_ENFO_CASE_INFO	案件基本信息
        POL_CASE_NO	警情编号
        CASE_PER_POL_NUM	办案人_警号
        CASE_PER_CTCT_WAY	办案人_联系方式
        REPO_CASE_TIME	报案时间
        BRIEF_CASE	简要案情
    :return:
    '''

    ##todo 警情数据暂时没有
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

    union_df = create_uniondf(table_list,tmp_sql)

    res = union_df.selectExpr('*','row_number() over (partition by jqbh order by table_sort asc) num') \
                        .where('num=1').drop('num').drop('table_sort')

    write_orc(res,add_save_path('vertex_alert'))
    logger.info('vertex_alert down')


def edge_person_call_alert():
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

    write_orc(df,add_save_path('edge_phone_call_alert'))
    logger.info('edge_phone_call_alert down')


def vertex_case():
    '''
    1、ODS_POL_CRIM_ENFCAS_POL_INFO	警情基本信息
        CASE_SNO 案件序号
        CASE_NO 案件编号
        CASE_NATU_CODE	案件性质代码
        CASE_NATU_NAME	案件性质名称
        CASE_LEVE_CODE	案件级别代码
        CASE_LEVE_NAME	案件级别名称
        BRIEF_COND	简要情况
        ACCE_ALARM_TIME	接警时间
    2、ODS_POL_CRIM_ENFO_CASE_INFO	案件基本信息
        CASE_NO	案件编号
        POL_CASE_NO	警情编号
        CASE_NAME	案件名称
        CASE_TYPE_CODE	案件类别代码
        CASCLA_NAME	案件类别名称
        CASE_SOUR_DESC	案件来源描述
        ACCEP_TIME	受理时间
        BRIEF_CASE	简要案情
        REPO_CASE_TIME	报案时间
        CAUS_BRIEF_COND	事由_简要情况
    :return:

    accep_time disc_time timestamp
    detect_date set_laws_date format_date
    '''
    table_list = [
        {'asjbh': 'case_no', 'ajmc': '""', 'asjfskssj': '""', 'asjfsjssj': '""', 'asjly': 'case_natu_name',
         'ajlb': '""', 'fxasjsj': 'acce_alarm_time', 'fxasjdd_dzmc': 'pol_case_occu_plac_addr_name',
         'jyaq': 'brief_cond', 'ajbs': '""', 'larq': '""',
         'tablename': 'ods_pol_crim_enfcas_pol_info','table_sort':2},
        {'asjbh': 'case_no', 'ajmc': 'case_name', 'asjfskssj': 'coalesce_str(accep_time,disc_time)',
         'asjfsjssj': 'coalesce_str(detect_date,set_laws_date)', 'asjly': 'case_sour_desc',
         'ajlb': 'cascla_name', 'fxasjsj': 'coalesce_str(accep_time,disc_time)',
         'fxasjdd_dzmc': 'coalesce_str(disc_loca_addr_name,case_addr_addr_name)',
         'jyaq': 'brief_case', 'ajbs': '""', 'larq': 'case_date',
         'tablename': 'ods_pol_crim_enfo_case_info','table_sort':1},
    ]

    tmp_sql = '''
            select format_data({asjbh}) asjbh, {ajmc} ajmc, cast(format_timestamp({asjfskssj}) as bigint) asjfskssj,
            cast(valid_datetime({asjfsjssj}) as bigint) asjfsjssj, {asjly} asjly, {ajlb} ajlb,
            cast(format_timestamp({fxasjsj}) as bigint) fxasjsj, {fxasjdd_dzmc} fxasjdd_dzmc, {jyaq} jyaq,
            {ajbs} ajbs, cast(valid_datetime({larq}) as bigint) larq, '{tablename}' tablename,{table_sort} table_sort
            from {tablename} where format_data({asjbh}) != ''
        '''

    union_df = create_uniondf(table_list,tmp_sql)

    res = union_df.selectExpr('*','row_number() over (partition by asjbh order by table_sort asc) as num') \
                .where('num=1').drop('num').drop('table_sort')

    write_orc(res,add_save_path('vertex_case'))
    logger.info('vertex_case down')


def edge_person_call_case():
    '''
    1、ODS_POL_CRIM_ENFO_CASE_INFO	案件基本信息
        REPORTER_CERT_NO	报案人_公民身份号码
        CASE_NO	案件编号
        CASE_NAME	案件名称
        REPO_CASE_TIME	报案时间
    :return:
    '''

    sql = '''
        select format_zjhm(reporter_cert_no) sfzh, format_data(case_no) asjbh,
        cast(format_timestamp(coalesce_str(accep_time,disc_time)) as bigint) start_time,
        cast(valid_datetime(coalesce_str(detect_date,set_laws_date)) as bigint) end_time,
        'ods_pol_crim_enfo_case_info' tablename
        from ods_pol_crim_enfo_case_info
        where verify_sfz(reporter_cert_no) = 1 and format_data(case_no) != ''
    '''

    init(spark,'ods_pol_crim_enfo_case_info',if_write=False)
    df = spark.sql(sql)
    df = df.drop_duplicates(['sfzh','asjbh'])
    write_orc(df,add_save_path('edge_case_reportby_person'))

    logger.info('edge_case_reportby_person down')


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
    write_orc(df,add_save_path('edge_alert_link_case'))

    logger.info("edge_alert_link_case down")


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    vertex_alert()
    vertex_case()
    edge_person_call_alert()
    edge_person_call_case()
    edge_alert_link_case()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))