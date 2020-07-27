# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 14:17
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : case.py
# @Software: PyCharm
# @content : 警情案件相关信息

import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession,DataFrame
# from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time, copy, re, math
from datetime import datetime, timedelta, date
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
reload(sys)
sys.setdefaultencoding('utf-8')
conf = SparkConf().set('spark.driver.maxResultSize', '10g')
# conf.set('hive.metasotre.uris','thrift://24.1.11.2:10005')
# conf.set('hive.server2.enable.doAs','false')
# conf.set('hive.server2.thrift.port',10000)
# conf.set('spark.yarn.executor.memoryOverhead', '30g')
# conf.set('spark.yarn.am.cores', 5)
# conf.set('spark.executor.memory', '2g')
# conf.set('spark.executor.instances', 50)
# conf.set('spark.executor.cores', 8)
# conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commUtil = CommonUdf(spark)
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
    pass


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
    pass


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
    '''
    pass


def edge_person_call_case():
    '''
    1、ODS_POL_CRIM_ENFO_CASE_INFO	案件基本信息
        REPORTER_CERT_NO	报案人_公民身份号码
        CASE_NO	案件编号
        CASE_NAME	案件名称
        REPO_CASE_TIME	报案时间


    :return:
    '''


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
    pass

def col_filter(row,indices):
    cols = row.split('\t')
    if len(cols) < max(indices) + 1:
        return
    tmp = [cols[_] for _ in indices]
    return tmp

def add_path(tablename,import_time='*'):
    ##源文件地址
    tmp_path = '/phoebus/_fileservice/users/zhjw/sjzl/daml/data/ods/{}/import/{}/*'
    tmp_path2 = '/phoebus/_fileservice/users/jz/jizhen/daml/data/ods/{}/import/{}/*'
    if 'dw_evt' in tablename:
        return tmp_path2.format(tablename.upper(),import_time)
    return tmp_path.format(tablename.upper(),import_time)

def add_save_path(tablename,cp='2020'):
    ## hive外表保存地址
    tmp_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/{}/cp={}'
    return tmp_path.format(tablename.lower(),cp)

## 分割后所需字段的索引值
table_indices = {
        'ods_gov_edu_stud_info':[6,10,11,21,53,54,60,61,63,66,67,69],
        'ods_pol_pub_dw_evt':[12, 14, 15, 16, 17, 18, 19, 22, 23, 24, 25, 38, 44, 45, 46]

    }

## 创建hive表的schema信息
table_schema = {
    'ods_gov_edu_stud_info': ['coll_time','name','fur_name','cred_num', 'guar_name', 'guar_cert_no', 'father_name',
                              'father_cert_no', 'father_pred_addr_name', 'mother_name', 'mother_cert_no',
                              'mother_pred_addr_name'],
    'ods_pol_pub_dw_evt':['capture_time', 'user_num', 'user_homloc_ctct_tel_arcod', 'oppos_num', 'oppos_homloc_ctct_tel_arcod',
                            'user_imsi', 'user_imei', 'loc_area_code', 'bassta_houest_idecod', 'bassta_lon', 'bassta_lat',
                            'bassta_geohash', 'norma_bassta_lon', 'norma_bassta_lat', 'norma_bassta_geohash']
}


def test():
    tablenames = ['ods_pol_pub_dw_evt']
    for tablename in tablenames:
        for thistime in thistimes:
            filepath = add_path(tablename,import_time=thistime)
            indices = table_indices[tablename]
            schema = table_schema[tablename]
            save_path = add_save_path(tablename,cp=str(udf_timestamp2cp_int(thistime,format='%Y%m%d%H%M%S')))
            print(filepath,save_path)
            df = spark.sparkContext.textFile(filepath) \
                    .map(lambda b: b.split('\t')) \
                    .map(lambda row: col_filter(row,indices)) \
                    .toDF(schema)
            df.write.format('orc').mode('overwrite').save(save_path)


def init(tablename):
    filepath = add_path(tablename)
    indices = table_indices[tablename]
    schema = table_schema[tablename]
    # save_path = add_save_path(tablename)
    df = spark.sparkContext.textFile(filepath) \
        .map(lambda row: col_filter(row, indices)) \
        .filter(lambda v : v) \
        .toDF(schema)
    df.createOrReplaceTempView(tablename)



if __name__ == "__main__":
    logger.info('tttt')
    thistimes = sys.argv[1].split(',')
    tablename = 'ods_gov_edu_stud_info'
    test()
    # init(tablename)
    # spark.sql('select * from ods_gov_edu_stud_info').show()