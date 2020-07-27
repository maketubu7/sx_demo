# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 11:49
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : school.py
# @Software: PyCharm
# @content : 学校相关信息，上学信息

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

##todo:all

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/school'

def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix,'parquet/',path))
    return df

def write_parquet(df,path):
    '''写 parquet'''
    df.write.mode("overwrite").parquet(path=os.path.join(path_prefix,'parquet/',path))

def write_orc(df, path):
    df.write.format('orc').mode('overwrite').save(path)
    logger.info('write success')

def read_orc(path):
    df = spark.read.orc(path)
    return df

def vertex_school():
    '''
    学校节点信息
    1、ODS_GOV_EDU_KINDERGARTEN_STU	幼儿园学生信息
        KINDERGARTEN_DESIG	幼儿园_名称
        KINDERGARTEN_ADDR_NAME	幼儿园_地址名称
    2、ODS_GOV_EDU_SCHO_INFO	学校信息
        SCHO_NO	学校编号
        SCHO_NAME	学校名称
        SCHO_LEVE	学校等级
        SCHO_NATU	学校性质
        SCHO_TYPE	学校类型描述
    3、ODS_GOV_EDU_STUD_INFO	大中小学生信息
        SCHO_NO	学校_编号
        SCHO_NAME	学校名称
        ACAD_DESIG	学院_名称
        SCHO_LEVE	学校等级
    :return:
    '''


    table_list = [
        {'tablename':'ods_gov_edu_kindergarten_stu','s_name':'kindergarten_desig','s_address':'kindergarten_addr_name',
         "s_his_name":'""','table_sort':3},
        {'tablename':'ods_gov_edu_scho_info','s_name':'scho_name','s_address':'scho_addr_name',
         "s_his_name":'his_scho_name','table_sort':1},
        {'tablename':'ods_gov_edu_stud_info','s_name':'scho_name','s_address':'""',"s_his_name":'""',
         'table_sort':2},
    ]

    tmp_sql = ''' 
        select md5(format_data({s_name})) school_id, format_data({s_name}) name, 
        {s_his_name} history_name, {s_address} school_address,
        '{tablename}' tablename, {table_sort} table_sort from {tablename}
        where verify_school({s_name}) = 1
    '''

    res = create_uniondf(spark,table_list,tmp_sql)
    df_res = res.selectExpr('school_id','name','history_name','school_address','tablename',
                'row_number() over (partition by school_id order by table_sort asc) num'). \
                where("num=1").drop("num").drop('table_sort')
    write_orc(df_res,add_save_path('vertex_school'))
    logger.info("deal vertex_school success!!")


def edge_person_attend_school():
    '''
    人上学信息
    1、ODS_GOV_EDU_KINDERGARTEN_STU	幼儿园学生信息
        CERT_NO	公民身份号码
        KINDERGARTEN_DESIG	幼儿园_名称
        KINDERGARTEN_ADDR_NAME	幼儿园_地址名称
    2、ODS_GOV_EDU_STUD_INFO	大中小学生信息
        CERT_NO	公民身份号码
        CRED_NUM	证件号码
        SCHO_NO	学校_编号
        SCHO_NAME	学校名称
        PROFE_NAME	专业代码
        PROF_NOUN	专业名称
        ENTE_SCHO_DATE	入学日期
    :return:
    '''
    table_list = [
        {'tablename':'ods_gov_edu_stud_info','zjhm':'cred_num','s_name':'scho_name','rx_year':'ente_scho_year',
         'rxsj':'ente_scho_date','bysj':'grad_date','xymc':'acad_desig','bj':'class','zymc':'prof_noun','xh':'stunum',
         'xz':'edusys','nj':'grade'},
        {'tablename': 'ods_gov_edu_kindergarten_stu', 'zjhm': 'cert_no', 's_name': 'kindergarten_desig',
         'rx_year':'""','rxsj':'""','bysj':'""','xymc': '""', 'bj': '""', 'zymc': '""', 'xh': '""', 'xz': '""', 'nj': '""'}
    ]

    tmp_sql = '''
        select format_zjhm({zjhm}) start_person,md5(format_data({s_name})) end_school_id,
        date2timestampstr({rxsj}) start_time, date2timestampstr({bysj}) end_time,trim({s_name}) xxmc,
        trim({xymc}) xymc, trim({bj}) bj, trim({zymc}) zymc, trim({xh}) xh, trim({xz}) xz,
        '{tablename}' tablename from {tablename} 
        where verify_sfz({zjhm}) = 1 and verify_school({s_name}) = 1
    '''

    union_df = create_uniondf(spark, table_list, tmp_sql)

    res = union_df.drop_duplicates(['start_person','end_school_id'])

    write_orc(res,add_save_path('edge_person_attend_school'))

    logger.info('edge_person_attend_school down')

def edge_schoolmate():
    '''
    同学推理
    前置 实名制，上学
    1、ODS_GOV_EDU_STUD_INFO	大中小学生信息
        CERT_NO	公民身份号码
        CRED_NUM	证件号码
        SCHO_NO	学校_编号
        SCHO_NAME	学校名称
        PROFE_NAME	专业代码
        PROF_NOUN	专业名称
        ENTE_SCHO_DATE	入学日期
    :return:
    '''
    pass

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    vertex_school()
    edge_person_attend_school()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))