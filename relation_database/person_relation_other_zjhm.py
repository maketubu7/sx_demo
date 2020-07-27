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
from datetime import datetime, timedelta, date
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')
# warehouse_location = '/user/hive/warehouse/'
conf = SparkConf().set('spark.driver.maxResultSize', '2g')
conf.set('spark.yarn.executor.memoryOverhead', '30g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '2g')
conf.set('spark.executor.instances', 10)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')
# conf.set("spark.sql.warehouse.dir", warehouse_location)


spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commonUtil = CommonUdf(spark)

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'

def person_relate_other_zjhm():
    # cert_type 证件类型
    # cert_num 证件号码
    # other_cert_type 其它证件类型
    # other_cert_num 其它证件号码
    # rel_type 最新关系类型
    # last_dis_place 最新发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    # totle_count 累计发现次数
    # totle_days 累计发现天数
    init(spark, 'nb_app_dws_per_res_his_dcothercard', if_write=False, is_relate=True)
    sql = '''
        select cert_num sfzh, other_cert_type other_type, md5(concat(other_cert_type,other_cert_num)) certificateid, cast(first_time as bigint)  start_time,
        cast(last_time as bigint) end_time from nb_app_dws_per_res_his_dcothercard where verify_sfz(cert_num) = 1 
        and other_cert_num != '' and other_cert_num is not null
    '''

    df = spark.sql(sql)
    cols = ['sfzh','certificateid','mc certificate_type','if(start_time>0,start_time,0) start_time','if(end_time>0,end_time,0) end_time']
    order_type_dm = get_all_dict(spark).where('lb="other_zjhm_type"')
    res = df.join(order_type_dm, df['other_type'] == order_type_dm['dm'], 'left') \
        .selectExpr(*cols).na.fill({"certificate_type": u'其他'}).dropDuplicates(['sfzh','certificateid'])

    write_orc(res, add_save_path('edge_person_link_certificate', root=save_root))

    v_sql = '''
        select md5(concat(other_cert_type,other_cert_num)) certificateid, other_cert_num certificate, 
        other_cert_type certificate_type from nb_app_dws_per_res_his_dcothercard 
        where other_cert_num != '' and other_cert_num is not null
    '''
    cols = ['certificateid','certificate','mc certificate_type']
    v_df = spark.sql(v_sql).dropDuplicates(['certificateid'])
    v_df = v_df.join(order_type_dm,v_df['certificate_type'] == order_type_dm['dm'],'left') \
            .selectExpr(*cols).na.fill({"certificate_type": u'其他'}).dropDuplicates(['certificateid'])
    write_orc(v_df,add_save_path('vertex_certificate',root=save_root))


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    person_relate_other_zjhm()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))