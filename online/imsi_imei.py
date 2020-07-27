# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : imsi_imei.py
# @Software: PyCharm
# @content : 设备码相关信息

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
conf=SparkConf().set('spark.driver.maxResultSize', '20g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '10g')
conf.set('spark.executor.instances', 40)
conf.set('spark.executor.cores', 8)
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
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
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

def edge_phonenumber_use_imsi():
    create_tmpview_table(spark,'bbd_dw_detail_tmp')
    sql = '''
        select phone, imsi,min(cast(capture_time as bigint)) start_time,
        max(cast(capture_time as bigint)) end_time from bbd_dw_detail_tmp
        where verify_phonenumber(phone) =1 and imsi != ''
        group by phone, imsi
    '''

    df = spark.sql(sql)
    write_orc(df,add_save_path('edge_phonenumber_use_imsi'))
    logger.info('edge_phonenumber_use_imsi down')

def edge_phonenumber_use_imei():
    create_tmpview_table(spark,'bbd_dw_detail_tmp')
    sql = '''
            select phone, imei,min(cast(capture_time as bigint)) start_time,
            max(cast(capture_time as bigint)) end_time from bbd_dw_detail_tmp
            where verify_phonenumber(phone) =1 and imei != ''
            group by phone, imei
        '''

    df = spark.sql(sql)
    write_orc(df, add_save_path('edge_phonenumber_use_imei'))
    logger.info('edge_phonenumber_use_imei down')

def vertex_imsi_imei():
    create_tmpview_table(spark,'edge_phonenumber_use_imei')
    imei = spark.sql('select imei from edge_phonenumber_use_imei').dropDuplicates()
    write_orc(imei,add_save_path('vertex_imei'))

    create_tmpview_table(spark,'edge_phonenumber_use_imsi')

    imei = spark.sql('select imsi from edge_phonenumber_use_imsi').dropDuplicates()
    write_orc(imei, add_save_path('vertex_imsi'))

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    edge_phonenumber_use_imsi()
    edge_phonenumber_use_imei()
    vertex_imsi_imei()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))