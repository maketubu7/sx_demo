# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : qq_msg.py
# @Software: PyCharm
# @content : qq相关信息

import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession
# from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time, copy, re, math
from datetime import datetime, timedelta,date
import argparse
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')
#warehouse_location = '/user/hive/warehouse/'
conf=SparkConf().set('spark.driver.maxResultSize', '20g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '20g')
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

spark.sparkContext.addPyFile('hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/graphframes.zip')
from graphframes import *

save_path = 'wa_data/kill_case'
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/qq'

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

def read_csv(file,schema):
    path= '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/kill_case/'+file
    return spark.read.csv(path, schema=schema,header=False)

def write_jdbc(df,tablename,prop,mode='overwrite'):
    df.write.jdbc(url=prop['url'],mode=mode,table=tablename,properties=prop)

def degree_filter(d_num):

    create_tmpview_table(spark,'edge_groupcall')
    sql = 'select start_phone src, end_phone dst, "call" relationship, start_time, end_time, call_total_duration,call_total_times from edge_groupcall'
    edges = spark.sql(sql)

    vertices = edges.selectExpr('src id').union(edges.selectExpr('dst id'))

    g = GraphFrame(vertices,edges)
    res = g.degrees.sort(desc('degree')).limit(d_num)
    res.write.csv('/phoebus/_fileservice/users/slmp/shulianmingpin/service_num')




if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    prop = {
        "url": 'jdbc:mysql://24.2.26.44:3307/graphspace_sx?characterEncoding=UTF-8',
        "user": "bbd",
        "password": "bbdtnb",
        "driver": "com.mysql.jdbc.Driver",
        "ip": "24.2.26.44",
        "port": "3307",
        "db_name":"graphspace_sx",
        "mode": 'overwrite'}
    degree_filter(5000)

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))