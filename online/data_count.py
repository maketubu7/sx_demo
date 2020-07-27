# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 19:01
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : data_count.py
# @Software: PyCharm
# @content : 统计每个表的数量
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import os,sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')


conf=SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '15g')
conf.set('spark.executor.instances', 30)
conf.set('spark.executor.cores', 8)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from jg_info import vertex_table_info,edge_info

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/open_phone'

def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix,'parquet/',path))
    return df
def write_parquet(df,path,method='overwrite'):
    '''写 parquet'''
    df.write.mode(method).parquet(path=os.path.join(path_prefix,'parquet/',path))

def write_csv(df, path, header=False, delimiter='\t'):
    '''写csv文件'''
    df.write.mode('overwrite').csv(os.path.join(path_prefix, 'csv/', path), header=header, sep=delimiter, quote='"',escape='"')


def write_orc(df, path,mode='overwrite'):
    df.write.format('orc').mode(mode).save(path)
    logger.info('write success')


def read_orc(path):
    df = spark.read.orc(path)
    return df

def write_jdbc(df,tablename,prop,mode='overwrite'):
    df.write.jdbc(url=prop['url'],mode=mode,table=tablename,properties=prop)

def add_save_path(tablename, cp=''):
    ## hive外表保存地址
    if cp:
        tmp_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/{}/cp={}'
        return tmp_path.format(tablename.lower(), cp)
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/{}/cp=2020'.format(tablename.lower())

detail_info = ['edge_groupcall_detail','edge_groupmsg_detail','edge_person_stay_hotel_detail']

def deal_table_count():
    '''
    统计每种节点关系数量
    '''
    rdds = []
    for tablename in vertex_table_info:
        df = read_orc(add_save_path(tablename))
        count = df.count()
        logger.info(tablename,count)
        rdds.append([tablename,count])
    for tablename in edge_info:
        df = read_orc(add_save_path(tablename))
        count = df.count()
        logger.info(tablename, count)
        rdds.append([tablename,count])
    for tablename in detail_info:
        df = read_orc(add_save_path(tablename))
        count = df.count()
        logger.info(tablename, count)
        rdds.append([tablename, count])
    df = spark.createDataFrame(rdds,['tablename','num'])
    table_name = 'tablecount'+time.strftime("%Y%m%d", time.localtime())
    write_jdbc(df,table_name,prop)

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
        "db_name": "graphspace_sx",
        "mode": 'overwrite'}

    deal_table_count()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))