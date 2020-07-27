# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 15:35
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : car_etl.py
# @Software: PyCharm

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.rdd import RDD
import logging
from pyspark import SparkConf
import socket
import os,sys

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

conf=SparkConf().set('spark.driver.maxResultSize', '15g')
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

from common import *
commonUtil = CommonUdf(spark)
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/car'

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

def vertex_autolpn():
    sql = '''
        select valid_hphm(ssjdc_jdchphm,ssjdc_jdchpzldm) autolpn,
        format_data(ssjdc_jdccsys_jdccsysmc) hpzl,
        'ods_bdq_tb_zy_qgbdqjdcdjxx' tablename from
        ods_bdq_tb_zy_qgbdqjdcdjxx where verify_hphm(ssjdc_jdchphm) = 1
    '''

    init(spark,'ods_bdq_tb_zy_qgbdqjdcdjxx',if_write=False)
    df = spark.sql(sql).drop_duplicates(['autolpn'])

    write_orc(df,add_save_path('vertex_autolpn'))
    logger.info('vertex_autolpn down')

def vertex_vehicle():
    sql = '''
            select valid_vehicle_id(ssjdc_clsbdh) vehicle,
            format_data(ssjdc_jdcfdjddjxh) fdjh,
            format_data(ssjdc_jdccllxmc) cllx,
            format_data(ssjdc_jdccsys_jdccsysmc) csys,
            'ods_bdq_tb_zy_qgbdqjdcdjxx' tablename from
            ods_bdq_tb_zy_qgbdqjdcdjxx where trim(ssjdc_clsbdh) != ''
        '''

    init(spark, 'ods_bdq_tb_zy_qgbdqjdcdjxx', if_write=False)
    df = spark.sql(sql).drop_duplicates(['vehicle'])

    write_orc(df, add_save_path('vertex_vehicle'))
    logger.info('vertex_vehicle down')


def edge_person_own_car():

    sql = '''
        select format_zjhm(ssjdc_soyr_zjhm) sfzh, valid_hphm(ssjdc_jdchphm,ssjdc_jdchpzldm) autolpn,
        'ods_bdq_tb_zy_qgbdqjdcdjxx' tablename from ods_bdq_tb_zy_qgbdqjdcdjxx
        where verify_sfz(ssjdc_soyr_zjhm) = 1 and verify_hphm(ssjdc_jdchphm) = 1
    '''

    init(spark,'ods_bdq_tb_zy_qgbdqjdcdjxx',if_write=False)
    df = spark.sql(sql).drop_duplicates(['sfzh','autolpn'])

    write_orc(df,add_save_path('edge_person_own_autolpn'))
    logger.info('edge_person_own_autolpn down')



if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    vertex_autolpn()
    vertex_vehicle()
    edge_person_own_car()


    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
