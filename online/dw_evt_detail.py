# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 14:54
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : train.py
# @Software: PyCharm
# @content : 航班相关信息

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
conf=SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.executor.memoryOverhead', '10g')
# conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '20g')
conf.set('soark.shuffle.partitions',800)
conf.set('spark.executor.instances', 100)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')


spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()
from common import *
commonUtil = CommonUdf(spark)

##todo:all


path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/train'
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

##todo: 入库历史文件 0313开始
def get_dw_detail():
    '''
    得到某一天的全部明细数据
    :param times: [time1,time2] 每一天的全部导入时间戳
    :param cp: 2019081000 每天的格式化分区
    :return:
    '''
    init_dw_history(spark,'ods_pol_pub_dw_evt',import_times=import_times,if_write=False)
    # read_orc(add_save_path(tablename, cp=cp, root='midfile')).createOrReplaceTempView('ods_pol_pub_dw_evt')
    sql = '''
                select format_phone(user_num) phone, trim(user_imsi) imsi, trim(user_imei) imei,
                trim(norma_bassta_lon) lon,  trim(norma_bassta_lat) lat, trim(norma_bassta_geohash) geohash,
                trim(elefen_event_type_code) event_type, capture_time
                from ods_pol_pub_dw_evt where capture_time is not null and verify_cell_phone(user_num) = 1
                and trim(norma_bassta_lat) != '' and trim(norma_bassta_lon) != '' and trim(capture_time) != ''
            '''
    # df = spark.sql(sql).drop_duplicates(['phone','capture_time']).repartition(300)
    df = spark.sql(sql)

    save_path = add_incr_path('bbd_dw_detail',cp)
    write_orc(df,save_path)
    logger.info('bbd_dw_detail %s down'%cp)


def find_zp():
    create_tmpview_table(spark,'bbd_dw_detail_tmp')
    sql = '''
        select * from bbd_dw_detail_tmp where phone in ('17110164410', '17110164412', '17110164413', '17110164414', '17110164415', '17110164416', '17110164417', '17110164418', '17110164419', '17110164420', '17110164421', '17110164423', '17110164424', '17110164425', '17110164426', '17110164427', '17110164428', '17110164429', '17110164430', '17110164431', '17110164432', '17110164434', '17110164436', '17110164437', '17110164438', '17110164439', '17110164440', '17110164441', '17110164442', '17110169741', '15222780193', '18822729741', '15922275197', '18822526237', '18222727359', '18722480482', '18722222930', '18322731867', '15222386359', '18702204575', '15222817992', '18722569197')
    '''
    df = spark.sql(sql)
    write_orc(df,add_save_path('zp_res'))

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    if len(sys.argv) == 4:
        import_times = sys.argv[1].split(',')
        cp = str(sys.argv[2])
        tablename = str(sys.argv[3])
        get_dw_detail()
    else:
        pass


    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))