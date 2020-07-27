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
import json
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')
#warehouse_location = '/user/hive/warehouse/'
conf=SparkConf().set('spark.driver.maxResultSize', '20g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memoryOverhead', '5g')
conf.set('spark.executor.memory', '15g')
conf.set('spark.executor.instances', 100)
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
    path= '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/zp_data/'+file+'.csv'
    return spark.read.csv(path, schema=schema,header=False)

def is_wee_hour(timestamp):
    h = time.localtime(timestamp).tm_hour
    return 1 if h >= 1 and h <= 5 else 0

spark.udf.register('is_wee_hour',is_wee_hour,IntegerType())

country_file = [u'杨暴村',u'南津良村',u'北津良村']

def find_phone():
    like_cond = '''  hkszdxz like '%杨暴村%' or hkszdxz like '%南津良村%' or hkszdxz like '%北津良村%'  '''
    df = read_orc(add_save_path('vertex_person'))
    op = df.where(like_cond).select('zjhm')
    link_phone = read_orc(add_save_path('edge_person_open_phone')).where('is_xiaohu="否"')

    res = op.join(link_phone,op['zjhm']==link_phone['sfzh'],'inner') \
            .selectExpr('zjhm','phone','start_time','end_time')
    res.persist()
    write_orc(res,add_save_path('steal_group_open_phone',root='wa_data'))

    all_phone = res.select('phone').dropDuplicates()
    intersect_call = group_call_intersect(all_phone)
    write_orc(intersect_call,add_save_path('steal_group_intersect_call',root='wa_data'))

    wee_hour_call = intersect_call.where('is_wee_hour(start_time)=1')
    write_orc(wee_hour_call,add_save_path('steal_group_wee_call',root='wa_data'))

def group_call_detail_intersect(phone):

    phone.createOrReplaceTempView('v_phone')
    create_tmpview_table(spark,'edge_groupcall_detail')

    sql = '''
        select a.start_phone, a.end_phone, start_time, end_time, call_duration from edge_groupcall_detail a
        inner join 
        v_phone b
        inner join 
        v_phone c
        on a.start_phone=b.phone and a.end_phone=c.phone
    '''

    res = spark.sql(sql)
    return res

def group_call_intersect(phone):
    phone.createOrReplaceTempView('v_phone')
    create_tmpview_table(spark, 'edge_groupcall')

    sql = '''
        select a.start_phone, a.end_phone, start_time, end_time, call_total_duration, call_total_times
        from edge_groupcall a
        inner join 
        v_phone b
        inner join 
        v_phone c
        on a.start_phone=b.phone and a.end_phone=c.phone
    '''

    res = spark.sql(sql)
    return res
# write_orc(res,add_save_path('sd_intersect_call',root='wa_data'))

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    find_phone()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))