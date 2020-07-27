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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/orderid'
save_root = 'relation_theme_extenddir'

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

def person_relate_orderid():
    # cert_type 证件类型
    # cert_num 证件号码
    # order_no 订单号
    # order_type 订单类型
    # order_time 下单时间
    # app_type 应用类型
    # user_name 用户账号
    # user_id 用户ID
    # last_dis_place 最后发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    # totle_count 累计发现次数
    # totle_days 累计发现天数

    ## 人关联订单 todo：暂无数据
    init(spark, 'nb_app_dws_per_res_his_dcorder', if_write=False, is_relate=True)
    sql = '''
        select cert_num sfzh, concat_ws('-',order_no,order_type) orderid, order_type,
        cast(order_time as bigint) order_time,app_type, user_name from nb_app_dws_per_res_his_dcorder 
        where verify_sfz(cert_num) = 1 and order_no != '' and order_no is not null
    '''
    df = spark.sql(sql).dropDuplicates(['sfzh','orderid'])
    cols = ['sfzh','orderid','mc order_type','order_time','app_type','user_name']
    order_type_dm = get_all_dict(spark).where('lb="order_type"')
    res = df.join(order_type_dm,df['order_type']==order_type_dm['dm'],'left') \
        .selectExpr(*cols).na.fill({"order_type":u'其他'})

    write_orc(res,add_save_path('edge_person_link_orderid',root=save_root))


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    person_relate_orderid()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

