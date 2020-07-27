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
import time, copy, re, math,random
from datetime import datetime, timedelta,date
import logging
from collections import OrderedDict
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')
#warehouse_location = '/user/hive/warehouse/'
conf=SparkConf().set('spark.driver.maxResultSize', '2g')
conf.set('spark.yarn.executor.memoryOverhead', '10g')
conf.set('spark.yarn.am.cores', 4)
conf.set('spark.executor.memory', '20g')
conf.set('spark.executor.instances', 10)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commonUtil = CommonUdf(spark)

save_root = 'node_lables'

lable_values = {
    'always_use_airline':'22',
    'high_education':'21',
    'oversea_call':'20',
    'frequent_change_phonedevice':'19',
    'with_muti_device':'18',
    'only_send_msg':'17',
    'with_muti_cards':'16',
    'delivery_abnormal':'15',
    'frequent_change_cards':'14',
    'day_in_nit_out':'13',
    'huge_start_call_end_call':'12',
    'unsusual_startup_shutdown':'10',
    'huge_start_call':'9',
    'by_day_by_night':'1',
    'abnormal_communication':'2',
    'cross_regional':'3',
    'many_connected ':'4',
    'frequent_move':'5',
    'homeless':'6',
    'sensitive_call':'7',
    'frequent_nig_call':'8',
}


phone_label_files = {
    ## 大量主叫
    'huge_start_call': ['start_phone nodenum', 'call_cnt value', 'valid_datetime(date) date'],
    # 频繁开关机 cnt 次数
    'unsusual_startup_shutdown': ['phone nodenum', 'cnt value','9 date'],
    # 跨区域 geocnt 区域个数
    'cross_regional': ['phone nodenum', 'geocnt value', 'valid_datetime(date) date'],
    # 大量主叫 被叫 次数
    'huge_start_call_end_call': ['phone nodenum', 'concat(start_call_cnt,"|",end_call_cnt) value', 'valid_datetime(date) date'],
    # zhoufuyechu 天数
    'day_in_nit_out': ['phone nodenum', 'll value', 'valid_datetime(date) date'],
    # 大量收快递 delivery_cnt 一天次数
    'delivery_abnormal': ['phone nodenum', 'delivery_cnt value', 'valid_datetime(date) date'],
    # 只发短信
    'only_send_msg': ['phone nodenum','"" value','9 date'],
    # 一号多机
    'with_muti_device': ['phone nodenum', 'mutidevice_num value', 'valid_datetime(date) date'],
    # 频繁换机
    'frequent_change_phonedevice': ['phone nodenum', 'mutidevice_num value', 'valid_datetime(date) date'],
    # 海外通联
    'oversea_call': ['phone nodenum', 'cnt value','9 date']
}

person_label_files = {
    'high_education': ['zjhm nodenum', 'whcd value','9 date'],
     # 空中飞人
    'always_use_airline': ['sfzh nodenum', 'cnt value','9 date'],
}

imei_label_files = {
    # 一机多卡
    'with_muti_cards': ['imei nodenum', 'mutiphone_num value', 'valid_datetime(date) date'],
    # 频繁换卡
    'frequent_change_cards': ['imei nodenum', 'mutiphone_num value', 'valid_datetime(date) date'],
}

def add_lable_path(file):
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/investlabel/%s/*'%file

def get_labels(files,label):
    dfs = []
    for file, cols in files.items():
        logger.info('dealing %s'%file)
        df = read_orc(spark,add_lable_path(file)).selectExpr(*cols)
        if df and df.take(1):
            df = df.withColumn('tagid',lit(lable_values.get(file)))
            df = df.withColumn('label',lit(label))
            dfs.append(df)
    if dfs:
        res = reduce(lambda a,b:a.unionAll(b),dfs)
        return res


def format_labels():
    person_label = get_labels(person_label_files,'person')
    phone_label = get_labels(phone_label_files,'phone')
    imei_label = get_labels(imei_label_files,'imei')
    write_orc(person_label,add_save_path('person_labels',root=save_root))
    write_orc(phone_label,add_save_path('phone_labels',root=save_root))
    write_orc(imei_label,add_save_path('imei_labels',root=save_root))


if __name__ == "__main__":
    ''' 格式化算法标签数据 '''
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    format_labels()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))