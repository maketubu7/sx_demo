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

def person_relate_im_account():
    # cert_type 证件类型
    # cert_num 证件号码
    # domain 域名
    # vt_type 虚拟身份类型
    # user_acc 用户账号
    # rel_type 关系类型
    # last_dis_place 发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    # totle_count 累计发现次数
    # totle_days 累计发现天数
    init(spark, 'nb_app_dws_per_per_his_dcacc', if_write=False, is_relate=True)
    sql = '''
        select cert_num sfzh, user_acc user_account, domain, vt_type, rel_type type, cast(first_time as bigint) start_time,
        cast(last_time as bigint) end_time from nb_app_dws_per_per_his_dcacc where verify_sfz(cert_num) = 1
        and user_acc != '' and user_acc is not null
    '''

    resource = spark.sql(sql).dropDuplicates(['sfzh','user_account'])

    write_orc(resource.where('vt_type="1030036"').drop('vt_type'),add_save_path('edge_person_link_wechat',root=save_root))
    write_orc(resource.where('vt_type="1330001"').drop('vt_type'),add_save_path('edge_person_link_sinablog',root=save_root))
    write_orc(resource.where('vt_type="1030001"').drop('vt_type'),add_save_path('edge_person_link_qq',root=save_root))


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    person_relate_im_account()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

