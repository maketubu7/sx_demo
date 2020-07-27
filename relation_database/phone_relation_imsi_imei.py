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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'

def person_relate_imsi_imei():
    # mob 手机号
    # imei MEI
    # rel_type 关系类型
    # last_dis_place 发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    # totle_count 累计发现次数
    # totle_days 累计发现天数
    init(spark, 'nb_app_dws_res_res_his_mobileimei', if_write=False, is_relate=True)
    sql = '''
        select mob phone, imei, cast(first_time as bigint) start_time, cast(last_time as bigint) end_time
        from nb_app_dws_res_res_his_mobileimei
    '''

    cols = ['phone','imei','if(start_time>0, start_time,0) start_time','if(end_time>0, end_time,0) end_time']
    df = spark.sql(sql).selectExpr(*cols).dropDuplicates(['phone','imei'])
    write_orc(df,add_save_path('edge_phonenumber_use_imei',root=save_root))
    write_orc(df.select('imei').distinct(),add_save_path('vertex_imei',root=save_root))

    # imsi IMSI
    # mob MOB
    # rel_type 关系类型
    # last_dis_place 发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    # totle_count 累计发现次数
    # totle_days 累计发现天数
    ## 手机关联imsi

    init(spark, 'nb_app_dws_res_res_his_mobileimsi', if_write=False, is_relate=True)
    sql = '''
            select mob phone, imsi, cast(first_time as bigint) start_time,cast(last_time as bigint) end_time 
            from nb_app_dws_res_res_his_mobileimsi
        '''

    cols = ['phone', 'imsi', 'if(start_time>0,start_time,0) start_time', 'if(end_time>0, end_time,0) end_time']
    df = spark.sql(sql).selectExpr(*cols).dropDuplicates(['phone','imsi'])
    write_orc(df,add_save_path('edge_phonenumber_use_imsi',root=save_root))
    write_orc(df.select('imsi').distinct(),add_save_path('vertex_imsi',root=save_root))


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    person_relate_imsi_imei()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))