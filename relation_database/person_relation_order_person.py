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

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commonUtil = CommonUdf(spark)

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'

order_types = {
    '1':u'火车',
    '2':u'航班',
    '3':u'汽车',
    '4':u'宾馆',
    '5':u'其他',
    '6':u'快递',

}

def get_order_name(col):
    try:
        return order_types.get(col)
    except:
        return u'其他'

spark.udf.register('get_order_name',get_order_name,StringType())

def person_relate_person():

    # nb_app_dws_per_per_his_partnerorder	公民身份与公民身份的同订单关系全量汇总表
    # cert_type	证件类型
    # cert_num	证件号码
    # rel_cert_type	关系人证件类型
    # rel_cert_num	关系人证件号码
    # order_type	订单类型
    # first_time	首次发现时间
    # last_time	最后发现时间

    init(spark,'nb_app_dws_per_per_his_partnerorder',if_write=False)

    sql = '''
        select format_zjhm(cert_num) sfzh1, format_zjhm(rel_cert_num) sfzh2, 
        get_order_name(trim(order_type)) order_type,cast(first_time as bigint) start_time, 
        cast(last_time as bigint) end_time from nb_app_dws_per_per_his_partnerorder
        where verify_sfz(cert_num) = 1 and verify_sfz(rel_cert_num) = 1
    '''
    cols = ['if(sfzh1<sfzh1,sfzh1,sfzh2) sfzh1','if(sfzh1<sfzh1,sfzh2,sfzh1) sfzh2','order_type',
            'if(start_time>0,start_time,0) start_time','if(end_time>0,end_time,0) end_time']

    df = spark.sql(sql).selectExpr(*cols).dropDuplicates(['sfzh1','sfzh2','order_type'])
    cols2 = ['sfzh1','sfzh2','mc order_type','start_time','end_time']
    order_type_dm = get_all_dict(spark).where('lb="order_type"')
    res = df.join(order_type_dm,df['order_type']==order_type_dm['dm'],'left') \
        .selectExpr(*cols2).na.fill({"order_type":u'其他'})

    write_orc(res,add_save_path('edge_person_same_order',root=save_root))

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    person_relate_person()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))