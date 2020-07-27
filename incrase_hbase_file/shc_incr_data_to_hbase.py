# Author:Dengwenxing
# -*- coding: utf-8 -*-
# @Time     :2019/12/30 15:09
# @Site     :
# @fILE     : hbaseReader.py
# @Software :

import sys, os
from decimal import Decimal
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time, copy, re, math
from datetime import date
from datetime import datetime, timedelta
import json
import functools
import logging
from random import randint
from collections import OrderedDict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')

conf=SparkConf().set('spark.driver.maxResultSize', '5g')
conf.set('spark.yarn.am.cores', 4)
conf.set('spark.executor.memory', '2g')
conf.set('spark.executor.memoryOverhead', '2g')
conf.set('spark.executor.instances', 100)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .appName('load_increase_data') \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

def add_incr_path(file,cp):
    return '/user/shulian/incrdir/%s/cp=%s'%(file,cp)
def read_orc(path):
    try:
        df = spark.read.orc(path)
        return df
    except:
        return None

## rowkey的r变换方法
def call_msg_rowkey(start_phone,end_phone):
    return start_phone[::-1]+'_'+end_phone

def sfzh_do_rowkey(sfzh):
    return sfzh[::-1]

spark.udf.register('call_msg_rowkey',call_msg_rowkey,StringType())
spark.udf.register('sfzh_do_rowkey',sfzh_do_rowkey,StringType())

increase_files = OrderedDict()
increase_files['edge_groupcall_detail'] = ["start_phone", "end_phone","start_time","end_time",
                                           "call_duration","homearea","relatehomeac"]
# increase_files['dege_groupmsg_detail'] = ['start_phone', 'end_phone', 'start_time', 'end_time', 'send_time',
#                                             'message', 'homearea', 'relatehomeac']
# increase_files['edge_person_stay_hotel_detail'] = ["sfzh", "lgdm", "zwmc", "start_time", "end_time"]
# increase_files['edge_person_surf_interbar_detail'] = ['sfzh','siteid','start_time','end_time']

call_msg_key = ['call_msg_rowkey(start_phone,end_phone) id','start_phone','end_phone','cast(start_time as string) start_time',
                'cast(end_time as string) end_time','cast(call_duration as string) call_duration','homearea','relatehomeac']
sfzh_do_key = ['sfzh_do_rowkey(sfzh) id','*']

rowkey_exprs = OrderedDict()
rowkey_exprs['edge_groupcall_detail'] = call_msg_key
rowkey_exprs['dege_groupmsg_detail'] = call_msg_key
rowkey_exprs['edge_person_stay_hotel_detail'] = sfzh_do_key
rowkey_exprs['edge_person_surf_interbar_detail'] = sfzh_do_key



def runWrite(filename):
    ''' 写入hbase数据 '''
    dep = 'org.apache.spark.sql.execution.datasources.hbase'
    df = read_orc(add_incr_path(filename,cp)).selectExpr(*rowkey_exprs[filename]).limit(20)
    catalog = '''  
        {
        "table":{"namespace":"SLMP","name":"EDGE_GROUPCALL_DETAIL_SHCS","tableCoder":"PrimitiveType"},
        "rowkey":"key",
        "columns":{
                    "id":{"cf":"rowkey","col":"key","type":"string"},
                    "start_phone":{"cf":"f","col":"start_phone","type":"string"},
                    "end_phone":{"cf":"f","col":"end_phone","type":"string"},
                    "start_time":{"cf":"f","col":"start_time","type":"bigint"},
                    "end_time":{"cf":"f","col":"end_time","type":"bigint"},
                    "call_duration":{"cf":"f","col":"call_duration","type":"bigint"},
                    "homearea":{"cf":"f","col":"homearea","type":"string"},
                    "relatehomeac":{"cf":"f","col":"relatehomeac","type":"string"}
        }
        }
    '''
    df.show()
    df.write.options(catalog=catalog,newTable="10").format(dep).save()


if __name__ == '__main__':
    logger.info('================start time:%s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # cp = (datetime.now() + timedelta(days=-1)).strftime('%Y%m%d00')
    cp = '2020050900'
    runWrite('edge_groupcall_detail')

    logger.info('=================end time:%s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))