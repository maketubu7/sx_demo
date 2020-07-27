# Author:Dengwenxing
# -*- coding: utf-8 -*-
# @Time     :2019/12/30 15:09
# @Site     :
# @fILE     : hbaseReader.py
# @Software :

import sys, os
import requests
import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark.sql.types import Row
import time, copy, re, math
from datetime import date
from datetime import datetime, timedelta
import json
import logging
from random import randint
from collections import OrderedDict


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')

conf=SparkConf().set('spark.driver.maxResultSize', '2g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '10g')
conf.set('spark.executor.instances', 50)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .appName('test_data') \
    .enableHiveSupport() \
    .getOrCreate()


def write_es(df,type,index='slmp-sxgraphspace',map_id='id',mode='append'):

    options = OrderedDict()
    options["es.nodes"] = "24.1.33.113,24.1.33.115"
    options["es.resource"] = "%s/%s" % (index, type)
    options["es.mapping.id"] = map_id

    df.write \
        .format("org.elasticsearch.spark.sql") \
        .options(**options) \
        .mode(mode) \
        .save()

def read_es(type,query='',index='slmp-sxgraphspace'):
    '''
    从es数据库查询数据
    :param type: 文档类型
    :param query: 查询ddl
    :param index: 默认索引库（slmp-sxgraphspace）
    :return:
    '''
    options = OrderedDict()
    options["es.nodes"] = "24.1.33.113,24.1.33.115"
    options["es.resource"] = "%s/%s"%(index,type)
    if query:
        options["es.query"] = query
    df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .options(**options) \
        .load()
    return df

def deal_es_status():
    response = requests.get('http://24.1.33.113:9200/_cat/indices?v')
    tmp = response.text.split('\n')
    res = []
    for v in tmp:
        row = filter(lambda a:a, v.split(' '))
        if row:
            if 'all' in row[2]:
                res.append([row[2],row[6]])
    df = pd.DataFrame(res[1:],columns=res[0])
    df.to_csv('/opt/workspace/incerase_file/es_tatus.csv',index=False)

if __name__ == '__main__':
    logger.info('================start time:%s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    data = [('make', 25), ('tubu', 22)]
    save_df = spark.createDataFrame(data, ['name', 'age'])
    write_es(save_df,'zjhm',index='test')



    logger.info('=================end time:%s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))