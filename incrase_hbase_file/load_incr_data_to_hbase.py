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

call_msg_key = ['call_msg_rowkey(start_phone,end_phone) rowkey','*']
sfzh_do_key = ['sfzh_do_rowkey(sfzh) rowkey','*']

rowkey_exprs = OrderedDict()
rowkey_exprs['edge_groupcall_detail'] = call_msg_key
rowkey_exprs['dege_groupmsg_detail'] = call_msg_key
rowkey_exprs['edge_person_stay_hotel_detail'] = sfzh_do_key
rowkey_exprs['edge_person_surf_interbar_detail'] = sfzh_do_key

def setHbaseConf(ips=None, tableName=None, Znode="/hbase", useing="input", rowStart=None, rowEnd=None):
    '''
    :param ips: [ip1,ip2,...]
    :param tableName: hbase tablename
    :param Znode: habse Znode in zookeeper
    :param rowStart: row start rowkey
    :param rowEnd: row end rowkey
    :return: hbaseConf
    '''
    if not ips:
        print("ips is null")
        sys.exit(1)
    if not tableName:
        print("tablename is null")
        sys.exit(1)
    ips = ','.join(ips)
    if useing == "input":
        hbaseConf = {
            "hbase.zookeeper.quorum": ips,
            "hbase.mapreduce.inputtable": tableName,
            "zookeeper.znode.parent": Znode
        }
    else:
        hbaseConf = {
            "hbase.zookeeper.quorum": ips,
            "hbase.cluster.distributed": "true",
            "hbase.rootdir": "hdfs://ngpcluster/hbase",
            "hbase.mapred.outputtable": tableName,
            "zookeeper.znode.parent": Znode,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.hbase.io.Writable"
        }
        return hbaseConf

    if rowStart is None or rowEnd is None:
        return hbaseConf

    hbaseConf["hbase.mapreduce.scan.row.start"] = rowStart
    hbaseConf["hbase.mapreduce.scan.row.end"] = rowEnd
    return hbaseConf

def hbaseSimpleWriter(rdd,tablename):

    ips = ['sxsthm-173','sxsthb-174','sxsths3699']
    hbaseConf = setHbaseConf(ips=ips, tableName="SLMP:%s"%tablename, useing="output")
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

    rdd.saveAsNewAPIHadoopDataset(conf=hbaseConf, keyConverter=keyConv, valueConverter=valueConv)

def runWrite(filename,columns,task_num=100):
    ''' 写入hbase数据 '''
    def formatHbaseOutput(row):
        ''' 写入rdd的格式为（rowkey,[rowkey, col_family, column, value]） '''
        rowkey = row.rowkey
        info = []
        for index,key in enumerate(columns):
            info.append((rowkey, [rowkey, 'f', columns[index], str(row[key])]))
        return info

    df = read_orc(add_incr_path(filename,cp)).selectExpr(*rowkey_exprs[filename]).limit(20)
    df.show(truncate=False)
    rdd = df.rdd.flatMap(formatHbaseOutput)
    logger.warn('sssssssssssssssss')
    logger.warn(rdd.take(1))
    logger.warn('rdd count %s'%rdd.count())
    if df and df.take(1):
        rdds = df.rdd.flatMap(formatHbaseOutput).randomSplit([float(Decimal(1)/Decimal(task_num)) for _ in range(task_num)],666)
        logger.warn('rdds length %s'%len(rdds))
        logger.warn(rdds[0].take(1))
        i = 0
        for rdd in rdds:
            logger.info(rdd.collect())
            hbaseSimpleWriter(rdd, filename.upper())
            logger.warn('%s success'%i)
            i += 1
    else:
        logger.warn('%s %s no increase data'%(filename,cp))
    # hbaseSimpleWriter(rdd,filename.upper())

def dealIncreaseData():
    ''' 处理每天的增量入库 '''
    for file,cols in increase_files.items():
        runWrite(file,cols,task_num=10)

if __name__ == '__main__':
    logger.info('================start time:%s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # cp = (datetime.now() + timedelta(days=-1)).strftime('%Y%m%d00')
    cp = '2020050900'
    dealIncreaseData()

    logger.info('=================end time:%s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))