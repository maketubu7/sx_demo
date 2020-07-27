# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 15:35
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : call_msg.py
# @Software: PyCharm

import logging
import os
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

conf=SparkConf().set('spark.driver.maxResultSize', '10g')
conf.set('spark.executor.memoryOverhead', '5g')
conf.set('spark.driver.memory','20g')
conf.set('spark.sql.shuffle.partitions',800)
conf.set('spark.default.parallelism',800)
conf.set('spark.executor.memory', '25g')
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
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/call_msg'
save_root = 'relation_theme_extenddir'




def do_something():

    macao_phone = read_orc(spark,add_save_path('vertex_phonenumber',root=save_root)).select('phone') \
            .where('(phone like "0852%" and length(phone)=12) or (phone like "852%" and length(phone)=11)')
    df = read_orc(spark,add_save_path('edge_groupcall',root=save_root))
    df.persist()
    out_degree = df.groupby('start_phone').agg(count('end_phone').alias('out_degree')).withColumnRenamed('start_phone','phone')
    in_degree = df.groupby('end_phone').agg(count('start_phone').alias('in_degree')).withColumnRenamed('end_phone','phone')
    all_phones = out_degree.selectExpr('phone').unionAll(in_degree.selectExpr('phone')).dropDuplicates()
    res = all_phones.join(out_degree,'phone','left').join(in_degree,'phone','left') \
            .select(all_phones.phone,out_degree.out_degree,in_degree.in_degree).na.fill(0) \
            .where('out_degree > 1 or in_degree > 1').select('phone')
    write_orc(res,add_save_path('all_call_degree'))
    macao_phone = macao_phone.intersect(res)
    write_orc(macao_phone,add_save_path('macao_phone3'))
    df.unpersist()

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    do_something()



logger.info('========================end time:%s==========================' % (
    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
