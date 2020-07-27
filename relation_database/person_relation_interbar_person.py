# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 14:54
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : with_interbar.py
# @Software: PyCharm
# @content : 上网推理信息

import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import StorageLevel
from pyspark.sql.types import *
import time, copy, re, math
from datetime import datetime, timedelta,date
import json
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')

conf=SparkConf().set('spark.driver.maxResultSize', '20g')
conf.set('spark.driver.memory', '20g')
conf.set('spark.executor.memoryOverhead', '15g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '15g')
conf.set('soark.shuffle.partitions',800)
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/with_travel'

def get_diff_time(start1, end1, start2, end2):
    try:
        ts = sorted([(start1,end1),(start2,end2)],key=lambda t: t[1])
        t1 = end1-start1
        t2 = end2-start2
        t = _min(t1,t2)
        s1 = _max(ts[0][0],ts[0][1])
        s2 = _min(ts[1][0],ts[1][1])
        return _min(s1-s2,t) if s1-s2 > 0  else 0
    except:
        return 0

def verify_timestamp(start1, end2):
    try:
        if start1 and end2:
            start1 = int(start1)
            end2 = int(end2)
            if start1 > end2:
                return 0
            else:
                return 1
        else:
            return 0
    except:
        return 0

def _max(data1, data2):
    if data1 and data2:
        return data1 if data1 > data2 else data2

def _min(data1, data2):
    if data1 and data2:
        return data1 if data1 < data2 else data2

spark.udf.register('get_max_int', _max, LongType())
spark.udf.register('get_min_int', _min, LongType())
spark.udf.register('get_diff_time', get_diff_time, IntegerType())
spark.udf.register('verify_timestamp', verify_timestamp, IntegerType())

def repartition(df):
    return df.repartition(200)

def find_ids_dup(person_df,df,key_id):
    df.createOrReplaceTempView('tmp')
    tmp_person = spark.sql('select sfzh, collect_set(%s) ids from tmp group by sfzh'%key_id)
    person_df1 = tmp_person.join(person_df, person_df.sfzh1 == tmp_person.sfzh, 'inner') \
        .selectExpr('sfzh sfzh1', 'sfzh2', 'ids ids_a')
    person_df2 = tmp_person.join(person_df1, person_df1.sfzh2 == tmp_person.sfzh, 'inner') \
        .selectExpr('sfzh1', 'sfzh sfzh2', 'ids_a', 'ids ids_b')

    ## 去除没有过相同火车id的人
    res_person = person_df2.where('find_ids_dup(ids_a,ids_b)>0').select('sfzh1','sfzh2').dropDuplicates()
    return res_person

def get_internetbar_detail():
    #上网明细
    df2 = read_orc(spark,add_incr_path('edge_person_surfing_internetbar_detail')) \
        .selectExpr('sfzh', 'siteid', 'start_time', 'end_time').dropDuplicates()

    write_parquet(df2, path_prefix,'internetbar_detail')
    logger.info('internetbar_detail down')

    ##相同网吧上网人员
    person_df = read_parquet(spark,path_prefix,'person_person') ##同出行的算出的人的直接关系表
    res_person = find_ids_dup(person_df,df2,'siteid')
    write_parquet(res_person,path_prefix,'surfing_person')

def edge_person_with_internetbar_surfing():

    df1 = read_parquet(spark,path_prefix,'internetbar_detail')
    person_df = read_parquet(spark,path_prefix,'surfing_person')
    df1.persist(StorageLevel.MEMORY_AND_DISK)
    cond = 'start_id == end_id and verify_timestamp(start_a, end_b) = 1 and verify_timestamp(start_b, end_a) = 1'

    df2 = df1.join(person_df, df1.sfzh == person_df.sfzh1, 'inner') \
        .select(person_df.sfzh1,df1.siteid.alias('start_id'),df1.start_time.alias('start_a'),
                df1.end_time.alias('end_a'),person_df.sfzh2).dropDuplicates()

    p_df = df1.join(df2, df1.sfzh == df2.sfzh2, 'inner') \
        .select(df2.sfzh1, df2.start_id, df2.start_a, df2.end_a, df1.sfzh.alias('sfzh2'),
                df1.siteid.alias('end_id'), df1.start_time.alias('start_b'), df1.end_time.alias('end_b')).where(cond)

    df4 = p_df.selectExpr('sfzh1', 'sfzh2', 'start_id as siteid', 'start_a', 'end_a', 'start_b', 'end_b')

    df4 = repartition(df4)

    df5 = df4.selectExpr('sfzh1','sfzh2','get_max_int(start_a,start_b) as start_time',
        'get_min_int(end_a, end_b) as end_time','get_diff_time(start_a,end_a,start_b,end_b) as duration_time','siteid')\
        .where('duration_time > 0')

    write_parquet(df5,path_prefix,'internetbar_df5')

    df6 = read_orc(spark,add_save_path('vertex_internetbar')).select('siteid','title','address')
    df5 = read_parquet(spark,path_prefix,'internetbar_df5')

    #### 同上网明细
    detail_res =df5.join(df6, df5.siteid == df6.siteid, 'left') \
        .selectExpr('sfzh1', 'sfzh2', 'start_time', 'end_time', 'duration_time','title', 'address') \
        .dropDuplicates()

    write_orc(detail_res, add_save_path('edge_person_internetbar_detail'))

    ## 同上网结果
    res = detail_res.groupby('sfzh1','sfzh2') \
        .agg(min('start_time').alias('start_time'),max('end_time').alias('end_time'),count('title').alias('num'))
    write_orc(res, add_save_path('edge_person_with_internetbar_surfing'))
    logger.info('edge_person_with_internetbar_surfing down')


def get_relation_internetbar_month():
    '''按月明细'''
    last_cp = '2020040000'
    create_tmpview_table(spark,'edge_person_internetbar_detail')
    sql = '''
        select sfzh1, sfzh2, timestamp2month(start_time) as cp from edge_person_internetbar_detail 
        where timestamp2month(start_time) >= %s
    '''%last_cp
    df1 = spark.sql(sql)

    #为了后面的人物图层的计算  这里需做成双向边  便于后面人物关系分的计算
    df2 = df1.selectExpr('sfzh2 sfzh1','sfzh1 sfzh2', 'cp')
    df = df1.unionAll(df2)
    df.persist(StorageLevel.MEMORY_AND_DISK)
    cps = [row.cp for row in df.selectExpr('cast(cp as string) cp').distinct().collect() if row.cp >= last_cp and row.cp <= time.strftime("%Y%m0000", time.localtime())]
    for cp in cps:
        res = df.where('cp=%s'%cp).groupby('sfzh1','sfzh2').agg(count('cp').alias('num'))
        res = res.withColumn('cp', lit(cp)).select('sfzh1','sfzh2','num','cp')
        write_orc(res,add_save_path('relation_withinter_month',cp=cp,root='person_relation_detail'))

    df.unpersist()

if __name__ == '__main__':
    logger.info("=================deal with_internetbar start time %s======================"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    today_cp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    get_internetbar_detail()
    edge_person_with_internetbar_surfing()
    get_relation_internetbar_month()

    logger.info("=================deal with_internetba start time %s======================"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
