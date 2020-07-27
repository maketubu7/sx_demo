# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 11:22
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : hotel.py
# @Software: PyCharm

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time, copy, re, math
from datetime import datetime, timedelta, date
import os, sys
import logging
import __builtin__

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

warehouse_location = '/user/hive/warehouse/'
conf = SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '20g')
conf.set('soark.shuffle.partitions',800)
conf.set('spark.executor.instances', 50)
conf.set('spark.executor.cores', 8)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')
conf.set("spark.sql.warehouse.dir", warehouse_location)

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commonUtil = CommonUdf(spark)
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/hotel'


##todo:all
def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix, 'parquet/', path))
    return df


def write_parquet(df, path):
    '''写 parquet'''
    df.write.mode("overwrite").parquet(path=os.path.join(path_prefix, 'parquet/', path))


def write_orc(df, path):
    df.write.format('orc').mode('overwrite').save(path)
    logger.info('write success')


def read_orc(path):
    df = spark.read.orc(path)
    return df

def _max(k1,k2):
    return k1 if k1 > k2 else k2

def _min(k1,k2):
    return k1 if k1 < k2 else k2
def _abs(data):
    return data if data >= 0 else -data

def same_hotel_fjh(data):
    '''同房间号'''
    ret = []
    hotel, rows = data
    lgdm, fjh = hotel
    rows = list(rows)
    if len(rows) > 1:
        for index, row in enumerate(rows):
            for i in range(index + 1, len(rows)):
                # 10分钟内同入住  且 排除退房的情况
                if _abs(row.start_time - rows[i].start_time) <= 60 * 10:
                    if row.end_time != 0 and row.end_time < rows[i].start_time:
                        pass
                    elif rows[i].end_time != 0 and rows[i].end_time < row.start_time:
                        pass
                    else:
                        sfzh1 = _min(row.sfzh, rows[i].sfzh)
                        sfzh2 = _max(row.sfzh, rows[i].sfzh)
                        start = _min(row.start_time, rows[i].start_time)
                        if row.end_time != 0 and rows[i].end_time != 0:
                            end = _min(row.end_time, rows[i].end_time)
                        else:
                            end = _max(row.end_time, rows[i].end_time)
                        ret.append([sfzh1, sfzh2, lgdm, fjh, start, end])
    return ret

def same_hotel_house():
    ## 分区推理 按照每月的数据进行推理
    last_cp = read_orc(add_save_path('edge_same_hotel_house_detail', cp='*')).selectExpr('max(cp)').collect()[0][0]
    if last_cp > today_cp:
        last_cp = '2020030000'
    create_tmpview_table(spark,'edge_person_stay_hotel_detail')
    cps = spark.sql('select distinct timestamp2month(start_time) cp from edge_person_stay_hotel_detail where timestamp2month(start_time) >=%s '%last_cp)
    partitions = cps.rdd.map(lambda row: row.cp).collect()
    partitions = sorted(filter(lambda cp:len(str(cp))==10,partitions))
    detail_df = read_orc(add_save_path('edge_person_stay_hotel_detail')) \
            .selectExpr("sfzh", "lgdm", "start_time", "end_time","zwmc","timestamp2month(start_time) cp")

    ## 对每月的住宿情况进行推理
    detail_df.persist()
    hotel_df = read_orc(add_save_path('vertex_hotel')).select('lgdm','qiyemc').cache()
    for cp in partitions:
        df = detail_df.select('lgdm','zwmc','sfzh','start_time','end_time').where('cp=%s'%cp)
        rdd = df.rdd.map(lambda r: ((r.lgdm, r.zwmc), r)).groupByKey().flatMap(same_hotel_fjh)
        if rdd.take(1):
            tmp = spark.createDataFrame(rdd, ['sfzh1', 'sfzh2', 'lgdm', 'fjh', 'start_time', 'end_time']) \
                        .dropDuplicates(['sfzh1', 'sfzh2', 'lgdm', 'fjh'])
            res = tmp.where('sfzh1!="" and sfzh1 != sfzh2')
            res = res.join(hotel_df,'lgdm','left') \
                .select(res.sfzh1, res.sfzh2, res.lgdm, hotel_df.qiyemc, res.fjh, res.start_time, res.end_time)
            res = res.withColumn('cp',lit(cp)).repartition(20)
            write_orc(res,add_save_path('edge_same_hotel_house_detail',cp=cp))
    detail_df.unpersist()

    create_tmpview_table(spark,'edge_same_hotel_house_detail')
    sql = ''' select sfzh1, sfzh2, min(start_time) start_time, max(end_time) end_time, count(lgdm) as num 
                from edge_same_hotel_house_detail group by sfzh1, sfzh2 '''
    df = spark.sql(sql)
    write_orc(df,add_save_path('edge_same_hotel_house'))

def get_relation_samehotel_month():
    '''按月明细'''
    try:
        last_cp = read_orc(add_save_path('edge_same_hotel_house_detail',cp='*')) \
            .selectExpr('cast(max(cp) as string)').collect()[0][0]
        if last_cp > today_cp:
            last_cp = today_cp
    except:
        last_cp = '0'
    create_tmpview_table(spark,'edge_same_hotel_house_detail')
    sql = '''
        select sfzh1, sfzh2, timestamp2month(start_time) as cp from edge_same_hotel_house_detail 
        where timestamp2month(start_time) >= %s
    '''%last_cp
    df1 = spark.sql(sql)
    #为了后面的人物图层的计算  这里需做成双向边  便于后面人物关系分的计算
    df = df1.unionAll(df1.selectExpr('sfzh2 sfzh1','sfzh1 sfzh2', 'cp'))
    df.persist(StorageLevel.MEMORY_AND_DISK)
    cps = [row.cp for row in df.select('cp').distinct().collect()]

    for cp in cps:
        res = df.where('cp=%s'%cp).groupby('sfzh1','sfzh2').agg(count('cp').alias('num'))
        res = res.withColumn('cp', lit(cp)).select('sfzh1','sfzh2','num','cp')
        write_orc(res,add_save_path('relation_samehotel_month',cp=cp,root='person_relation_detail'))

    df.unpersist()

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    today_cp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    same_hotel_house()
    get_relation_samehotel_month()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))