# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 15:35
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : jg.py
# @Software: PyCharm
# @content : 为节点添加jid, 关系添加 start_jid end_jid
import logging
import random
import time
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.yarn.am.cores', 5)
conf.set('soark.shuffle.partitions',400)
conf.set('spark.executor.memory', '20g')
conf.set('spark.executor.instances', 50)
conf.set('spark.executor.cores', 6)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')

# from jg_info_sxwa import *
from jg_info import *
spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

def write_orc(df, path, mode='overwrite'):
    df.write.mode(mode).format('orc').save(path)
    logger.info('write success')

def write_parquet(df, path, mode='overwrite'):
    df.write.format('parquet').mode(mode).save(path)
    logger.info('write success')

def read_orc(path):
    df = spark.read.orc(path)
    return df


def add_save_path(tablename, cp=''):
    ## hive外表保存地址
    if cp:
        tmp_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/{}/cp={}'
        return tmp_path.format(tablename.lower(), cp)
    # return '/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/{}/cp=2020'.format(tablename.lower())
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/relation_theme_extenddir/{}/cp=2020'.format(tablename.lower())

def add_save_jg_path(tablename,standby=False):
    ## jg文件保存地址
    if standby:
        return '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_data_standby/{}'.format(tablename.lower()+"_jg")
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_data/{}'.format(tablename.lower()+"_jg")


def create_tmpview_table(tablename):
    df = read_orc(add_save_path(tablename))
    df.createOrReplaceTempView(tablename)

def create_tmpview_jg_table(tablename):
    df = read_orc(add_save_jg_path(tablename))
    df.createOrReplaceTempView(tablename)


# 节点
vertex_zd = ['jid']


def deal_vertex():
    def map_rdd(data):
        ret = []
        row, index = data
        jid = int(str(random.randint(1, 9)) + str(index + table_index))
        ret.append(jid)
        row = list(row)
        for item in row:
            ret.append(item)
        return tuple(ret)
    for tablename in vertex_table_info:
        logger.info('dealing %s' % tablename)
        source_table_info = vertex_table_info[tablename]
        table_index = get_table_index(tablename)
        df = read_orc(add_save_path(tablename)).selectExpr(*source_table_info)
        rdd = df.rdd.zipWithIndex().map(map_rdd)
        new_schema = vertex_zd + source_table_info
        res = spark.createDataFrame(rdd, new_schema)
        write_orc(res, add_save_jg_path(tablename + '_jg'))
        logger.info('%s_jg down' % tablename)


edge_zd = ['start_jid', 'end_jid']

def deal_edge():
    for tablename in edge_table_info:
        logger.info('dealing %s ' % tablename)
        # 边字段
        source_table_info = edge_table_info[tablename]
        # 依赖信息
        start_tablename = edge_info[tablename][0]
        start_id = edge_info[tablename][1]
        end_tablename = edge_info[tablename][2]
        end_id = edge_info[tablename][3]
        # 原始表开始节点和结束节点
        s_start_id = source_table_info[0]
        s_end_id = source_table_info[1]

        zd = ','.join(source_table_info)
        create_tmpview_table(tablename)
        df = spark.sql('''select %s from %s ''' % (zd, tablename))

        create_tmpview_jg_table(start_tablename)
        create_tmpview_jg_table(end_tablename)
        df_1 = spark.sql('''select jid as start_jid,%s as s_start  from %s ''' % (start_id, start_tablename))
        df_2 = spark.sql('''select jid as end_jid,%s as s_end from %s ''' % (end_id, end_tablename))

        df1 = df.join(df_1, col(s_start_id) == col('s_start'), 'left').drop(col('s_start'))
        df2 = df1.join(df_2, col(s_end_id) == col('s_end'), 'left').drop(col('s_end'))

        res_zd = edge_zd + source_table_info
        res = df2.select(res_zd).where('start_jid is not null and end_jid is not null and start_jid != end_jid')

        write_orc(res, add_save_jg_path(tablename + '_jg'))

        logger.info('%s_jg down' % tablename)


if __name__ == "__main__":
    ''' 生成JG文件， 并对应的生成parquet格式的文件一份 '''
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    deal_vertex()
    # deal_edge()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

# spark-submit  --master yarn --deploy-mode  cluster  --name jg --driver-memory 40g  --queue bbd_01 jg.py
