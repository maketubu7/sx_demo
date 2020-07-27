# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 15:35
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : jg.py
# @Software: PyCharm
# @content : 为节点添加jid及全图的度, 关系添加 start_jid end_jid
import logging
import random,sys
from pyspark import SparkConf,StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.yarn.am.cores', 4)
conf.set('spark.executor.memoryOverhead', '10g')
conf.set('soark.shuffle.partitions',3000)
conf.set('spark.executor.memory', '20g')
conf.set('spark.executor.instances', 100)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/jg'
save_root = 'relation_theme_extenddir'

from jg_info import *
from common import *

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setCheckpointDir('/phoebus/_fileservice/users/slmp/shulianmingpin/job_checkpoint')


def add_save_jg_path(tablename,root='jg_data'):
    ## jg文件保存地址
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/{}/{}'.format(root,tablename.lower()+"_jg")

def add_save_vertex_tmp(tablename):
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_tmp/{}'.format(tablename.lower()+"_jg")

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
        df = read_orc(spark,add_save_path(tablename,root=save_root)).selectExpr(*source_table_info)
        rdd = df.rdd.zipWithIndex().map(map_rdd)
        new_schema = vertex_zd + source_table_info
        res = spark.createDataFrame(rdd, new_schema)
        write_orc(res, add_save_vertex_tmp(tablename))
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
        df = read_orc(spark,add_save_path(tablename,root=save_root))
        if df and df.take(1):
            df = df.selectExpr(*source_table_info)
            read_orc(spark,add_save_vertex_tmp(start_tablename)).createOrReplaceTempView(start_tablename)
            read_orc(spark,add_save_vertex_tmp(end_tablename)).createOrReplaceTempView(end_tablename)
            df_1 = spark.sql('''select jid as start_jid,%s as s_start  from %s ''' % (start_id, start_tablename))
            df_2 = spark.sql('''select jid as end_jid,%s as s_end from %s ''' % (end_id, end_tablename))

            df1 = df.join(df_1, col(s_start_id) == col('s_start'), 'left').drop(col('s_start'))
            df2 = df1.join(df_2, col(s_end_id) == col('s_end'), 'left').drop(col('s_end'))

            res_zd = edge_zd + source_table_info
            res = df2.select(res_zd).where('start_jid is not null and end_jid is not null and start_jid != end_jid')

            write_orc(res, add_save_jg_path(tablename,root='jg_data_standby'))
            logger.info('%s_jg down' % tablename)

def get_all_vertex_degree():
    dfs = []
    edge_table_info.pop('relation_score_res')
    for tablename in edge_table_info:
        logger.info('dealing %s'%tablename)
        df = read_orc(spark,add_save_jg_path(tablename,root='jg_data_standby'))
        if df:
            df = df.selectExpr('start_jid','end_jid')
            dfs.append(df)
    res = reduce(lambda a,b:a.unionAll(b),dfs)
    write_parquet(res,path_prefix,'res_tmp')
    res = read_parquet(spark,path_prefix,'res_tmp')
    res.persist(StorageLevel.MEMORY_AND_DISK)
    out_degree = res.groupby('start_jid').agg(count('end_jid').alias('out_degree')).withColumnRenamed('start_jid','jid')
    in_degree = res.groupby('end_jid').agg(count('start_jid').alias('in_degree')).withColumnRenamed('end_jid','jid')
    all_vertex = out_degree.select('jid').union(in_degree.select('jid')).dropDuplicates()
    res.unpersist()
    all_degree = all_vertex.join(out_degree,'jid','left').join(in_degree,'jid','left') \
            .select(all_vertex.jid,out_degree.out_degree,in_degree.in_degree).na.fill(0)
    write_parquet(all_degree,path_prefix,'all_degree')
    all_degree = read_parquet(spark,path_prefix,'all_degree').selectExpr('jid','(out_degree+in_degree) degree')
    all_degree.persist(StorageLevel.MEMORY_AND_DISK)

    for tablename in vertex_table_info:
        schema = ['jid'] + vertex_table_info[tablename] + ['degree']
        df = read_orc(spark,add_save_vertex_tmp(tablename))
        df = df.join(all_degree,'jid','inner').drop(all_degree.jid).selectExpr(*schema) \
                        .na.fill({'degree':0})
        write_orc(df,add_save_jg_path(tablename,root='jg_data_standby'))

    all_degree.unpersist()

if __name__ == "__main__":
    ''' 生成JG文件， 并对应的生成parquet格式的文件一份 '''
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # deal_vertex()
    deal_edge()
    # get_all_vertex_degree()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
