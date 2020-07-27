# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : qq_msg.py
# @Software: PyCharm
# @content : qq相关信息

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
conf=SparkConf().set('spark.driver.maxResultSize', '20g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '1g')
conf.set('spark.executor.instances', 20)
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/qq'
root_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/touyou_case/'

def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix,'parquet/',path))
    return df
def write_parquet(df,path):
    '''写 parquet'''
    df.write.mode("overwrite").parquet(path=os.path.join(path_prefix,'parquet/',path))

def write_orc(df, path,mode='overwrite'):
    df.write.format('orc').mode(mode).save(path)
    logger.info('write success')

def read_orc(path):
    df = spark.read.orc(path)
    return df

def read_csv(file,schema):
    path= '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/zp_data/'+file+'.csv'
    return spark.read.csv(path, schema=schema,header=False)

def read_csv_heaer(file):
    path= '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/zp_data/'+file+'.csv'
    return spark.read.csv(path,header=True)

def vertex_case():
    case_schema = StructType([
        StructField("asjbh", StringType(), True),
        StructField("ajmc", StringType(), True),
        StructField("link_app", StringType(), True),
        StructField("city", StringType(), True),
    ])
    df = read_csv('vertex_case',case_schema)
    df1 = df.where('asjbh!="A"')
    df2 = df.where('asjbh="A"').drop('asjbh').selectExpr('md5(ajmc) asjbh','ajmc','link_app','city')

    tmp = df1.unionAll(df2).dropDuplicates(['asjbh','ajmc'])
    all_case = read_orc(add_save_path('vertex_case')).selectExpr('asjbh','jyaq')

    res = tmp.join(all_case,'asjbh','left').select(tmp.asjbh,tmp.ajmc,tmp.link_app,tmp.city,all_case.jyaq)
    write_orc(res,root_path+'vertex_case')

def edge_case_link_account():

    link_qq_schema = StructType([
        StructField("ajbh", StringType(), True),
        StructField("ajmc", StringType(), True),
        StructField("qq", StringType(), True),
    ])

    link_we_schema = StructType([
        StructField("ajbh", StringType(), True),
        StructField("ajmc", StringType(), True),
        StructField("wechatid", StringType(), True),
    ])

    link_ali_schema = StructType([
        StructField("ajbh", StringType(), True),
        StructField("ajmc", StringType(), True),
        StructField("alipay", StringType(), True),
    ])

    link_bank_schema = StructType([
        StructField("ajbh", StringType(), True),
        StructField("ajmc", StringType(), True),
        StructField("bank_num", StringType(), True),
    ])

    files =[
        {'file':'case_link_qq','path':'edge_case_link_qq','schema':link_qq_schema,'drop_key':['ajbh','qq'],'vertex':'vertex_qq'},
        {'file':'case_link_wechatid','path':'edge_case_link_wechat','schema':link_we_schema,'drop_key':['ajbh','wechatid'],'vertex':'vertex_wechat'},
        {'file':'case_link_bank_num','path':'edge_case_link_banknum','schema':link_bank_schema,'drop_key':['ajbh','bank_num'],'vertex':'vertex_banknum'},
        {'file':'case_link_alipay','path':'edge_case_link_alipay','schema':link_ali_schema,'drop_key':['ajbh','alipay'],'vertex':'vertex_alipay'}
    ]
    for info in files:
        df = read_csv(info['file'],info['schema'])
        df = df.withColumn('start_time',lit(0))
        df = df.withColumn('end_time',lit(0))
        res = df.dropDuplicates(info['drop_key'])
        write_orc(res,root_path+info['path'])
        # v = res.selectExpr(info['drop_key'][1]).dropDuplicates()
        # write_orc(v,add_save_path(info['vertex']))

def group_link_banknum():

    group_link_bank_schema = StructType([
        StructField("groupid", StringType(), True),
        StructField("qq", StringType(), True),
        StructField("send_time", StringType(), True),
        StructField("banknum", StringType(), True),
    ])
    # group, qq, label, groupcard, is_creator, is_master, del_status, join_time, quit_time, lsat_ip, ip_area, last_time
    qq_schema = StructType([
        StructField("groupid", StringType(), True),
        StructField("qq", StringType(), True),
        StructField("label", StringType(), True),
        StructField("groupcard", StringType(), True),
        StructField("is_creator", StringType(), True),
        StructField("is_master", StringType(), True),
        StructField("del_status", StringType(), True),
        StructField("join_time", StringType(), True),
        StructField("quit_time", StringType(), True),
        StructField("last_ip", StringType(), True),
        StructField("ip_area", StringType(), True),
        StructField("last_time", StringType(), True),
    ])

    df = read_csv('edge_group_bank_detail',group_link_bank_schema)
    df.createOrReplaceTempView('tmp')
    sql = '''
        select groupid, banknum, min(valid_datetime(send_time)) relate_time,min(valid_datetime(send_time)) start_time,
        max(valid_datetime(send_time)) end_time,count(*) relate_num from tmp
        group by groupid, banknum
    '''
    res = spark.sql(sql)

    # write_orc(res,root_path+'edge_qqgroup_link_banknum')
    #
    # group_cols = ["groupname","count","masterid","groupflag","groupclass","introduction",
    #             "annment","create_time","last_time"]
    #
    # tmp = res.select('groupid').drop_duplicates()
    # def df_add_cols(df,cols):
    #     for col in cols:
    #         if 'time' in col or 'rq' in col:
    #             df = df.withColumn(col, lit(0))
    #         else:
    #             df = df.withColumn(col,lit(''))
    #     return df
    # res = df_add_cols(tmp,group_cols)
    # write_orc(res,root_path+'vertex_qq_group')
    #
    # tmp = read_orc(add_save_path('edge_qqgroup_link_banknum')).selectExpr('banknum yhkh').drop_duplicates()
    # tmp2 = read_orc(add_save_path('edge_case_link_banknum')).selectExpr('bank_num yhkh')
    # tmp = tmp.union(tmp2)
    # bank_cols = ['khh','xm','sfzh','wyyzdh','khrq']
    # res = df_add_cols(tmp,bank_cols).drop_duplicates()
    # write_orc(res,add_save_path('vertex_zp_bankcard'))
    #
    # # qq数据
    # df = read_csv('qq_info_res',qq_schema)
    # qq_cols = ['groupid', 'qq', 'label', 'groupcard', 'is_creator', 'is_master', 'del_status',
    #            'join_time', 'quit_time', 'last_ip', 'ip_area', 'last_time']
    # df_pre = read_orc(add_save_path('vertex_zp_qq')).select('qq')
    # df_pre = df_add_cols(df_pre,qq_cols[2:])
    # res = df.selectExpr(*qq_cols[1:]).unionAll(df_pre.selectExpr(*qq_cols[1:]))
    # write_orc(res,add_save_path('tmp'))
    # tmp = read_orc(add_save_path('tmp')).drop_duplicates(['qq']).groupby('qq') \
    #     .agg(collect_set('label').alias('labels'))
    # write_orc(tmp,add_save_path('vertex_zp_qq'))
    # #
    # df1 = df.selectExpr(*qq_cols).drop_duplicates(['groupid', 'qq'])
    # write_orc(df1,add_save_path('edge_qq_link_group'))



if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    vertex_case()
    edge_case_link_account()
    group_link_banknum()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))