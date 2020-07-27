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
conf.set('spark.executor.memory', '20g')
conf.set('spark.executor.instances', 40)
conf.set('spark.executor.cores', 8)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')
#conf.set("spark.sql.warehouse.dir", warehouse_location)


spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commonUtil = CommonUdf(spark)

save_path = 'wa_data/kill_case'
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/qq'
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
    path= '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/kill_case/'+file
    return spark.read.csv(path, schema=schema,header=False)

def df_add_cols(df,cols):
    for col in cols:
        if 'time' in col or 'rq' in col or 'sj' in col:
            df = df.withColumn(col, lit(0))
        else:
            df = df.withColumn(col,lit(''))
    return df

def vertex_qq():
    # qq, nickname, sex, csrq, regis_ip, sign_name, mobile, email, regis_time, area, province, city, college, study
    qq_schema = StructType([
        StructField("qq", StringType(), True),
        StructField("nickname", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("csrq", StringType(), True),
        StructField("regis_ip", StringType(), True),
        StructField("sign_name", StringType(), True),
        StructField("mobile", StringType(), True),
        StructField("email", StringType(), True),
        StructField("regis_time", StringType(), True),
        StructField("area", StringType(), True),
        StructField("province", StringType(), True),
        StructField("city", StringType(), True),
        StructField("college", StringType(), True),
        StructField("study", StringType(), True),
    ])

    qq_cols = ['qq','nickname', 'sex', 'city', 'mobile', 'email', 'realname', 'csrq',
     'college', 'area', 'province', 'regis_time', 'regis_ip', 'study', 'sign_name']

    we_cols = ['wechat','wxid','qq','mobile','nickname','sex','address','zcsj']

    # wechat, wechatid, qq, mobile, nickname, sex, province, city, oversea, age, email

    we_schema = StructType([
        StructField("wechat", StringType(), True),
        StructField("wxid", StringType(), True),
        StructField("qq", StringType(), True),
        StructField("mobile", StringType(), True),
        StructField("nickname", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("province", StringType(), True),
        StructField("city", StringType(), True),
        StructField("oversea", StringType(), True),
        StructField("age", StringType(), True),
        StructField("email", StringType(), True),
    ])

    tmp = read_csv('vertex_qq.csv', qq_schema)
    tmp = tmp.withColumn('realname', lit('')).selectExpr(*qq_cols)

    tmp2 = read_orc(add_save_path('edge_person_own_qq',root=save_path)).select('qq') \
            .union(read_orc(add_save_path('edge_qq_link_qq',root=save_path)).selectExpr('qq1 qq')) \
            .union(read_orc(add_save_path('edge_qq_link_qq',root=save_path)).selectExpr('qq2 qq'))

    tmp2 = df_add_cols(tmp2,qq_cols[1:])
    res = tmp.unionAll(tmp2.selectExpr(*qq_cols))
    df = res.na.fill('').dropDuplicates(['qq']).where('qq != "" and qq != "nan"')
    write_orc(df, add_save_path('vertex_qq',root=save_path))


    tmp = read_csv('vertex_wechat.csv',we_schema)
    tmp = tmp.withColumn('address',lit(''))
    tmp = tmp.withColumn('zcsj',lit(0))
    tmp = tmp.selectExpr(*we_cols)

    tmp2 = read_orc(add_save_path('edge_person_own_wechat', root=save_path)).select('wechat') \
        .union(read_orc(add_save_path('edge_wechat_link_wechat', root=save_path)).selectExpr('wechat1 wechat')) \
        .union(read_orc(add_save_path('edge_wechat_link_wechat', root=save_path)).selectExpr('wechat2 wechat'))

    tmp2 = df_add_cols(tmp2, we_cols[1:])
    res = tmp.unionAll(tmp2.selectExpr(*we_cols))
    df = res.na.fill('').dropDuplicates(['wechat']).where('wechat != "" and wechat != "nan"')
    write_orc(df, add_save_path('vertex_wechat', root=save_path))


def qq_smz():

    qq_smz_schema = StructType([
        StructField("wechat", StringType(), True),
        StructField("qq", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("sfzh", StringType(), True),
    ])

    tmp = read_csv('edge_person_netcode.csv', qq_smz_schema)
    df1 = tmp.na.fill('').dropDuplicates(['sfzh', 'qq']) \
        .selectExpr('sfzh', 'qq', '0 start_time', '0 end_time')
    write_orc(df1, add_save_path('edge_person_own_qq',root=save_path))

    df2 = tmp.na.fill('').dropDuplicates(['sfzh', 'wechat']) \
        .selectExpr('sfzh', 'wechat', '0 start_time', '0 end_time')
    write_orc(df2, add_save_path('edge_person_own_wechat',root=save_path))

    df3 = tmp.na.fill('').dropDuplicates(['sfzh', 'phone']) \
        .selectExpr('sfzh start_person', 'phone end_phone', '0 start_time', '0 end_time')
    write_orc(df3, add_save_path('edge_person_smz_phone', root=save_path))

    ## 提取人节点
    p1 = read_orc(add_save_path('vertex_person'))
    p2 = df1.select('sfzh').union(df2.select('sfzh'))

    p = p2.join(p1, p2.sfzh == p1.zjhm, 'left') \
        .selectExpr('sfzh zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                    'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz').na.fill('')
    res = p.drop_duplicates(['zjhm'])
    write_orc(res, add_save_path('vertex_person',root=save_path))


def qq_friends():
    we_link = StructType([
        StructField("wechat1", StringType(), True),
        StructField("wechat2", StringType(), True),
        StructField("id", StringType(), True),
    ])

    qq_link = StructType([
        StructField("qq1", StringType(), True),
        StructField("qq2", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
    ])


    tmp = read_csv('edge_wechat_friend_wechat.csv',we_link)
    df = tmp.drop_duplicates(['wechat1','wechat2']) \
        .selectExpr('wechat1','wechat2','0 start_time','0 end_time')
    write_orc(df,add_save_path('edge_wechat_link_wechat',root=save_path))

    tmp = read_csv('edge_qq_friend_qq.csv', qq_link)
    df = tmp.drop_duplicates(['qq1', 'qq2']) \
        .selectExpr('qq1', 'qq2', '0 start_time', '0 end_time')
    write_orc(df, add_save_path('edge_qq_link_qq', root=save_path))


def vip_call():
    call_schema = StructType([
        StructField("call_duration", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("call_type", StringType(), True),
        StructField("start_phone", StringType(), True),
        StructField("end_phone", StringType(), True),
    ])


    tmp = read_csv('call_detail.csv',call_schema)
    call = tmp.where('call_type in ("65","66")') \
            .selectExpr('if(call_type="65",start_phone,end_phone) start_phone',
                        'if(call_type="66",start_phone,end_phone) end_phone',
                        'cast(date2timestampstr(start_time) as bigint) start_time',
                        'cast(call_duration as bigint) call_duration') \
            .selectExpr('format_phone(start_phone) start_phone','format_phone(end_phone) end_phone',
                        'start_time','start_time+call_duration end_time','call_duration') \
            .where('verify_phonenumber(start_phone) = 1 and verify_phonenumber(end_phone) = 1')
    call.createOrReplaceTempView('tmp')
    sql = '''
                select start_phone,end_phone,min(start_time) start_time,
                max(end_time) end_time, sum(call_duration) call_total_duration , 
                count(1) call_total_times from tmp
                group by start_phone, end_phone
            '''
    groupcall = spark.sql(sql)
    write_orc(groupcall,add_save_path('edge_groupcall',root=save_path))

    msg = tmp.where('call_type in ("49","50")').where('verify_phonenumber(start_phone) = 1 and verify_phonenumber(end_phone) = 1') \
            .selectExpr('format_phone(start_phone) start_phone','format_phone(end_phone) end_phone',
                       'cast(date2timestampstr(start_time) as bigint) start_time') \
            .selectExpr('start_phone','end_phone','start_time','start_time+1 end_time')
    msg.createOrReplaceTempView('tmp')
    sql = '''
                    select start_phone,end_phone,min(start_time) start_time,
                    max(end_time) end_time, count(1) message_number from tmp
                    group by start_phone, end_phone
                '''
    groupmsg = spark.sql(sql)
    write_orc(groupmsg, add_save_path('edge_groupmsg',root=save_path))





if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # vertex_qq()
    qq_smz()
    # qq_friends()
    # vip_call()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))