# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : company.py
# @Software: PyCharm
# @content : 工商相关信息

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
conf=SparkConf().set('spark.driver.maxResultSize', '2g')
conf.set('spark.yarn.executor.memoryOverhead', '30g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '2g')
conf.set('spark.executor.instances', 10)
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

##todo:all
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'

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

def person_relate_phone():
    # mob 手机号
    # rel_mob 关系手机号
    # send_count 手机号发送次数
    # send_days 手机号发送天数
    # recv_count 手机号接收次数
    # recv_days 手机号接收天数
    # rel_send_count 关系手机号发送次数
    # rel_send_days 关系手机号发送天数
    # rel_recv_count 关系手机号接收次数
    # rel_recv_days 关系手机号接收天数
    # last_dis_place 最后一次发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    ## 短信通联关系

    # init(spark, 'nb_app_dws_res_res_his_message', if_write=False, is_relate=True)
    # sql = '''
    #         select mob start_phone, rel_mob end_phone, send_count, recv_count, rel_send_count,
    #         rel_recv_count, first_time start_time,last_time end_time from nb_app_dws_res_res_his_message
    #     '''
    # resource = spark.sql(sql)
    # col1 = ['start_phone','end_phone','send_count num','if(start_time>0,start_time,0) start_time',
    #         'if(end_time>0,end_time,0) end_time']
    # df1 = resource.where('send_count > 0').selectExpr(*col1)
    #
    # col2 = ['start_phone','end_phone','send_count num','if(start_time>0,start_time,0) start_time',
    #         'if(end_time>0,end_time,0) end_time']
    # df2 = resource.where('rel_send_count > 0').selectExpr(*col2)
    # res = df1.unionAll(df2)
    # write_orc(res,add_save_path('edge_groupmsg_detail',root=save_root))

    ## 电话通联关系
    # mob 手机
    # rel_mob 关系手机
    # call_dur 通话总时长，单位是秒
    # send_count 主手机号发送总次数
    # send_days 主手机号发送总天数
    # recv_count 主手机号接收总次数
    # recv_days 主手机号接收总天数
    # rel_send_count 关系手机号发送总次数
    # rel_send_days 关系手机号发送总天数
    # rel_recv_count 关系手机号接收总次数
    # rel_recv_days 关系手机号接收总天数
    # rel_type 关系类型
    # last_dis_place 发现地
    # first_time 次发现时间
    # last_time 最后发现时间

    # init(spark, 'nb_app_dws_res_res_his_mobilecall', if_write=False, is_relate=True)
    # sql = '''
    #         select mob start_phone, rel_mob end_phone, send_count, recv_count, rel_send_count,
    #         rel_recv_count, call_dur, cast(first_time as bigint) start_time,cast(last_time as bigint) end_time from nb_app_dws_res_res_his_mobilecall
    #         '''
    # resource = spark.sql(sql)
    # col1 = ['start_phone', 'end_phone', 'send_count num', 'call_dur',
    #         'if(start_time>0,start_time,0) start_time', 'if(end_time>0,end_time,0) end_time']
    # df1 = resource.where('send_count > 0').selectExpr(*col1)
    #
    # col2 = ['end_phone start_phone', 'start_phone end_phone', 'rel_send_count num', 'call_dur',
    #         'if(start_time>0,start_time,0) start_time', 'if(end_time>0,end_time,0) end_time']
    # df2 = resource.where('rel_send_count > 0').selectExpr(*col2)
    # res = df1.unionAll(df2)
    # write_orc(res, add_save_path('edge_groupcall_detail', root=save_root))

    ## 电话寄递关系
    # mob 手机号
    # rel_mob 关系手机号
    # first_maling_time 首次寄件时间
    # last_maling_time 末次寄件时间
    # send_count 手机号寄件次数
    # recv_count 手机号收件次数
    # rel_send_count 系手机号寄件次数
    # rel_recv_count 关系手机号收件次数
    # last_dis_place 最后发现地
    # first_time 次发现时间
    # last_time 后发现时间

    ## 手机寄递关系 todo 可能存在无法朔源的情况
    init(spark, 'nb_app_dws_res_res_his_mailing', if_write=False, is_relate=True)
    sql = '''
        select format_phone(mob) start_phone, format_phone(rel_mob) end_phone, send_count, recv_count, rel_send_count,
            rel_recv_count, cast(first_maling_time as bigint) start_time, cast(last_maling_time as bigint) end_time 
            from nb_app_dws_res_res_his_mailing where verify_phonenumber(mob) = 1 and verify_phonenumber(rel_mob) = 1
    '''
    cols1 = ['start_phone','end_phone','if(start_time>0,start_time,0) start_time','if(end_time>0,end_time,0) end_time']
    source = spark.sql(sql)
    df1 = source.where('send_count > 0 and start_phone != end_phone').selectExpr(*cols1)

    cols2 = ['end_time start_phone ','start_time end_phone','if(start_time>0,start_time,0) start_time','if(end_time>0,end_time,0) end_time']
    df2 = source.where('recv_count > 0 and start_phone != end_phone').selectExpr(*cols2)

    df = df1.unionAll(df2).dropDuplicates()
    write_orc(df,add_save_path('edge_phone_sendpackage_phone_detail',root=save_root))

    df.createOrReplaceTempView('tmp')
    sql2 = '''
            select start_phone, end_phone,min(start_time) start_time,max(end_time) end_time,
            count(1) num from tmp 
            where verify_phonenumber(start_phone) = 1 and verify_phonenumber(end_phone) = 1
            and start_phone != end_phone group by start_phone,end_phone
        '''

    df = spark.sql(sql2)

    write_orc(df,add_save_path('edge_phone_sendpackage_phone',root=save_root))
    logger.info('edge_phone_sendpackage_phone down')



if __name__ == "__main__":
    '''暂时不用此数据'''
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    person_relate_phone()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))