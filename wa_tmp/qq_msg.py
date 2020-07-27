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
    path= '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/touyou_case/'+file
    return spark.read.csv(path, schema=schema)

def vertex_phone():
    qq_schema = StructType([
        StructField("qq", StringType(), True),
        StructField("nickname", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("city", StringType(), True),
        StructField("mobile", StringType(), True),
        StructField("email", StringType(), True),
        StructField("realname", StringType(), True),
        StructField("csrq", StringType(), True),
        StructField("college", StringType(), True),
        StructField("area", StringType(), True),
        StructField("province", StringType(), True),
        StructField("regis_time", StringType(), True),
        StructField("regis_ip", StringType(), True),
        StructField("study", StringType(), True),
        StructField("sign_name", StringType(), True),
    ])

    group_schema = StructType([
        StructField("groupid", StringType(), True),
        StructField("groupname", StringType(), True),
        StructField("count", StringType(), True),
        StructField("masterid", StringType(), True),
        StructField("groupflag", StringType(), True),
        StructField("groupclass", StringType(), True),
        StructField("introduction", StringType(), True),
        StructField("annment", StringType(), True),
        StructField("create_time", StringType(), True),
        StructField("last_time", StringType(), True),
    ])
    # qq_names = ['qq', 'nickname', 'sex', 'city', 'mobile', 'email', 'realname', 'csrq',
    #             'college', 'area', 'province', 'regis_time', 'regis_ip', 'study', 'sign_name']
    tmp = read_csv('all_qq_res.csv',qq_schema)
    df = tmp.na.fill('').dropDuplicates(['qq'])
    write_orc(df,add_save_path('vertex_qq'))

    tmp = read_csv('all_group_res.csv',group_schema)
    df = tmp.na.fill('').dropDuplicates(['groupid'])
    write_orc(df, add_save_path('vertex_qq_group'))

def edge_qq_link_group():

    link_schema = StructType([
        StructField("qq", StringType(), True),
        StructField("groupid", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
    ])

    friend_schema = StructType([
        StructField("qq1", StringType(), True),
        StructField("qq2", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
    ])

    tmp = read_csv('all_attend_res.csv',link_schema)
    df = tmp.na.fill('').dropDuplicates(['qq','groupid']) \
            .selectExpr('qq','groupid','cast(start_time as bigint) start_time','cast(end_time as bigint) end_time')
    write_orc(df, add_save_path('edge_qq_link_group'))

    tmp = read_csv('all_qq_friend_res.csv', friend_schema)
    df = tmp.selectExpr('if(qq1>qq2,qq2,qq1) qq1','if(qq1<qq2,qq2,qq1) qq2',
                        'cast(start_time as bigint) start_time','cast(end_time as bigint) end_time') \
                .na.fill('').dropDuplicates(['qq1', 'qq2']).where('qq1 != qq2')
    write_orc(df, add_save_path('edge_qq_link_qq'))

def qq_smz():

    qq_smz_schema = StructType([
        StructField("sfzh", StringType(), True),
        StructField("qq", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
    ])

    tmp = read_csv('person_own_qq.csv', qq_smz_schema)
    df = tmp.na.fill('').dropDuplicates(['sfzh', 'qq']) \
        .selectExpr('sfzh', 'qq', 'cast(start_time as bigint) start_time', 'cast(end_time as bigint) end_time')
    write_orc(df, add_save_path('edge_person_own_qq'))

    ## 提取人节点
    p1 = read_orc(add_save_path('vertex_person'))
    p2 = read_orc(add_save_path('edge_person_own_qq'))

    p = p2.join(p1, p2.sfzh == p1.zjhm, 'left') \
        .selectExpr('sfzh zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                    'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz').na.fill('')
    res = p.drop_duplicates(['zjhm'])
    write_orc(res, add_save_path('vertex_person_qq'))


def find_common_group():
    ## 推理QQ同群
    qq_link = read_orc(add_save_path('edge_qq_link_qq'))
    qq_group = read_orc(add_save_path('edge_qq_link_group'))

    group_ids = qq_group.groupby('qq').agg(collect_set('groupid').alias('ids'))
    group_ids.createOrReplaceTempView('group')
    qq_link.select('qq1','qq2').createOrReplaceTempView('friend')
    sql = '''
    select t1.qq1, t1.qq2, t1.idas, t2.ids idbs from 
    (select a.qq1,a.qq2,b.ids idas from friend a left join group b on a.qq1 = b.qq where b.ids is not null) t1 
    left join group t2 on t1.qq2=t2.qq where t2.ids is not null
    '''

    res = spark.sql(sql).selectExpr('qq1','qq2','find_ids_dup(idas,idbs) num') \
            .where('num > 0')
    res = res.selectExpr('if(qq1>qq2,qq2,qq1) qq1','if(qq1<qq2,qq2,qq1) qq2','num') \
            .drop_duplicates(['qq1','qq2']).where('qq1 != qq2')
    write_orc(res,add_save_path('find_qq_common_group'))

def vip_call():
    call_schema = StructType([
        StructField("start_phone", StringType(), True),
        StructField("end_phone", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("call_duration", StringType(), True),
        StructField("call_type", StringType(), True),
        StructField("type", StringType(), True),
    ])


    tmp = read_csv('vip_call.csv',call_schema)
    call = tmp.where('type="话单" and call_type in ("65","66")') \
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
    write_orc(groupcall,add_save_path('edge_groupcall_qq'))
    msg = tmp.where('type="短信"').where('verify_phonenumber(start_phone) = 1 and verify_phonenumber(end_phone) = 1') \
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
    write_orc(groupmsg, add_save_path('edge_groupmsg_qq'))

def vip_swtl_call():
    call_schema = StructType([
        StructField("start_phone", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("call_type", StringType(), True),
        StructField("end_phone", StringType(), True),
        StructField("call_duration", StringType(), True),
    ])

    tmp = read_csv('wstl.csv',call_schema)

    call = tmp.where('call_type in ("1","2")') \
        .selectExpr('if(call_type="1",start_phone,end_phone) start_phone',
                    'if(call_type="2",start_phone,end_phone) end_phone',
                    'cast(date2timestampstr(start_time) as bigint) start_time',
                    'cast(call_duration as bigint) call_duration') \
        .selectExpr('format_phone(start_phone) start_phone', 'format_phone(end_phone) end_phone',
                    'start_time', 'start_time+call_duration end_time', 'call_duration') \
        .where('verify_phonenumber(start_phone) = 1 and verify_phonenumber(end_phone) = 1')
    call.createOrReplaceTempView('tmp')
    sql = '''
                    select start_phone,end_phone,min(start_time) start_time,
                    max(end_time) end_time, sum(call_duration) call_total_duration , 
                    count(1) call_total_times from tmp
                    group by start_phone, end_phone
                '''
    groupcall1 = spark.sql(sql)
    groupcall2 = read_orc(add_save_path('edge_groupcall_qq'))
    groupcall = groupcall1.unionAll(groupcall2)
    write_orc(groupcall, add_save_path('edge_groupcall_wstl'))

def phone_smz():
    path = '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/qq_data/person_phone_res.csv'
    smz = spark.read.csv(path).selectExpr('_c0 start_person','_c1 end_phone','0 start_time','0 end_time')
    res = smz.drop_duplicates()
    write_orc(res,add_save_path('edge_person_smz_phone_tmp'))

    p1 = read_orc(add_save_path('vertex_person'))
    p2 = spark.read.csv(path).selectExpr('_c0 start_person', '_c2 name')

    p = p2.join(p1,p2.start_person==p1.zjhm,'left') \
            .selectExpr('start_person zjhm','zjlx', 'gj', 'name xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                            'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz').na.fill('')
    res = p.drop_duplicates(['zjhm'])
    write_orc(res,add_save_path('vertex_person_tmp'))

def union_jg():

    jg_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_data/'
    tmp_jg_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/sd/jg_data/'
    columns = ['start_phone','end_phone','start_time','end_time','call_total_duration','call_total_times']
    df = read_orc(jg_path+'edge_groupcall_sxwa_jg').selectExpr(*columns)
    write_orc(df,add_save_path('edge_groupcall_sxwa'))


    files_1 = ['edge_groupcall_sxwa']
    files_2 = ['edge_groupcall_wstl']

    for i in range(len(files_1)):
        df1 = read_orc(add_save_path(files_1[i]))
        df2 = read_orc(add_save_path(files_2[i]))
        df = df1.unionAll(df2)
        df.createOrReplaceTempView('tmp')
        sql = '''
                        select start_phone,end_phone,min(start_time) start_time,
                        max(end_time) end_time, sum(call_total_duration) call_total_duration , 
                        sum(call_total_times) call_total_times from tmp
                        group by start_phone, end_phone
                    '''
        res = spark.sql(sql)
        # res.show()
        write_orc(res,add_save_path('edge_groupcall_tmp'))


def group_call_intersect():
    phones = spark.sparkContext \
        .textFile('/phoebus/_fileservice/users/slmp/shulianmingpin/fh_data_share/data_phone.txt') \
        .map(lambda line: line.split('\t')).map(lambda row: (row[2], row[4])).toDF(['sfzh', 'phone'])
    phone = phones.selectExpr('phone').dropDuplicates()

    phone.createOrReplaceTempView('v_phone')
    create_tmpview_table(spark,'edge_groupcall_all')

    sql = '''
        select a.start_phone, a.end_phone, start_time, end_time, call_total_duration, 
        call_total_times from edge_groupcall_all a
        inner join 
        v_phone b
        inner join 
        v_phone c
        on a.start_phone=b.phone and a.end_phone=c.phone
    '''

    res = spark.sql(sql)
    write_orc(res,add_save_path('sd_intersect_call',root='wa_data'))

def extract_tl_num():
    create_tmpview_table(spark,'sd_intersect_call')
    sql = '''
        select a.start_phone phone,b.num start_num, c.num end_num from v_phone a
        left join
        start_tmp b
        left join 
        end_tmp c
        on a.start_phone=b.start_phone or a.start_phone = c.end_phone
    '''



if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # vertex_qq()
    # edge_qq_link_group()
    # find_common_group()
    # qq_smz()
    # vip_call()
    # union_jg()
    # phone_smz()
    # vip_call()
    # vip_swtl_call()
    union_jg()
    # qq_smz()
    # group_call_intersect()
    # vip_swtl_call()
    # union_jg()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))