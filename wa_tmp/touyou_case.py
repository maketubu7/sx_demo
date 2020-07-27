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

def add_save_path(tablename, cp='', root='extenddir'):
    '''
    保存hive外表文件，分区默认为cp=2020，root为extenddir
    :param tablename:
    :param cp:
    :return:
    '''
    if cp:
        tmp_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/{}/{}/cp={}'
        return tmp_path.format(root, tablename.lower(), cp)
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/{}/{}/cp=2020'.format(root, tablename.lower())

def read_orc(path):
    try:
        df = spark.read.orc(path)
        if df.take(1):
            return df
    except:
        return None

def read_csv(file,schema):
    path= '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/touyou_case/'+file+'.csv'
    return spark.read.csv(path, schema=schema,header=None)

def exists_travel_zjhm(df,zjhms,start_key='sfzh1',end_key='sfzh2'):
    ''' 双向一度 任一节点满足 '''
    df.createOrReplaceTempView('all')
    zjhms.createOrReplaceTempView('zjhms')

    sql = ''' select /*+ BROADCAST (zjhms)*/all.* from all where (select count(1) as num from zjhms where all.%s = zjhms.zjhm) != 0
                    or (select count(1) as num from zjhms where all.%s = zjhms.zjhm) != 0 '''%(start_key,end_key)

    res = spark.sql(sql)
    return res


def inner_df_zjhm(df, zjhms, all_key='sfzh',find_key='zjhm'):
    ''' 是否存在 '''
    df.createOrReplaceTempView('all')
    zjhms.createOrReplaceTempView('zjhms')

    sql = ''' select /*+ BROADCAST (zjhms)*/all.* from all inner join zjhms on all.%s=zjhms.%s''' % (all_key,find_key)

    res = spark.sql(sql)
    return res


def find_one_degree_call():

    phone_schema = StructType([
        StructField("phone", StringType(), True)
    ])

    phones = read_csv('phone',phone_schema)
    edge_groupcall_detail = read_orc(add_save_path('edge_groupcall_detail'))

    phones.createOrReplaceTempView('phones')
    edge_groupcall_detail.createOrReplaceTempView('call')

    sql = ''' select /*+ BROADCAST (phones)*/call.* from call where (select count(1) as num from phones where call.start_phone = phones.phone) != 0
                or (select count(1) as num from phones where call.end_phone = phones.phone) != 0 '''

    res1 = spark.sql(sql)

    one_degree_phones = (res1.selectExpr('start_phone phone').union(res1.selectExpr('end_phone phone'))).subtract(phones)

    one_degree_phones.createOrReplaceTempView('other_phone')

    sql = '''
            select /*+ BROADCAST (other_phone)*/ a.* from call a
            inner join 
            other_phone b
            inner join 
            other_phone c
            on a.start_phone=b.phone and a.end_phone=c.phone
        '''
    res2 = spark.sql(sql)

    res = res1.unionAll(res2)
    write_orc(res,root_path+'edge_groupcall_detail/cp=2020',)

    call_detail = read_orc(root_path+'edge_groupcall_detail/cp=2020')
    call_detail.createOrReplaceTempView('call_detail')

    sql = '''
            select start_phone,end_phone,min(start_time) start_time,
            max(end_time) end_time, sum(call_duration) call_total_duration , 
            count(1) call_total_times from call_detail
            group by start_phone, end_phone
        '''

    df = spark.sql(sql)
    write_orc(df,root_path+'edge_groupcall/cp=2020',)

def find_all_phone_dw():
    cps = ['2020051800', '2020051900', '2020052000', '2020052100', '2020052200',
           '2020052300', '2020052400', '2020052500', '2020052600', '2020052700']
    call = read_orc(root_path+'edge_groupcall/cp=2020')

    phones = (call.selectExpr('start_phone phone').union(call.selectExpr('end_phone phone'))).distinct()
    phones.unpersist()
    phones.createOrReplaceTempView('phones')
    for cp in cps:
        df = read_orc(add_save_path('bbd_dw_detail_tmp',cp=cp))
        df.createOrReplaceTempView('dw')
        sql = ''' select /*+ BROADCAST (phones)*/ dw.* from dw inner join phones on dw.phone=phones.phone '''
        res = spark.sql(sql)
        write_orc(res,root_path+'case_one_degree_dw/cp=%s'%cp)
        logger.info('%s dw down'%cp)
    phones.unpersist()

    # smz = read_orc(add_save_path('edge_person_smz_phone_top'))
    #
    # smz_res = smz.join(phones,phones['phone']==smz['end_phone'],'inner') \
    #             .selectExpr('start_person','end_phone','0 start_time','0 end_time')
    #
    # write_orc(smz_res, root_path + 'edge_person_smz_phone/cp=2020', )

def vertex_case_info():
    cols = ['ajbh asjbh','ajmc','0 asjfskssj','0 asjfsjssj','"" asjly','""ajlb','0 fxasjsj','"" fxasjdd_dzmc',
                                        'jyaq','"" ajbs','0 larq']
    df = spark.read.csv(root_path+'vertex_case.csv',header=True, sep='\t').selectExpr(*cols)
    res = df.drop_duplicates(['asjbh'])
    write_orc(res,root_path+'vertex_case/cp=2020')

    ## 人节点
    p1 = read_orc(add_save_path('vertex_person'))
    p_smz = read_orc(root_path+'edge_person_smz_phone/cp=2020').selectExpr('start_person')
    p_links =  read_orc(root_path+'edge_case_link_person/cp=2020').selectExpr('sfzh start_person')
    p_airline1 = read_orc(root_path+'edge_person_with_airline_travel/cp=2020').selectExpr('sfzh1 start_person')
    p_airline2 = read_orc(root_path+'edge_person_with_airline_travel/cp=2020').selectExpr('sfzh2 start_person')
    p_train1 = read_orc(root_path+'edge_person_with_trainline_travel/cp=2020').selectExpr('sfzh1 start_person')
    p_train2 = read_orc(root_path+'edge_person_with_trainline_travel/cp=2020').selectExpr('sfzh2 start_person')
    p_house1 = read_orc(root_path+'edge_same_hotel_house/cp=2020').selectExpr('sfzh1 start_person')
    p_house2 = read_orc(root_path+'edge_same_hotel_house/cp=2020').selectExpr('sfzh2 start_person')
    p_com = read_orc(root_path+'edge_person_legal_com/cp=2020').selectExpr('start_person')
    dfs = [p_smz,p_links,p_airline1,p_airline2,p_train1,p_train2,p_house1,p_house2,p_com]
    all_p = reduce(lambda a,b:a.unionAll(b),filter(lambda a:a,dfs))
    p2 = all_p.distinct()
    p = p2.join(p1, p2.start_person == p1.zjhm, 'left') \
        .selectExpr('start_person zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                    'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz').na.fill('')
    res = p.drop_duplicates(['zjhm'])
    write_orc(res, root_path+'vertex_person/cp=2020')

    ## 公司节点
    coms = read_orc(root_path+'edge_person_legal_com/cp=2020').selectExpr('end_company').distinct()
    all_com = read_orc(add_save_path('vertex_company'))
    res = inner_df_zjhm(all_com,coms,all_key='company',find_key='end_company')
    write_orc(res,root_path+'vertex_company/cp=2020')

def edge_info():
    ## 案件关联人
    col1 = ['ajbh','zjhm sfzh','0 start_time','0 end_time']
    df1 = spark.read.csv(root_path+'case_link_person.csv',header=True, sep='\t').selectExpr(*col1)
    write_orc(df1,root_path+'edge_case_link_person/cp=2020')
    ## 案件关联电话
    col2 = ['ajbh', 'phone', '0 start_time', '0 end_time']
    df2 = spark.read.csv(root_path + 'case_link_phone.csv', header=True, sep='\t').selectExpr(*col2)
    write_orc(df2, root_path + 'edge_case_link_phone/cp=2020')

def person_do_something():
    zjhms = read_orc(root_path +'edge_person_smz_phone/cp=2020').select('start_person zjhm').distinct()

    ## 同出行信息，同房
    airline_travel = read_orc(add_save_path('edge_person_with_airline_travel'))
    trainline_travel = read_orc(add_save_path('edge_person_with_trainline_travel'))
    same_hotel_house = read_orc(add_save_path('edge_same_hotel_house'))
    write_orc(exists_travel_zjhm(airline_travel,zjhms),root_path+'edge_person_with_airline_travel/cp=2020')
    write_orc(exists_travel_zjhm(trainline_travel,zjhms),root_path+'edge_person_with_trainline_travel/cp=2020')
    write_orc(exists_travel_zjhm(same_hotel_house,zjhms),root_path+'edge_same_hotel_house/cp=2020')

    ## 工商信息
    legal_com = read_orc(add_save_path('edge_person_legal_com'))
    write_orc(inner_df_zjhm(legal_com,zjhms,all_key='start_person'),root_path+'edge_person_legal_com/cp=2020')

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # find_one_degree_call()
    find_all_phone_dw()
    # edge_info()
    # vertex_case_info()
    # person_do_something()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))