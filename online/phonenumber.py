# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 15:35
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : phonenumber.py
# @Software: PyCharm

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.rdd import RDD
import logging
from pyspark import SparkConf
import socket
import os,sys

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

conf=SparkConf().set('spark.driver.maxResultSize', '15g')
conf.set('spark.yarn.am.cores', 5)
conf.set('soark.shuffle.partitions',800)
conf.set('spark.executor.memory', '20g')
conf.set('spark.executor.instances', 50)
conf.set('spark.executor.cores', 8)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')


spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commonUtil = CommonUdf(spark)
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/phonenumber'

##todo:all
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

zj_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/zj_location.csv'
sj_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/sj_location.csv'

def vertex_phonenumber():

    table_list = [
        {'tablename': 'edge_groupcall', 'phone': 'start_phone'},
        {'tablename': 'edge_groupcall', 'phone': 'end_phone'},
        {'tablename': 'edge_groupmsg', 'phone': 'start_phone'},
        {'tablename': 'edge_groupmsg', 'phone': 'end_phone'},
        {'tablename': 'edge_phone_call_alert', 'phone': 'phone'},
        {'tablename': 'edge_phone_send_package', 'phone': 'phone'},
        {'tablename': 'edge_phone_receive_package', 'phone': 'phone'},
        {'tablename': 'edge_phone_sendpackage_phone', 'phone': 'start_phone'},
        {'tablename': 'edge_phone_sendpackage_phone', 'phone': 'end_phone'},
        {'tablename': 'edge_person_open_phone', 'phone': 'phone'},
        {'tablename': 'edge_person_link_phone', 'phone': 'phone'},
    ]

    ## 网安临时关联表
    # table_list = [
    #     {'tablename': 'edge_groupcall_tmp', 'phone': 'start_phone'},
    #     {'tablename': 'edge_groupcall_tmp', 'phone': 'end_phone'},
    #     {'tablename': 'edge_groupmsg_tmp', 'phone': 'start_phone'},
    #     {'tablename': 'edge_groupmsg_tmp', 'phone': 'end_phone'},
    #     {'tablename': 'edge_person_smz_phone_tmp', 'phone': 'end_phone'},
    # ]

    tmp_sql = ''' select {phone} phone,"{tablename}" tablename from {tablename} '''

    dfs = []
    for info in table_list:
        sql = tmp_sql.format(**info)
        create_tmpview_table(spark, info['tablename'])
        df = spark.sql(sql)
        dfs.append(df)

    union_df = reduce(lambda x, y: x.unionAll(y), dfs)

    union_df = union_df.drop_duplicates(['phone'])

    df = union_df.selectExpr('phone', 'tablename','substr(phone,1,7) as area',
                             'substr(phone,1,1) as pre1','substr(phone,1,3) as pre3',
                             'substr(phone,1,3) as pre4')

    sj_location = spark.read.csv(sj_path,header=True,sep='\t')
    df_sj = df.where('pre1 != "0"')
    df_sj_res = df_sj.join(sj_location, df_sj.area == sj_location.pre7, 'left') \
        .select(df_sj.phone, sj_location.province, sj_location.city,df_sj.tablename)
    write_parquet(df_sj_res,'df_sj_res')

    # 座机的数据-010 020 021 022 023 024 025 027 028 029  3位区号的
    df_zj1 = df.where("pre1=='0' and pre3 in('010','020','021','022','023','024','025','027','028','029')")

    zj_location = spark.read.csv(zj_path,header=True,sep='\t')

    df_zj1_res = df_zj1.join(zj_location, df_zj1.pre3 == zj_location.qhao, 'left') \
        .select(df_zj1.phone, zj_location.province, zj_location.city, df_zj1.tablename)
    df_zj1_res = df_zj1_res.na.fill({'province': '', 'city': ''})

    write_parquet(df_zj1_res, 'df_zj1_res')
    logger.info(' df_zj1_res down')

    # 座机4位区号的
    df_zj2 = df.where("pre1=='0' and pre3 not in('010','020','021','022','023','024','025','027','028','029')")
    df_zj2_res = df_zj2.join(zj_location, df_zj2.pre4 == zj_location.qhao, 'left') \
        .select(df_zj2.phone, zj_location.province, zj_location.city, df_zj2.tablename)
    df_zj2_res = df_zj2_res.na.fill({'province': '', 'city': ''})
    write_parquet(df_zj2_res, 'df_zj2_res')
    logger.info(' df_zj2_res down')

    df_zj2_res = read_parquet('df_zj2_res')
    df_zj1_res = read_parquet('df_zj1_res')
    df_sj_res = read_parquet('df_sj_res')

    res = df_sj_res.unionAll(df_zj1_res).unionAll(df_zj2_res).drop_duplicates(['phone'])
    res = res.selectExpr('phone','"" xm','province','city','tablename')
    write_orc(res,add_save_path('vertex_phonenumber'))
    logger.info('vertex_phonenumber down')

def drup_number_etl():
    path = '/phoebus/_fileservice/users/slmp/shulianmingpin/drug_number.csv'

    all_number = read_orc(add_save_path('vertex_phonenumber'))
    drug = spark.read.csv(path,header=True)
    df = all_number.intersect(drug)
    logger.info(df.count())

    groupcall = read_orc(add_save_path('edge_groupcall'))
    drug.createOrReplaceTempView('dtmp')
    groupcall.createOrReplaceTempView('gtmp')

    pass

def sxwa_text():
    columns = ['start_phone','end_phone','start_time','call_duration','call_type']
    path = '/phoebus/_fileservice/users/slmp/shulianmingpin/txtfile'
    text = spark.sparkContext.textFile(path)
    df = text.map(lambda line: line.split(',')).map(lambda row: [row[1],row[4],row[0],row[5],row[6]]) \
            .filter(lambda x: not u'短信' in x[4]).toDF(columns)
    df.show()
    res = df.where('verify_phonenumber(start_phone)=1 and verify_phonenumber(end_phone)=1') \
            .selectExpr('format_phone(start_phone) start_phone','format_phone(end_phone) end_phone',
                        'date2timestampstr(start_time) start_time','call_duration','call_type') \
            .selectExpr('case when call_type = "主叫" then start_phone else end_phone end as start_phone',
                  'case when call_type = "被叫" then start_phone else end_phone end as end_phone',
                  'cast(start_time as bigint) start_time',
                  'cast(start_time as bigint)+cast(call_duration as bigint) end_time',
                  'cast(call_duration as bigint) call_duration')
    res = res.where('call_duration > 0')
    write_orc(res,add_save_path('edge_groupcall_detail_sxwa'))

    create_tmpview_table('edge_groupcall_detail_sxwa')

    sql = '''
            select start_phone,end_phone,min(start_time) start_time,
            max(end_time) end_time, sum(call_duration) call_total_duration , 
            count(1) call_total_times from edge_groupcall_detail_sxwa
            group by start_phone, end_phone
        '''

    df = spark.sql(sql)
    write_orc(df, add_save_path('edge_groupcall_sxwa'))

def deal_smz():
    path = '/phoebus/_fileservice/users/slmp/shulianmingpin/person_phone_res.csv'
    smz = spark.read.csv(path).selectExpr('_c0 start_person','_c1 end_phone','0 start_time','0 end_time')
    res = smz.drop_duplicates()
    write_orc(res,add_save_path('edge_person_smz_phone_sxwa'))

    p1 = read_orc(add_save_path('vertex_person'))
    p2 = read_orc(add_save_path('edge_person_smz_phone_sxwa'))

    p = p2.join(p1,p2.start_person==p1.zjhm,'left') \
            .selectExpr('start_person zjhm','zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                            'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz').na.fill('')
    res = p.drop_duplicates(['zjhm'])
    write_orc(res,add_save_path('vertex_person_sxwa'))


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # sxwa_text()
    # deal_smz()
    vertex_phonenumber()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
