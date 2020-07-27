# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 15:03
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : package.py
# @Software: PyCharm
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
conf.set('spark.yarn.executor.memoryOverhead', '10g')
conf.set('soark.shuffle.partitions',800)
conf.set('soark.shuffle.io.retryWait','10s')
conf.set('spark.executor.memory', '10g')
conf.set('spark.executor.instances', 100)
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


path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/package'
def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix,'parquet/',path))
    return df
def write_parquet(df,path):
    '''写 parquet'''
    df.write.mode("overwrite").parquet(path=os.path.join(path_prefix,'parquet/',path))


def write_orc(df, path):
    df.write.format('orc').mode('overwrite').save(path)
    logger.info('write success')


def read_orc(path):
    df = spark.read.orc(path)
    return df


#todo:all 数据量过大分区文件有些为空，异常处理
def deal_package_source_day(import_times,cp):
    '''
    cp= 2020030000 的分区文件因存储不足 无法落地
    处理每天所有的快递基本信息
    字段 包含快递节点的属性信息，以及两端的接收信息
    :return:
    '''

    table_list = [
        ##orderid [waybill_no,order_no] 2020032800 order_no
        ## 2020032900 - now waybill_no
        {'company_name':'expr_comp_comp_name','company_code':'expr_comp_com_abb','orderid':'waybill_no',
         'accepttime':'send_good_time','inputtime':'deliv_time','item_name':'mai_goods_name','item_type':'maiitem_clas_name',
         'senderphone':'coalesce_str(sener_mob,sener_fixed_tel)','senderaddress':'sener_addr_name',
         'receiverphone':'coalesce_str(rece_per_ctct_tel,rece_per_fixed_tel)','receiveraddress':'rece_per_addr_name',
         'tablename':'ods_soc_lgs_expr_integ_info','cond':'trim(waybill_no) != "" and trim(expr_comp_com_abb) != "" and   '
         }
    ]

    tmp_sql = '''
            select md5(concat({company_code},{orderid})) packageid, trim({company_name}) company,
            {orderid} orderid, cast(format_timestamp({accepttime}) as bigint) accepttime, 
            cast(format_timestamp({inputtime}) as bigint) inputtime,{item_name} item_name, {item_type} item_type, 
            format_phone({senderphone}) senderphone,{senderaddress} senderaddress, 
            format_phone({receiverphone}) receiverphone, {receiveraddress} receiveraddress,
            '{tablename}' tablename from {tablename}
            where trim({orderid}) != '' and trim({company_code}) != ''
        '''

    for info in table_list:
        init_history(spark,info['tablename'],import_times,if_write=False)

        sql = tmp_sql.format(**info)
        df = spark.sql(sql).drop_duplicates(['packageid']).repartition(10)
        if df.take(1):
            write_orc(df,add_incr_path('package_source_detail',cp=cp))
            logger.info('package_source_detail %s down'%cp)

def deal_vertex_package():
    '''
    处理每天所有的快递基本信息
    字段 包含快递节点的属性信息，以及两端的接收信息
    :return:
    '''
    create_tmpview_table(spark,'package_source_detail',root='incrdir')

    sql = '''
        select packageid, company,orderid, accepttime, inputtime,
        `replace`(item_name,'0','') item_name, item_type, senderphone, senderaddress, 
        receiverphone,receiveraddress, tablename from package_source_detail
    '''
    df = spark.sql(sql).drop_duplicates(['packageid'])

    write_orc(df,add_save_path('vertex_package'))
    logger.info('vertex_package down')


def deal_package_relation():
    '''
    处理每天所有的快递的两端信息
    id,时间,两段证件号，手机号
    :return:
    '''
    pass

def vertex_package_detail():
    '''
    快递节点详细信息，不入图，入后置存储数据库
    :return:
    '''
    pass

def vertex_package():
    '''
    提取快递详细信息的一部分概要信息，入图
    :return:
    '''
    pass

def edge_phone_send_package():
    '''
    从快递两端信息中，提取手机号寄件信息 入图
    :return:
    '''
    cols = ['phone', 'packageid', 'send_time','send_time start_time','send_time+3600 end_time']
    create_tmpview_table(spark,'vertex_package')

    sql = '''
        select senderphone phone, packageid, accepttime send_time from 
        vertex_package where verify_phonenumber(senderphone) = 1
    '''

    df = spark.sql(sql)

    res = df.drop_duplicates(['phone','packageid']).selectExpr(*cols)
    write_orc(res,add_save_path('edge_phone_send_package'))
    logger.info('edge_phone_send_package down')



def edge_phone_receive_package():
    '''
    从快递两端信息中，提取手机号收件信息 入图
    :return:
    '''
    cols = ['phone', 'packageid', 'receive_time','receive_time start_time','receive_time+3600 end_time']
    create_tmpview_table(spark,'vertex_package')

    sql = '''
            select receiverphone phone, packageid, inputtime receive_time from 
            vertex_package where verify_phonenumber(receiverphone) = 1
        '''

    df = spark.sql(sql)

    res = df.drop_duplicates(['phone', 'packageid']).selectExpr(*cols)
    write_orc(res, add_save_path('edge_phone_receive_package'))
    logger.info('edge_phone_receive_package down')

def edge_phone_sendpackage_phone_detail():

    if create_tmpview_table(spark, 'package_source_detail',cp=cp, root='incrdir'):
        sql1 = '''
                select senderphone start_phone, receiverphone end_phone,
                if(accepttime=0,inputtime,accepttime) start_time,if(accepttime=0,inputtime,accepttime) end_time
                from package_source_detail where verify_phonenumber(senderphone) = 1 and verify_phonenumber(receiverphone) = 1
                and senderphone != receiverphone 
            '''
        df = spark.sql(sql1).repartition(100)
        write_orc(df, add_incr_path('edge_phone_sendpackage_phone_detail',cp=cp))
    else:
        logger.info('package_source_detail 在分区%s无数据'%cp)

def edge_phone_sendpackage_phone():
    '''
    从快递两端信息中，提取手机号收发快递信息，入图
    两段手机号都存在的情况
    :return:

    '''

    create_tmpview_table(spark,'vertex_package')

    sql1 = '''
        select senderphone start_phone, receiverphone end_phone,
        if(accepttime=0,inputtime,accepttime) start_time,if(accepttime=0,inputtime,accepttime) end_time
        from vertex_package where verify_phonenumber(senderphone) = 1 and verify_phonenumber(receiverphone) = 1
        and senderphone != receiverphone 
    '''
    df = spark.sql(sql1)
    write_orc(df,add_save_path('edge_phone_sendpackage_phone_detail'))

    sql2 = '''
            select senderphone start_phone, receiverphone end_phone,
            min(coalesce(accepttime,inputtime)) start_time,max(coalesce(accepttime,inputtime)) end_time,
            count(1) num from vertex_package 
            where verify_phonenumber(senderphone) = 1 and verify_phonenumber(receiverphone) = 1
            and senderphone != receiverphone group by senderphone,receiverphone
        '''

    df = spark.sql(sql2)

    write_orc(df,add_save_path('edge_phone_sendpackage_phone'))
    logger.info('edge_phone_sendpackage_phone down')

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    if len(sys.argv) == 4:
        import_times = sys.argv[1].split(',')
        cp = str(sys.argv[2])
        tablename = sys.argv[3]
        deal_package_source_day(import_times,cp)
    elif len(sys.argv) == 2:
        cp = sys.argv[1]
        edge_phone_sendpackage_phone_detail()
    else:
        deal_vertex_package()
        edge_phone_send_package()
        edge_phone_receive_package()
        edge_phone_sendpackage_phone()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))