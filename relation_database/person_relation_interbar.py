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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'

def vertex_internet():
    '''
    网吧节点信息
    1、ODS_POL_SEC_NETBAR_INTPER_INFO	网吧上网人员信息
        PREM_NO	营业场所_编号
        PREM_DESIG	营业场所_名称
        ADDR_NAME	地址名称
        LEGAL_NAME	法人_姓名
        LEGAL_CTCT_TEL	法人_联系电话
        LEGAL_CRED_NUM	法人_证件号码
    :return:
    '''
    sql = '''
        select trim(prem_no) siteid, reg_type_name sitetype, prem_desig title,
        addr_name address, 'ods_pol_sec_netbar_intper_info' tablename
        from ods_pol_sec_netbar_intper_info 
        where format_data(prem_no) != '' and format_data(prem_desig) != ''
    '''
    init(spark,'ods_pol_sec_netbar_intper_info',if_write=False)
    df = spark.sql(sql).drop_duplicates(['siteid'])

    write_orc(df,add_save_path('vertex_internetbar',root=save_root))
    logger.info('vertex_internetbar down')


def edge_person_surfing_internetbar_detail():
    '''
    人上网信息
    1、ODS_POL_SEC_NETBAR_INTPER_INFO	网吧上网人员信息
        CRED_NUM	证件号码
        MAIENG_IP	主机_IP地址
        INTENET_START_TIME	上网_开始时间
        INTENET_END_TIME	上网_结束时间
        INTENET_TIME_LENGTH	上网_时长
        PREM_NO	营业场所_编号
        PREM_DESIG	营业场所_名称
    :return:
    '''
    sql = '''
        select format_zjhm(cred_num) sfzh, trim(prem_no) siteid,
        cast(format_timestamp(intenet_start_time) as bigint) start_time,
        cast(format_timestamp(intenet_end_time) as bigint) end_time
        from ods_pol_sec_netbar_intper_info
        where verify_sfz(cred_num) = 1 and format_data(prem_no) != '' and format_data(prem_desig) != ''
    '''

    init_history(spark, 'ods_pol_sec_netbar_intper_info',import_times=import_times, if_write=False)
    df = spark.sql(sql).repartition(10)

    write_orc(df, add_incr_path('edge_person_surfing_internetbar_detail',cp=cp))
    logger.info('edge_person_surfing_internetbar_detail down')


def edge_person_surfing_internetbar():
    '''
    人上网信息
    1、ODS_POL_SEC_NETBAR_INTPER_INFO	网吧上网人员信息
        CRED_NUM	证件号码
        MAIENG_IP	主机_IP地址
        INTENET_START_TIME	上网_开始时间
        INTENET_END_TIME	上网_结束时间
        INTENET_TIME_LENGTH	上网_时长
        PREM_NO	营业场所_编号
        PREM_DESIG	营业场所_名称
    :return:
    '''
    create_tmpview_table(spark,'edge_person_surfing_internetbar_detail',root='incrdir')
    detail = spark.sql(''' select * from edge_person_surfing_internetbar_detail''')
    write_orc(detail,add_save_path('edge_person_surfing_internetbar_detail',root=save_root))
    sql = '''
        select sfzh,siteid,min(start_time) start_time, max(end_time) end_time,
        count(1) as num
        from edge_person_surfing_internetbar_detail group by sfzh,siteid
    '''

    df = spark.sql(sql)
    write_orc(df,add_save_path('edge_person_surfing_internetbar',root=save_root))
    logger.info('edge_person_surfing_internetbar down')


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    if len(sys.argv) == 4:
        import_times = sys.argv[1].split(',')
        cp = str(sys.argv[2])
        tablename = sys.argv[3]
        edge_person_surfing_internetbar_detail()
    else:
        vertex_internet()
        edge_person_surfing_internetbar()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

