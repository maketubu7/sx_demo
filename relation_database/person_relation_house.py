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
conf.set('spark.executor.memoryOverhead', '10g')
conf.set('spark.yarn.am.cores', 5)
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'

def person_relate_house():
    # cert_type 证件类型
    # cert_num 证件号码
    # mob 手机号
    # house_no 房屋编号
    # house_addr 房屋地址
    # rent_begin_time 租赁起始时间
    # rent_end_time 租赁结束时间
    # house_tran_time 交易时间
    # house_tran_price 交易金额
    # rel_type 关系类型
    # last_dis_place 最后发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    # totle_count 累计出现次数
    # totle_days 累计出现天数
    init(spark, 'nb_app_dws_per_res_his_dchouse', if_write=False, is_relate=True)
    sql = '''
        select cert_num sfzh, mob phone, house_no house_id, house_addr, cast(rent_begin_time as bigint) start_time,
        cast(rent_end_time as bigint) end_time,house_tran_time trade_time, house_tran_price trade_price, rel_type type
        from nb_app_dws_per_res_his_dchouse where house_no is not null and house_no != '' and verify_sfz(cert_num)=1
    '''

    resource = spark.sql(sql)
    resource.persist()
    col1 = ['sfzh','phone','house_id','start_time','end_time','trade_time','trade_price']
    col2 = ['sfzh','phone','house_id','0 start_time','0 end_time','trade_time','trade_price']
    col3 = ['sfzh','phone','house_id','0 start_time','0 end_time']
    drop_col = ['sfzh','house_id']

    write_orc(resource.where('type="DHU02"').dropDuplicates(drop_col).selectExpr(*col1),add_save_path('edge_person_rent_house',root=save_root))
    write_orc(resource.where('type in ("DHU04","DHU01")').dropDuplicates(drop_col).selectExpr(*col2),add_save_path('edge_person_own_house',root=save_root))
    write_orc(resource.where('type="DHU05"').dropDuplicates(drop_col).selectExpr(*col3),add_save_path('edge_person_live_house',root=save_root))


    v_df = resource.selectExpr('house_id','house_addr').dropDuplicates(['house_id'])
    write_orc(v_df,add_save_path('vertex_house',root=save_root))

    resource.unpersist()

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    person_relate_house()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))