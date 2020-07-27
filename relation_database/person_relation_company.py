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


def person_relate_company():
    # cert_num 证件号码
    # company_id 公司编码
    # company_name 公司名称
    # emp_date_fs 入职时间
    # dep_date_fs 离职时间
    # depart 所在部门
    # prof 职业
    # position 职位
    # salary 薪酬
    # rel_type 关系类型
    # last_dis_place 发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    init(spark,'nb_app_dws_per_org_his_dccpy',if_write=False,is_relate=True)
    sql = '''
        select cert_num start_person, md5(format_data(company_name)) end_company,format_data(company_name) company_name,multidate2timestamp(emp_date_fs) start_time, 
        multidate2timestamp(dep_date_fs) end_time, depart, prof, position,salary, rel_type type 
        from nb_app_dws_per_org_his_dccpy where verify_sfz(cert_num) = 1 and company_id != '' and 
        company_id is not null and format_data(company_name) != '' and verify_company_name(format_data(company_name)) = 1
    '''

    resource = spark.sql(sql).dropDuplicates(['start_person','end_company','type']).na.fill('')
    #DCOM001 : 法人
    #DCOM002 : 任职
    #DCOM003 : 股东

    write_orc(resource.where('type="DCOM001"').drop('type'),add_save_path('edge_person_legal_com',root=save_root))
    write_orc(resource.where('type="DCOM002"').drop('type'),add_save_path('edge_person_work_com',root=save_root))
    write_orc(resource.where('type="DCOM003"').drop('type'),add_save_path('edge_person_shareholder_com',root=save_root))

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    person_relate_company()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))