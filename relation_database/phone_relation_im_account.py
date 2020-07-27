# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : person_relation_car.py
# @Software: PyCharm
# @content : 人车关联信息

import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession
# from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark.sql.types import *
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

def edge_phone_link_account():

    # nb_app_dws_per_res_his_accmobile	用户账号与手机号的关系历史全量汇总
    # vt_type	虚拟身份类型
    # user_acc	用户账号
    # mob	手机号码
    # rel_type	关系类型

    init(spark,'nb_app_dws_per_res_his_accmobile',if_write=False)
    # and rel_type in ('VM02','VM03') 伴随加入，否则没有数据
    sql = '''
        select format_phone(mob) phone, trim(user_acc) user_account,first_time start_time, last_time end_time,
        vt_type,'nb_app_dws_per_res_his_accmobile' tablename from nb_app_dws_per_res_his_accmobile
        where verify_phonenumber(mob) = 1 and trim(user_acc) != "" 
    '''

    cols = ['phone','user_account','vt_type','if(start_time>0,start_time,0) start_time','if(end_time>0,end_time,0) end_time']
    resource = spark.sql(sql).selectExpr(*cols).dropDuplicates(['phone','vt_type','user_account'])

    write_orc(resource.where('vt_type="1030036"').drop('vt_type'),add_save_path('edge_phone_link_wechat',root=save_root))
    write_orc(resource.where('vt_type="1330001"').drop('vt_type'),add_save_path('edge_phone_link_sinablog',root=save_root))
    write_orc(resource.where('vt_type="1030001"').drop('vt_type'),add_save_path('edge_phone_link_qq',root=save_root))

    logger.info('phone_link_im_account down')

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    edge_phone_link_account()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))