# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 19:01
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : person_open_phone.py
# @Software: PyCharm
# @content : 身份证号码开户信息，暂时为实名制
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os,sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')


conf=SparkConf().set('spark.driver.maxResultSize', '30g')
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/open_phone'

##todo:all
def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix,'parquet/',path))
    return df
def write_parquet(df,path,method='overwrite'):
    '''写 parquet'''
    df.write.mode(method).parquet(path=os.path.join(path_prefix,'parquet/',path))


def write_orc(df, path,mode='overwrite'):
    df.write.format('orc').mode(mode).save(path)
    logger.info('write success')

def read_orc(path):
    df = spark.read.orc(path)
    return df

def write_jdbc(df,tablename,prop,mode='overwrite'):
    df.write.jdbc(url=prop['url'],mode=mode,table=tablename,properties=prop)
# 'nb_app_dws_per_res_his_uidremark'
filenames = ['nb_app_dws_per_res_his_dcmobile', 'nb_app_dws_per_per_his_fri_qq',
             'nb_app_dws_per_per_his_dcguardian',
             'nb_app_dws_res_res_his_mobcontact', 'nb_app_dws_per_per_his_tr_email',
             'nb_app_dws_per_cas_his_dccase', 'nb_app_dws_per_add_his_dcaddr',
             'nb_app_dws_per_add_his_dcschool', 'nb_app_dws_res_res_his_mobilemobile',
             'nb_app_dws_res_res_his_imsiimsi', 'nb_app_dws_res_res_his_message',
             'nb_app_dws_res_res_his_mobileimei', 'nb_app_dws_per_per_his_fri_wx',
             'nb_app_dws_add_cas_his_addrcase', 'nb_app_dws_res_res_his_mobileimsi',
             'nb_app_dws_res_res_his_mobilecall', 'nb_app_dws_per_org_his_dccpy',
             'nb_app_dws_per_res_his_dchouse', 'nb_app_dws_per_res_his_dcothercard',
             'nb_app_dws_per_per_his_dcacc', 'nb_app_dws_res_res_his_mailing',
             'nb_app_dws_cas_res_his_vehcas', 'nb_app_dws_per_per_his_tr_uidacc',
             'nb_app_dws_per_add_his_dcnetbar', 'nb_app_dws_per_res_his_dcorder',
             'nb_app_dws_per_res_his_dchousehold', 'nb_app_dws_per_per_his_dcfr', ]


def data_count():
    dfs = []
    for file in filenames:
        rdd = spark.sparkContext.textFile(add_path(file.upper(),is_relate=True))
        dfs.append((file,rdd.count()))
    res = spark.createDataFrame(dfs,['filename','count'])
    write_jdbc(res,'relation_data_count',prop)


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    prop = {
        "url": 'jdbc:mysql://24.2.26.44:3307/graphspace_sx?characterEncoding=UTF-8',
        "user": "bbd",
        "password": "bbdtnb",
        "driver": "com.mysql.jdbc.Driver",
        "ip": "24.2.26.44",
        "port": "3307",
        "db_name": "graphspace_sx",
        "mode": 'overwrite'}


    data_count()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))