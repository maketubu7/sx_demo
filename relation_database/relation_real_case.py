# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 19:01
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : data_count.py
# @Software: PyCharm
# @content : 统计每个表的数量
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import os,sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')


conf=SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.yarn.am.cores', 4)
conf.set('spark.executor.memory', '15g')
conf.set('spark.executor.instances', 100)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
from jg_info import vertex_table_info,edge_table_info


path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/open_phone'
save_root = 'relation_theme_extenddir'
zp_source_root = 'wa_data/zp_jg'
steal_oil_source_root = 'wa_data/touyou_jg'

exists_files = vertex_table_info.keys() + edge_table_info.keys()
zp_file_cols = {
    'vertex_phonenumber':['phone', 'xm','province', 'city'],
    'vertex_person':['zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                                      'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz'],
    'vertex_case':['asjbh','ajmc','0 asjfskssj','0 asjfsjssj','"" asjly','ajlb','"" fxasjsj','fxasjdd_dzmc',
                                        'jyaq','"" ajbs','larq','city','link_app'],
    'vertex_qq':['qq'],
    'vertex_alipay':['alipay'],
    'vertex_qq_group':['groupid','groupname'],
    'vertex_bankcard':['yhkh banknum','khh','sfzh','khrq'],
    'edge_qqgroup_link_banknum':['banknum','groupid','relate_time','start_time','end_time','relate_num'],
    'edge_qq_link_group':['qq','groupid','start_time','end_time'],
    'edge_qq_link_qq':['qq1','qq2','start_time','end_time'],
    'edge_person_link_phone':['phone','sfzh','start_time','end_time'],
    'edge_case_link_wechat':['wechatid wechat','ajbh asjbh','start_time','end_time'],
    'edge_case_link_qq':['qq','ajbh asjbh','start_time','end_time'],
    'edge_case_link_phone':['phone','ajbh asjbh','start_time','end_time'],
    'edge_case_link_person':['sfzh','ajbh asjbh','start_time','end_time'],
    'edge_case_link_banknum':['banknum','ajbh asjbh','start_time','end_time'],
    'edge_case_link_alipay':['alipay','ajbh asjbh','start_time','end_time'],
}

steal_file_cols = {
    'vertex_case':['asjbh','ajmc','asjfskssj','asjfsjssj','asjly','ajlb','fxasjsj','fxasjdd_dzmc',
                                        'jyaq','ajbs','larq','"" city','"" link_app'],
    'edge_case_link_person':['sfzh','ajbh asjbh','start_time','end_time'],
    'edge_case_link_phone':['phone','ajbh asjbh','start_time','end_time'],

}


def deal_zp_data():
    for file,cols in zp_file_cols.items():
        logger.info('dealing %s'%file)
        logger.info(add_save_path(file+"_jg",cp='*',root=zp_source_root))
        df = read_orc(spark,add_save_path(file+"_jg",cp='*',root=zp_source_root)).selectExpr(*cols)
        file_name = file+"_zp" if file in exists_files else file
        write_orc(df,add_save_path(file_name,root=save_root))


def deal_steal_data():
    for file,cols in steal_file_cols.items():
        df = read_orc(spark,add_save_path(file+"_jg",cp='*',root=steal_oil_source_root)).selectExpr(*cols)
        file_name = file+"_steal" if file in exists_files else file
        write_orc(df,add_save_path(file_name,root=save_root))




if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))


    # deal_zp_data()
    deal_steal_data()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))