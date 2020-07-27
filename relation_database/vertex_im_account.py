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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'

def create_uniondf(spark,table_list,tmp_sql):
    '''
    :param spark:
    :param table_list: 所需原始表及其字段对应信息
    :param tmp_sql: 原始sql语句，根据table_list进行格式化
    :return: table_List所有df的合集
    '''
    dfs = []
    for info in table_list:
        sql = tmp_sql.format(**info)
        df = read_orc(spark,add_save_path(info['tablename'],root=save_root))
        if df:
            df.createOrReplaceTempView(info['tablename'])
            df = spark.sql(sql)
            dfs.append(df)
    union_df = reduce(lambda x,y:x.unionAll(y),dfs).dropDuplicates([table_list[0]['col']])
    write_orc(union_df,add_save_path('vertex_'+table_list[0]['type'],root=save_root))


def vertex_im_account():

    # nb_app_dws_per_res_his_accmobile	用户账号与手机号的关系历史全量汇总
    # vt_type	虚拟身份类型
    # user_acc	用户账号
    # mob	手机号码
    # rel_type	关系类型
    # todo: 暂无数据

    table_list = [
        {'tablename':'edge_phone_link_qq','account':'user_account','type':'qq','col':'qq'},
        {'tablename':'edge_case_link_qq','account':'qq','type':'qq','col':'qq'},
        {'tablename':'edge_person_link_qq','account':'user_account','type':'qq','col':'qq'},
        {'tablename':'edge_qq_link_qq','account':'qq1','type':'qq','col':'qq'},
        {'tablename':'edge_qq_link_qq','account':'qq2','type':'qq','col':'qq'},
        {'tablename':'edge_phone_link_wechat','account':'user_account','type':'wechat','col':'wechat'},
        {'tablename':'edge_case_link_wechat','account':'wechat','type':'wechat','col':'wechat'},
        {'tablename':'edge_person_link_wechat','account':'user_account','type':'wechat','col':'wechat'},
        {'tablename':'edge_wechat_link_wechat','account':'wechat1','type':'wechat','col':'wechat'},
        {'tablename':'edge_wechat_link_wechat','account':'wechat2','type':'wechat','col':'wechat'},
        {'tablename':'edge_phone_link_sinablog','account':'user_account','type':'sina','col':'sina'},
        {'tablename':'edge_person_link_sinablog','account':'user_account','type':'sina','col':'sina'},

    ]

    qq_list = filter(lambda row:row.get('type')=='qq',table_list)
    we_list = filter(lambda row:row.get('type')=='wechat',table_list)
    sina_list = filter(lambda row:row.get('type')=='sina',table_list)

    tmp_sql = '''
        select {account} {col} from {tablename} group by {account}
    '''

    create_uniondf(spark,qq_list,tmp_sql)
    create_uniondf(spark,we_list,tmp_sql)
    create_uniondf(spark,sina_list,tmp_sql)


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    vertex_im_account()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))