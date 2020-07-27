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
conf.set('spark.executor.memory', '1g')
conf.set('spark.executor.instances', 20)
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/qq'


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

def read_csv(file,schema):
    path= '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/zp_data/'+file+'.csv'
    return spark.read.csv(path, schema=schema,header=False)

def read_csv_heaer(file,sep=','):
    path= '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/zp_data/'+file+'.csv'
    return spark.read.csv(path,header=True,sep=sep)

source_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/zp_jg/'
old_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/zp_first/'
new_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/zp_new/'


old_files = ['edge_case_link_alipay', 'edge_case_link_banknum', 'edge_case_link_qq',
             'edge_case_link_wechat', 'edge_qq_link_group', 'edge_qqgroup_link_banknum',
             'vertex_alipay', 'vertex_zp_bankcard', 'vertex_case', 'vertex_qq_group',
             'vertex_qq', 'vertex_wechat']

zp_schemas = {
    'edge_case_link_alipay':['ajbh','alipay','start_time','end_time'],
    'edge_case_link_banknum':['ajbh','bank_num banknum','start_time','end_time'],
    'edge_case_link_qq':['ajbh','qq','start_time','end_time'],
    'edge_case_link_wechat':['ajbh','wechatid','start_time','end_time'],
    'edge_qq_link_group':['groupid', 'qq','0 start_time', '0 end_time','ip_area'],
    'edge_qqgroup_link_banknum':['groupid','banknum','relate_time','start_time','end_time','relate_num'],
    'vertex_alipay':['alipay'],
    'vertex_bankcard':['yhkh','khh','xm','sfzh','wyyzdh','khrq'],
    'vertex_case':['asjbh','ajmc','link_app','city','"" jyaq','0 larq','"" ajlb','"" fxasjdd_dzmc',],
    'vertex_qq_group':['groupid','groupname'],
    'vertex_qq':['qq'],
    'vertex_wechat':['wechatid wechat'],
}


def deal_zp_old_data():
    for file in old_files:
        save_name = file.replace('_jg','')
        if file.startswith('vertex'):
            save_name = save_name.replace('_zp','')
            df = spark.read.orc(source_path+file).drop('jid')
            write_orc(df,old_path+save_name)
        df = spark.read.orc(source_path+file).drop('start_jid').drop('end_jid')
        write_orc(df, old_path + save_name)

def add_new_data():
    # qq_friend = read_csv_heaer('qq_friend')
    # qq_friend = qq_friend.withColumn('start_time',lit(0))
    # qq_friend = qq_friend.withColumn('end_time',lit(0))
    # write_orc(qq_friend,new_path+'edge_qq_friend_qq')

    qq_in_group = read_csv_heaer('qq_groupmember')
    qq_in_group = qq_in_group.withColumn('ip_area',lit(''))
    qq_in_group = qq_in_group.withColumn('start_time',lit(0))
    qq_in_group = qq_in_group.withColumn('end_time',lit(0))
    qq_in_group = qq_in_group.selectExpr(*zp_schemas['edge_qq_link_group'])

    qq_in_group2 = read_orc(old_path+'edge_qq_link_group')
    qq_in_group2 = qq_in_group2.selectExpr(*zp_schemas['edge_qq_link_group'])
    res = qq_in_group.union(qq_in_group2).dropDuplicates(['qq','groupid'])
    write_orc(res,new_path+'edge_qq_link_group')

    link_alipay_1 = read_orc(old_path+'edge_case_link_alipay').selectExpr(*zp_schemas['edge_case_link_alipay'])
    link_alipay_2 = read_csv_heaer('case_link_source').where('type="alipay"').selectExpr('ajbh','value alipay','0 start_time','0 end_time')
    link_alipay = link_alipay_1.union(link_alipay_2).dropDuplicates(['ajbh','alipay'])
    write_orc(link_alipay,new_path+'edge_case_link_alipay')

    link_qq_1 = read_orc(old_path + 'edge_case_link_qq').selectExpr(*zp_schemas['edge_case_link_qq'])
    link_qq_2 = read_csv_heaer('case_link_source').where('type="qq"').selectExpr('ajbh', 'value qq',
                                                                                         '0 start_time', '0 end_time')
    link_qq = link_qq_1.union(link_qq_2).dropDuplicates(['ajbh', 'qq'])
    write_orc(link_qq, new_path + 'edge_case_link_qq')

    link_wechat_1 = read_orc(old_path + 'edge_case_link_wechat').selectExpr(*zp_schemas['edge_case_link_wechat'])
    link_wechat_2 = read_csv_heaer('case_link_source').where('type="wechat"').selectExpr('ajbh', 'value wechatid',
                                                                                         '0 start_time', '0 end_time')
    link_wechat = link_wechat_1.union(link_wechat_2).dropDuplicates(['ajbh', 'wechatid'])
    write_orc(link_wechat, new_path + 'edge_case_link_wechat')

    link_banknum_1 = read_orc(old_path + 'edge_case_link_banknum').selectExpr(*zp_schemas['edge_case_link_banknum'])
    link_banknum_2 = read_csv_heaer('case_link_source').where('type="bankcard"').selectExpr('ajbh', 'value banknum',
                                                                                         '0 start_time', '0 end_time')
    link_backnum = link_banknum_1.union(link_banknum_2).dropDuplicates(['ajbh', 'banknum'])
    write_orc(link_backnum, new_path + 'edge_case_link_banknum')

    ## 案件受害人
    edge_person_link_case = read_csv_heaer('case_link_source').where('type="person"') \
            .selectExpr('ajbh', 'value sfzh','0 start_time', '0 end_time').dropDuplicates()
    write_orc(edge_person_link_case,new_path + 'edge_case_link_person')

    ## 案件关联电话
    edge_phone_link_case = read_csv_heaer('case_link_source').where('type="phone"') \
        .selectExpr('ajbh', 'value phone', '0 start_time', '0 end_time').dropDuplicates()
    write_orc(edge_phone_link_case, new_path + 'edge_case_link_phone')

    ## 人关联电话
    edge_person_link_phone = read_csv_heaer('edge_person_link_phone').selectExpr('sfzh','phone','1 linknum','0 start_time','0 end_time')
    write_orc(edge_person_link_phone, new_path + 'edge_person_link_phone')


def all_vertex():
    # s_qq = read_orc(old_path+'vertex_qq').selectExpr(*zp_schemas['vertex_qq'])
    # qq1 = read_orc(new_path+'edge_qq_friend_qq').selectExpr('qq1 qq')
    # qq2 = read_orc(new_path+'edge_qq_friend_qq').selectExpr('qq2 qq')
    # qq3 = read_orc(new_path+'edge_qq_link_group').selectExpr('qq')
    # qq4 = read_orc(new_path+'edge_case_link_qq').selectExpr('qq')
    # qq = s_qq.union(qq1).union(qq2).union(qq3).union(qq4).dropDuplicates()
    # write_orc(qq,new_path+'vertex_qq')
    #
    # s_qq_group = read_orc(old_path+'vertex_qq_group').selectExpr(*zp_schemas['vertex_qq_group'])
    # qq_group1 = read_orc(new_path+'edge_qq_link_group').selectExpr('groupid','"" groupname')
    # qq_group = s_qq_group.union(qq_group1).dropDuplicates()
    # write_orc(qq_group,new_path+'vertex_qq_group')
    #
    # s_wechat = read_orc(old_path + 'vertex_wechat').selectExpr(*zp_schemas['vertex_wechat'])
    # wechat1 = read_orc(new_path + 'edge_case_link_wechat').selectExpr('wechatid wechat')
    # qq_group = s_wechat.union(wechat1).dropDuplicates()
    # write_orc(qq_group, new_path + 'vertex_wechat')
    #
    # s_alipay = read_orc(old_path + 'vertex_alipay').selectExpr(*zp_schemas['vertex_alipay'])
    # alipay1 = read_orc(new_path + 'edge_case_link_alipay').selectExpr('alipay')
    # qq_group = s_alipay.union(alipay1).dropDuplicates()
    # write_orc(qq_group, new_path + 'vertex_alipay')

    # s_bankcard = read_orc(old_path + 'vertex_bankcard').selectExpr(*zp_schemas['vertex_bankcard'])
    # bankcard1 = read_orc(new_path + 'edge_case_link_banknum').selectExpr('banknum yhkh','"" khh','"" xm','"" sfzh','"" wyyzdh','"" khrq')
    # qq_group = s_bankcard.union(bankcard1).dropDuplicates()
    # write_orc(qq_group, new_path + 'vertex_bankcard')

    ## 提取人节点
    # p1 = read_orc(add_save_path('vertex_person'))
    #     # df1 = read_orc(new_path+'edge_case_link_person')
    #     # df2 = read_orc(new_path+'edge_person_link_phone')
    #     # p2 = df1.select('sfzh').union(df2.select('sfzh'))
    #     #
    #     # p = p2.join(p1, p2.sfzh == p1.zjhm, 'left') \
    #     #     .selectExpr('sfzh zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
    #     #                 'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz').na.fill('')
    #     # res = p.drop_duplicates(['zjhm'])
    #     # write_orc(res, new_path+'vertex_person')

    ## 提取案件节点
    cols = ['asjbh','ajmc','link_app','city','jyaq','valid_datetime(larq) larq','ajlb','fxasjdd_dzmc']
    vertex_case1= read_csv_heaer('zp_case_new',sep='|').selectExpr(*cols)
    vertex_case2 = read_orc(old_path+'vertex_case').selectExpr(*zp_schemas['vertex_case'])
    vertex_case = vertex_case1.union(vertex_case2).dropDuplicates(['asjbh'])
    write_orc(vertex_case, new_path + 'vertex_case')

def deal_all_relation():
    # edge_table_info = {}
    # edge_table_info['edge_case_link_person'] = ['sfzh start_vertex', 'ajbh end_vertex']
    # edge_table_info['edge_case_link_phone'] = ['phone start_vertex', 'ajbh end_vertex']
    # edge_table_info['edge_case_link_alipay'] = ['alipay start_vertex', 'ajbh end_vertex']
    # edge_table_info['edge_case_link_banknum'] = ['banknum start_vertex', 'ajbh end_vertex']
    # edge_table_info['edge_case_link_wechat'] = ['wechatid start_vertex', 'ajbh end_vertex']
    # edge_table_info['edge_case_link_qq'] = ['qq start_vertex', 'ajbh end_vertex']
    # edge_table_info['edge_qq_link_group'] = ['qq start_vertex', 'groupid end_vertex']
    # edge_table_info['edge_qqgroup_link_banknum'] = ['banknum start_vertex', 'groupid end_vertex']
    # edge_table_info['edge_person_link_phone'] = ['phone start_vertex', 'sfzh end_vertex']
    # edge_table_info['edge_qq_friend_qq'] = ['qq1 start_vertex', 'qq2 end_vertex']
    # dfs = []
    # for k,v in edge_table_info.items():
    #     df = read_orc(source_path+k+'_jg').selectExpr(*v)
    #     dfs.append(df)
    # res = reduce(lambda a,b:a.unionAll(b),dfs)
    # res = res.dropDuplicates()
    # res.write.mode('overwrite').csv(source_path+'all_relation',header=None)
    #
    # df = read_orc(source_path+"vertex_case"+'_jg').selectExpr('asjbh').dropDuplicates()
    # df.write.mode('overwrite').csv(source_path + 'all_caseid', header=None)

    # df = read_orc(source_path + "vertex_bankcard" + '_jg').selectExpr('yhkh').dropDuplicates()
    # df.write.mode('overwrite').csv(source_path + 'all_bankcard', header=None)

    df = read_orc(source_path + "vertex_qq" + '_jg').selectExpr('qq').dropDuplicates()
    df.write.mode('overwrite').csv(source_path + 'all_qq', header=None)

    df = read_orc(source_path + "vertex_qq_group" + '_jg').selectExpr('groupid').dropDuplicates()
    df.write.mode('overwrite').csv(source_path + 'all_qqgroup', header=None)

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    #add_new_data()
    # all_vertex()
    deal_all_relation()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))