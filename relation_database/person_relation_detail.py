# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 14:54
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : person_relation_detail.py
# @Software: PyCharm
# @content : 人人间接关联关系，每月跑一次即可

import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession
# from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark.sql.types import *
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
##堆外内存
conf.set('spark.yarn.executor.memoryOverhead', '15g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '15g')
conf.set('soark.shuffle.partitions',800)
conf.set('spark.executor.instances', 100)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')


spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commonUtil = CommonUdf(spark)

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/with_travel'
root_home = 'person_relation_detail'

def join_smz(df,smz,start_key='start_phone',end_key='end_phone'):
    # df.createOrReplaceTempView('df')
    # smz.createOrReplaceTempView('smz')
    column = [col[0] for col in df.dtypes if col[0] not in [start_key,end_key]]
    fir_c = ['zjhm start_person',end_key] + column
    sec_c = ['start_person','zjhm end_person'] + column

    df1 = df.join(smz,df[start_key]==smz['phone'],'inner').selectExpr(*fir_c)
    df2 = df1.join(smz,df1[end_key]==smz['phone'],'inner').selectExpr(*sec_c)

    res = df2.where('start_person != end_person')
    return res

def reverse_df(df, start_key='start_person',end_key='end_person',extra_cols=None):
    '''将通话也强行做成双向边，双方的通话次数强行变成了一致 '''
    cols = ['%s %s'%(end_key,start_key),'%s %s'%(start_key,end_key)]
    if extra_cols:
        for col in extra_cols:
            cols.append(col)
    return df.selectExpr(*cols)

table_list = [
        {
            'from_table': 'edge_groupcall_detail',
            'res': 'relation_telcall_month',
            'detail':'edge_person_groupcall_detail',
            'columns':['start_phone','end_phone','timestamp2month(start_time) cp'],
            'par_num': 100
        },
        {
            'from_table': 'edge_groupmsg_detail',
            'res': 'relation_telmsg_month',
            'detail': 'edge_person_groupmsg_detail',
            'columns': ['start_phone', 'end_phone', 'timestamp2month(start_time) cp'],
            'par_num': 10
        },
        {
            'from_table': 'edge_phone_sendpackage_phone_detail',
            'res': 'relation_post_month',
            'detail': 'edge_person_grouppost_detail',
            'columns': ['start_phone', 'end_phone', 'timestamp2month(start_time) cp'],
            'par_num': 10
        },
    ]

def edge_person_relation_person_detail(cp):
    '''
    人-实名制-电话号码-通话-电话号码-实名制-人
    '''
    for info in table_list:
        source = info['from_table']
        cols = info['columns']
        detail_path = info['detail']
        df = read_orc(spark,add_incr_path(source,cp=cp))
        par_num = info['par_num']
        if df:
            df = df.selectExpr(*cols)
            detail_res = join_smz(df,smz).coalesce(par_num)
            # detail_res.persist()
            ## 明细保存
            write_orc(detail_res,add_save_path(detail_path,root=root_home,cp=cp))

def person_month_detail():
    ##保存每月关系计量次数 last_cp 最新分区月份
    columns = ['start_person','end_person','num','cp']
    for info in table_list:
        target = info['res']
        detail_path = info['detail']
        ### 每次的最大一次分区
        last_cp = read_orc(spark,add_save_path(target, cp='*', root=root_home)).selectExpr('max(cp)').collect()[0][0]
        detail_res = read_orc(spark,add_save_path(detail_path,cp='*',root=root_home)).where('cp>=2019100000')
        # detail_res.show()
        detail_res.persist()
        cps = [row.cp for row in detail_res.selectExpr('cast(cp as string) cp').distinct().collect() if row.cp >= str(last_cp) and row.cp <= time.strftime("%Y%m0000", time.localtime())]
        logger.info('cps')
        logger.info('||'.join(cps))
        for cp in cps:
            df = detail_res.where('cp=%s'%cp)
            reverse_df(df,extra_cols=['cp']).show()
            union_df = df.unionAll(reverse_df(df,extra_cols=['cp']))
            res = union_df.groupby(['start_person','end_person']) \
                .agg(count('cp').alias('num'))
            res = res.withColumn('cp',lit(cp)).selectExpr(*columns)
            write_orc(res,add_save_path(target,cp=cp,root=root_home))
            logger.info(add_save_path(target,cp=cp,root=root_home) + ' success')
        detail_res.unpersist()

def deal_history_data(start_cp,end_cp):
    start_date = datetime.strptime(start_cp,'%Y%m%d00')
    end_date = datetime.strptime(end_cp,'%Y%m%d00')
    num = (end_date - start_date).days
    cps = [(start_date + timedelta(days=_)).strftime('%Y%m%d00') for _ in range(num)][::-1]
    for cp in cps:
        edge_person_relation_person_detail(cp)


if __name__ == '__main__':
    ''' 获取人人之间的通联关系, 
     20200612 变更，任何人人关系都需要做成双向边
     '''
    logger.info('================================start time:%s' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    smz = read_orc(add_save_path('edge_person_smz_phone_top')).selectExpr('start_person zjhm','end_phone phone')
    smz.persist()
    if len(sys.argv) == 2:
        cp = sys.argv[1]
        edge_person_relation_person_detail(cp)
    elif len(sys.argv) == 3:
        start_cp = sys.argv[1]
        end_cp = sys.argv[2]
        deal_history_data(start_cp,end_cp)
    else:
        person_month_detail()
    smz.unpersist()



logger.info('================================end time:%s' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
