# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 14:54
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : with_travel.py
# @Software: PyCharm
# @content : 出行推理信息

import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession
# from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark import StorageLevel
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
conf=SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.driver.memory', '30g')
conf.set('spark.executor.memoryOverhead', '10g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '20g')
conf.set('soark.shuffle.partitions',5000)
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
root_home = 'person_relation_detail'   ## 数据原始目录
save_root = 'relation_theme_extenddir'  ## 数据保存目录

def _min(sfz1, sfz2):
        return sfz1 if sfz1  < sfz2 else sfz2

def _max(sfz1, sfz2):
        return sfz1 if sfz1  > sfz2 else sfz2

spark.udf.register('get_min', _min, StringType())
spark.udf.register('get_max', _max, StringType())


def find_ids_dup(person_df,df,key_id):
    df.createOrReplaceTempView('tmp')
    tmp_person = spark.sql('select sfzh, collect_list(%s) ids from tmp group by sfzh'%key_id)

    person_df1 = tmp_person.join(person_df, person_df.sfzh1 == tmp_person.sfzh, 'inner') \
        .selectExpr('sfzh sfzh1', 'sfzh2', 'ids ids_a')
    person_df2 = tmp_person.join(person_df1, person_df1.sfzh2 == tmp_person.sfzh, 'inner') \
        .selectExpr('sfzh1', 'sfzh sfzh2', 'ids_a', 'ids ids_b')

    ## 去除没有过相同id的人
    res_person = person_df2.where('find_ids_dup(ids_a,ids_b)>0').select('sfzh1', 'sfzh2').dropDuplicates()
    return res_person

table_list = [
    # 配偶
    {'table': 'edge_person_spouse_person', 'sfzh1': 'sfzhnan', 'sfzh2': 'sfzhnv'},
    # 父亲
    {'table': 'edge_person_fatheris_person', 'sfzh1': 'sfzh', 'sfzh2': 'fqsfzh'},
    # 母亲
    {'table': 'edge_person_motheris_person', 'sfzh1': 'sfzh', 'sfzh2': 'mqsfzh'},
    # 监护人
    {'table': 'edge_person_guardianis_person', 'sfzh1': 'sfzh', 'sfzh2': 'jhrsfzh'},
    # 同父
    {'table': 'edge_find_common_father', 'sfzh1': 'sfzh1', 'sfzh2': 'sfzh2'},
    # 同母
    {'table': 'edge_find_common_mother', 'sfzh1': 'sfzh1', 'sfzh2': 'sfzh2'},
    # 同祖父母
    {'table': 'edge_find_common_grand', 'sfzh1': 'sfzh1', 'sfzh2': 'sfzh2'},
    # 同曾祖父母
    {'table': 'edge_find_common_greatgrand', 'sfzh1': 'sfzh1', 'sfzh2': 'sfzh2'},
    #邮件通联
    # {'table': 'edge_person_sendemail_person_detail', 'sfzh1': 'start_person', 'sfzh2': 'end_person'},
    #微信通联
    # {'table': 'edge_person_wechat_person_detail', 'sfzh1': 'start_person', 'sfzh2': 'end_person'},
    #qq通联
    # {'table': 'edge_person_qq_person_detail', 'sfzh1': 'start_person', 'sfzh2': 'end_person'},
    #通话
    {'table': 'relation_telcall_month', 'sfzh1': 'start_person', 'sfzh2': 'end_person','root':root_home},
    #短信
    {'table': 'relation_telmsg_month', 'sfzh1': 'start_person', 'sfzh2': 'end_person','root':root_home},
    #寄递
    {'table': 'relation_post_month', 'sfzh1': 'start_person', 'sfzh2': 'end_person','root':root_home},
]

#获取存在社会关系，家庭关系， 通联关系的人 为后面的同行关系的前置
def deal_relation_person():
    tmp_sql = ''' 
        select get_min({sfzh1},{sfzh2}) sfzh1, get_max({sfzh1},{sfzh2}) sfzh2 
        from {table} 
        '''
    dfs = []
    for info in table_list:
        sql = tmp_sql.format(**info)
        if info.get('root'):
            read_orc(spark,add_save_path(info['table'],cp='*',root=root_home)).createOrReplaceTempView(info['table'])
        else:
            read_orc(spark,add_save_path(info['table'])).createOrReplaceTempView(info['table'])
        write_parquet(spark.sql(sql).dropDuplicates(),path_prefix,info['table'])
    for info in table_list:
        dfs.append(read_parquet(spark,path_prefix,info['table']))
    union_df = reduce(lambda x,y:x.unionAll(y),dfs)
    person_df = union_df.dropDuplicates()
    write_parquet(person_df,path_prefix,'person_person')
    logger.info('person_person down')

##航班明细
def get_with_airline_detail():
    tmp_sql = ''' 
        select sfzh, airlineid, hbh, hbrq, sfd, mdd, tablename, 
        {table_sort} as table_sort from {tablename}
        '''

    # 机场安检的权重最高 订票最低
    table_list = [{'tablename':'edge_person_arrived_airline','table_sort':'2'},
                  {'tablename':'edge_person_reserve_airline','table_sort':'3'},
                  {'tablename':'edge_person_checkin_airline','table_sort':'1'}]

    union_df = create_extend_uniondf(spark,table_list,tmp_sql)
    # 事后删除数据
    union_df.createOrReplaceTempView('tmp')
    sql = '''
        select sfzh, airlineid, hbh, hbrq, sfd, mdd, tablename, 
        row_number() over (partition by sfzh,airlineid order by table_sort asc) as num from tmp
    '''
    df = spark.sql(sql).where('num=1').drop('num').repartition(200)

    write_parquet(df,path_prefix,'airline_detail')
    logger.info('airline_detail parquet down')

    res_person = find_ids_dup(person_df,df,'airlineid')
    write_parquet(res_person,path_prefix,'airline_person')

# 火车明细
def get_with_trainline_detail():
    ''' 身份证，车次id, 车次，发车日期，座位号 始发站 终点站 '''
    cols = ['sfzh', 'trianid', 'cc', 'fcrq', 'fz', 'dz']
    df = read_orc(spark,add_save_path('edge_person_reserve_trainline',root=save_root)).selectExpr(*cols)
    df = df.dropDuplicates(['sfzh', 'trianid']).coalesce(200)
    write_parquet(df,path_prefix,'train_detail')
    logger.info('train_detail parquet down')
    ### 找出坐过相同火车的人
    res_person = find_ids_dup(person_df,df,'trianid')
    write_parquet(res_person, path_prefix,'train_person')
    df.unpersist()
    logger.info('train_person down')

def edge_person_with_airline_travel():
    ## 同航班推理
    df1 = read_parquet(spark,path_prefix,'airline_detail')
    df1.persist(StorageLevel.MEMORY_AND_DISK)
    person_df = read_parquet(spark,path_prefix,'airline_person')
    df2 = person_df.join(df1,df1.sfzh == person_df.sfzh1,'inner') \
        .select(df1.sfzh.alias('sfzh1'), df1.sfd.alias('sfd1'), df1.mdd.alias('mdd1'),
                df1.airlineid.alias('start_id'), person_df.sfzh2)
    df3 = df2.join(df1,df2.sfzh2 == df1.sfzh, 'inner') \
        .select(df2.sfzh1, df1.sfzh.alias('sfzh2'), df2.start_id,df2.sfd1, df2.mdd1,
                df1.sfd.alias('sfd2'), df1.mdd.alias('mdd2'), df1.airlineid.alias('end_id'))

    df4 = df3.where("start_id==end_id and sfd1==sfd2 and mdd1==mdd2") \
        .selectExpr('sfzh1', 'sfzh2', 'sfd1 sfd','mdd1 mdd', 'start_id as airlineid')

    ## 同航班明细
    df1 = df1.selectExpr('airlineid','hbh','hbrq','tablename').dropDuplicates()
    detail = df4.join(df1,'airlineid','inner') \
        .select(df4.sfzh1, df4.sfzh2, df4.airlineid, df1.hbh , df1.hbrq, df4.sfd, df4.mdd, df1.tablename)
    detail_res = detail.dropDuplicates(['sfzh1','sfzh2','airlineid','sfd','mdd'])
    write_orc(detail_res, add_save_path('edge_person_airline_detail'))
    logger.info('edge_person_airline_detail down')
    df1.unpersist()

    #同航班结果
    detail_res.createOrReplaceTempView('tmp')
    sql = '''
          select sfzh1, sfzh2, min(hbrq) as start_time, max(hbrq) as end_time, count(*) as num 
          from tmp group by sfzh1, sfzh2
    '''
    write_orc(spark.sql(sql).coalesce(200), add_save_path('edge_person_with_airline_travel',root=save_root))
    logger.info('edge_person_with_airline_travel down')

def edge_person_with_trainline_travel():
    ## 同火车推理
    df1 = read_parquet(spark,path_prefix,'train_detail')
    df1.persist(StorageLevel.MEMORY_AND_DISK)

    person_df = read_parquet(spark,path_prefix,'train_person')

    df2 = person_df.join(df1,df1.sfzh == person_df.sfzh1,'inner').select(df1.sfzh.alias('sfzh1'),
                df1.fz.alias('fz1'), df1.dz.alias('dz1'), df1.trianid.alias('start_id'), person_df.sfzh2)
    logger.info('count_tag %s' % df2.count()) #4490031558
    df3 = df2.join(df1,df2.sfzh2 == df1.sfzh, 'inner') \
        .select(df2.sfzh1, df1.sfzh.alias('sfzh2'), df2.fz1, df2.dz1,df1.fz.alias('fz2'), df1.dz.alias('dz2'),
                df2.start_id, df1.trianid.alias('end_id'))
    logger.info('count_tag %s' % df3.count())
    df4 = df3.where("start_id == end_id and fz1== fz2 and dz1==dz2") \
        .selectExpr('sfzh1', 'sfzh2', 'fz1 fz','dz1 dz', 'start_id as trianid')

    ## 同火车明细
    df1 = df1.selectExpr('trianid', 'cc', 'fcrq').dropDuplicates()
    detail = df4.join(df1, 'trianid', 'inner') \
        .select(df4.sfzh1, df4.sfzh2, df4.trianid, df1.cc,df1.fcrq, df4.fz, df4.dz)
    df1.unpersist()
    detail_res = detail.dropDuplicates(['sfzh1', 'sfzh2', 'trianid', 'fz', 'dz']).repartition(2000)
    # logger.info('count_tag %s'%detail_res.count())
    write_orc(detail_res, add_save_path('edge_person_train_detail'))

    ## 同火车结果
    detail_res.createOrReplaceTempView('tmp')
    sql = '''
        select sfzh1, sfzh2, min(fcrq) as start_time, max(fcrq) as end_time, count(*) as num 
        from tmp group by sfzh1, sfzh2
    '''
    write_orc(spark.sql(sql).coalesce(200), add_save_path('edge_person_with_trainline_travel'))
    logger.info('edge_person_with_trainline_travel down')


with_travel_table = [
    {'table':'edge_person_train_detail','rq':'fcrq','target':'relation_withtrain_month'},
    {'table':'edge_person_airline_detail','rq':'hbrq','target':'relation_withair_month'}
]

## 人物图层计算明细按月前置表
def get_relation_travel_month():
    last_cp = '2020040000'
    tmp_sql = '''
        select sfzh1, sfzh2, timestamp2month({rq}) as cp from {table} where timestamp2month({rq}) > '%s'
    '''%last_cp
    for info in with_travel_table:
        read_orc(spark,add_save_path(info['table'])).createOrReplaceTempView(info['table'])
        df1 = spark.sql(tmp_sql.format(**info))
        #为了后面的人物图层的计算  这里需做成双向边  便于后面人物关系分的计算
        df = df1.unionAll(df1.selectExpr('sfzh2 sfzh1','sfzh1 sfzh2', 'cp'))
        ## 对每月进行分区统计,两人的同行次数
        df.persist(StorageLevel.MEMORY_AND_DISK)
        cps = [row.cp for row in df.selectExpr('cast(cp as string) cp').distinct().collect() if row.cp >= last_cp and  row.cp <= time.strftime("%Y%m0000", time.localtime())]
        logger.info('cps %s'%info['target'])
        logger.info('||'.join(cps))
        for cp in cps:
            res = df.where('cp=%s'%cp).groupby('sfzh1','sfzh2').agg(count('cp').alias('num'))
            res = res.withColumn('cp',lit(cp)).select('sfzh1','sfzh2','num','cp')
            write_orc(res,add_save_path(info['target'],cp=cp,root='person_relation_detail'))
            logger.info('%s %s down'%(info['table'],cp))
        df.unpersist()

if __name__ == '__main__':
    logger.info("=================deal with_travel_detail start time %s======================"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    today_cp = time.strftime("%Y%m0000", time.localtime())
    deal_relation_person()
    person_df = read_parquet(spark,path_prefix,'person_person')
    person_df.persist(StorageLevel.MEMORY_AND_DISK)
    # 航班
    get_with_airline_detail()
    get_with_trainline_detail()
    person_df.unpersist()
    edge_person_with_airline_travel()
    edge_person_with_trainline_travel()
    # 按月同行次数
    get_relation_travel_month()

    logger.info("=================deal with_travel_detail end time %s======================"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
