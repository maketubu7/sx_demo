# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : company.py
# @Software: PyCharm
# @content : 工商相关信息

import sys, os
from pyspark import SparkConf,StorageLevel
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
conf.set('spark.executor.memoryOverhead', '15g')
conf.set('spark.yarn.am.cores', 4)
conf.set('spark.executor.memory', '15g')
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/workmate'
save_root = 'relation_theme_extenddir'

def _min(a, b):
    return a if a < b else b

def _max(a, b):
    return a if a > b else b

spark.udf.register('get_min_long',_min, IntegerType())
spark.udf.register('get_max_long',_max, IntegerType())

def edge_company_workmate():
    #新版  由于公司节点数据太大，因此 先用通联关联实名制，再去关联公司，判断公司ID一样 即可
    call = read_orc(spark,add_save_path('edge_groupcall',root=save_root))
    smz = read_orc(spark,add_save_path('edge_person_smz_phone_top',root=save_root))

    call.createOrReplaceTempView('call')
    smz.createOrReplaceTempView('smz')

    sql = '''  
        select /*+ BROADCAST (smz)*/ b.start_person, c.start_person as end_person, a.start_time, a.end_time
        from call a 
        inner join smz b on a.start_phone=b.end_phone
        inner join smz c on a.end_phone=c.end_phone
        where b.start_person is not null and c.start_person is not null
    '''
    cols = ["if(start_person>end_person,start_person,end_person) as start_person","if(start_person>end_person,end_person,start_person) as  end_person", "start_time", "end_time"]
    df = spark.sql(sql).dropDuplicates(['start_person','end_person']).selectExpr(*cols)
    df.persist(StorageLevel.MEMORY_AND_DISK)

    get_company_workmate(df)
    get_school_workmate(df)

    company_df = read_parquet(spark,path_prefix,'company_workmate')
    school_df = read_parquet(spark,path_prefix,'school_workmate')
    cols = ['start_person sfzh1','end_person sfzh2','start_time','end_time','dwmc']
    res = company_df.unionAll(school_df).where('dwmc != "" and start_person != end_person') .selectExpr(*cols).dropDuplicates(['sfzh1','sfzh2'])
    write_orc(res,add_save_path('edge_company_workmate',root=save_root))
    df.unpersist()
    logger.info('edge_company_workmate down')

def get_company_workmate(df):
    edge_person_work_com = read_orc(spark,add_save_path('edge_person_work_com',root=save_root))
    edge_person_legal_com = read_orc(spark,add_save_path('edge_person_legal_com',root=save_root))

    edge_person_work_com = edge_person_work_com.union(edge_person_legal_com)

    edge_person_work_com.createOrReplaceTempView('work_com')
    df.createOrReplaceTempView('re_call')

    sql = '''
        select /*+ BROADCAST (work_com)*/ b.start_person, c.start_person as end_person, 
        get_max_long(b.start_time,c.start_time) start_time, get_min_long(b.end_time,c.end_time) end_time,
        b.end_company as company from re_call a 
        inner join work_com b on a.start_person=b.start_person
        inner join work_com c on a.end_person=c.start_person
        where b.end_company = c.end_company
    '''

    res_tmp = spark.sql(sql).where('end_time >= start_time')

    write_parquet(res_tmp,path_prefix,'res_tmp')
    res_tmp = read_parquet(spark,path_prefix,'res_tmp')

    #关联公司名
    company_df = read_orc(spark,add_save_path('vertex_company',root=save_root)) \
            .where("format_data(dwmc) != ''").selectExpr('company as start_company','dwmc')
    cols = ['start_person','end_person','start_time','end_time','dwmc']
    df9 = res_tmp.join(company_df,res_tmp.company==company_df.start_company,'inner').selectExpr(*cols)
    df9.createOrReplaceTempView("table1")
    #得到公司的同事所有信息
    res = spark.sql("select start_person,end_person,min(start_time) as start_time, max(end_time) as end_time, concat_ws(',',collect_set(dwmc)) as dwmc from table1 group by start_person,end_person")
    write_parquet(res,path_prefix,'company_workmate')
    logger.info('company_workmate down')

def get_school_workmate(df):
    edge_person_work_school = read_orc(spark,add_save_path('edge_person_work_school',root=save_root))
    edge_person_work_school.createOrReplaceTempView('work_school')
    df.createOrReplaceTempView('re_call')

    sql = '''
        select /*+ BROADCAST (work_school)*/ b.start_person, c.start_person as end_person, 
        get_max_long(b.start_time,c.start_time) start_time, get_min_long(b.end_time,c.end_time) end_time,
        b.end_school_id as company from re_call a 
        inner join work_school b on a.start_person=b.start_person
        inner join work_school c on a.end_person=c.start_person
        where b.end_school_id = c.end_school_id
    '''
    cols = ['start_person','end_person','start_time','end_time','company']
    res_tmp = spark.sql(sql).where("end_time >= start_time").selectExpr(*cols)

    #关联学校名
    schoolname_df = read_orc(spark,add_save_path('vertex_school',root=save_root)) \
        .selectExpr('school_id','name dwmc').where('format_data(dwmc) != ""')
    df_res = res_tmp.join(schoolname_df,res_tmp.company==schoolname_df.school_id,'inner') \
            .selectExpr('start_person','end_person','start_time','end_time','dwmc')
    df_res.createOrReplaceTempView("table1")
    #得到公司的同事所有信息
    res = spark.sql("select start_person,end_person,min(start_time) as start_time,max(end_time) as end_time,concat_ws(',',collect_set(dwmc)) as dwmc from table1 group by start_person,end_person")
    write_parquet(res,path_prefix,'school_workmate')
    logger.info('company_workmate down')

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    edge_company_workmate()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))