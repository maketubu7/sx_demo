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

##todo:all
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/relatiion_school'
save_root = 'relation_theme_extenddir'

def person_relate_school():
    # cert_type 证件类型
    # cert_num 证件号码
    # school_no 学校编码
    # school_name 学校名称
    # rel_type 关系类型
    # school_addr 学校地址
    # admission_time 入学时间
    # graduation_time 毕业时间
    # grade 年级
    # class 班级
    # totle_count 累计发现次数
    # totle_days 累计发现天数
    # first_time 首次关联时间, 绝对秒
    # last_time 最晚关联时间, 绝对秒

    #DSCH001	雇佣
    # DSCH002	就读
    init(spark, 'nb_app_dws_per_add_his_dcschool', if_write=False, is_relate=True)

    sql = '''
        select cert_num start_person, md5(format_data(school_name)) end_school_id,school_addr,valid_datetime(admission_time) start_time,
        valid_datetime(graduation_time) end_time,school_name xxmc, '' xymc, class bj, '' zymc, '' xh, '' xz, 
        rel_type type from nb_app_dws_per_add_his_dcschool where format_data(school_name) != ''
    '''

    source = spark.sql(sql)

    df1 = source.where('type="DSCH002"').drop('type').dropDuplicates(['start_person','end_school_id'])
    write_orc(df1,add_save_path('edge_person_attend_school',root=save_root))


    cols = ['start_person','end_school_id','start_time','end_time','"" position_name']
    df2 = source.where('type="DSCH001"').drop('type').selectExpr(*cols).dropDuplicates(['start_person','end_school_id'])
    write_orc(df2, add_save_path('edge_person_work_school', root=save_root))

def vertex_school():
    # ods_gov_edu_scho_info	学校信息
    # scho_no	学校编号
    # scho_name	学校名称
    # his_scho_name	历史名称
    # scho_addr_name	学校地址
    init(spark,'ods_gov_edu_scho_info',if_write=False)

    sql = '''
        select md5(format_data(scho_name)) school_id, scho_name name, his_scho_name history_name, 
        scho_addr_name school_address,'ods_gov_edu_scho_info' tablename
        from ods_gov_edu_scho_info where scho_name != ""
    '''
    res1 = spark.sql(sql)
    ## 原始数据提取的学校
    res2 = read_orc(spark,add_save_path('vertex_school_bbd',root=save_root))
    res = res1.unionAll(res2).dropDuplicates(['school_id'])
    write_orc(res,add_save_path('vertex_school',root=save_root))

def vertex_school_from_source():
    table_list = [
        {'tablename':'ods_gov_edu_kindergarten_stu','s_name':'kindergarten_desig','s_address':'kindergarten_addr_name',
         "s_his_name":'""','table_sort':3},
        {'tablename':'ods_gov_edu_scho_info','s_name':'scho_name','s_address':'scho_addr_name',
         "s_his_name":'his_scho_name','table_sort':1},
        {'tablename':'ods_gov_edu_stud_info','s_name':'scho_name','s_address':'""',"s_his_name":'""',
         'table_sort':2},
    ]

    tmp_sql = ''' 
        select md5(format_data({s_name})) school_id, format_data({s_name}) name, 
        {s_his_name} history_name, {s_address} school_address,
        '{tablename}' tablename, {table_sort} table_sort from {tablename}
        where verify_school({s_name}) = 1
    '''

    res = create_uniondf(spark,table_list,tmp_sql)
    df_res = res.selectExpr('school_id','name','history_name','school_address','tablename',
                'row_number() over (partition by school_id order by table_sort asc) num'). \
                where("num=1").drop("num").drop('table_sort')
    write_orc(df_res,add_save_path('vertex_school_bbd',root=save_root))

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    person_relate_school()
    vertex_school_from_source()
    vertex_school()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))