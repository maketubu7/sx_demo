# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 14:42
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : airline.py
# @Software: PyCharm
# @content : 航班相关信息

import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
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

conf = SparkConf().set('spark.driver.maxResultSize', '30g')
# conf.set('spark.yarn.am.cores', 5)
conf.set('soark.shuffle.partitions',800)
conf.set('spark.executor.memory', '20g')
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
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/airline'
save_root = 'relation_theme_extenddir'

table_list = [
        ##订票
        {'tablename': 'ods_soc_civ_avia_rese','zjhm':'cred_num', 'hbh': 'concat(carr_orga_airline_code,flinum)', 'hbrq': 'staoff_date',
         'yjqfsj': 'staoff_time', 'yjddsj': 'arr_time', 'sfd': 'fly_init_nrt_airport_name',
         'mdd': 'arr_at_air_airport_name', 'table_sort': 2,'path':'edge_person_reserve_airline_detail'
         },
        ##离港
        {'tablename':'ods_soc_civ_avia_leaport_data','zjhm':'cred_num','hbh':'concat(airline_code,flinum)','hbrq':'flight_date',
         'yjqfsj':'sail_time','yjddsj':'dock_time','sfd':'fly_init_nrt_airport_name',
         'mdd':'arr_at_air_airport_name','table_sort': 1,'path':'edge_person_checkin_airline_detail'},
        ##到港
        {'tablename': 'ods_soc_civ_avia_arrport_data','zjhm':'cred_num','hbh': 'flinum','hbrq':'flight_date',
         'yjqfsj':'sail_time','yjddsj':'dock_time','sfd':'fly_init_nrt_airport_name',
         'mdd': 'arr_at_air_airport_name','table_sort':3,'path':'edge_person_arrived_airline_detail'}
    ]

def vertex_airline_from_edge():
    '''从航班关系中获取航班节点 '''
    for info in table_list:
        df = read_orc(spark,add_incr_path(info['path']))
        df = df.withColumn('table_sort',lit(info['table_sort']))
        df.createOrReplaceTempView("tmp_view")
        sql1 = ''' select airlineid,hbh,hbrq,yjqfsj,sfd,table_sort,tablename,
                            row_number() over(partition by airlineid order by yjqfsj asc) as num from tmp_view where valid_hbh(hbh) = 1 '''
        sql2 = ''' select airlineid airline_id2,yjddsj,mdd,
                            row_number() over(partition by airlineid order by yjddsj desc) as num from tmp_view where valid_hbh(hbh) = 1 '''
        df1 = spark.sql(sql1).where('num=1').drop('num')
        df2 = spark.sql(sql2).where('num=1').drop('num')
        df3 = df1.join(df2, df1.airlineid == df2.airline_id2, 'left') \
            .select(df1.airlineid, df1.hbh, df1.hbrq, df1.yjqfsj, df2.yjddsj, df1.sfd, df2.mdd, df1.tablename,df1.table_sort)
        write_parquet(df3, path_prefix,'pt/%s' % info['path'])
    dfs = []
    for info in table_list:
        df = read_parquet(spark,path_prefix,'pt/%s' % info['path'])
        dfs.append(df)

    union_df = reduce(lambda x,y:x.unionAll(y),dfs)
    union_df.createOrReplaceTempView("tmp_view")
    sql = ''' select airlineid,hbh,hbrq,yjqfsj,yjddsj,sfd,mdd,tablename,
                row_number() over(partition by airlineid order by table_sort asc) as num from tmp_view '''

    res = spark.sql(sql).where('num=1').drop('num')
    write_orc(res, add_save_path('vertex_airline',root=save_root))
    logger.info('vertex_airline down')

def edge_person_airline_day(tablename,import_times,cp):
    '''
    每天的航班数据提取
    '''
    tmp_sql = '''select format_zjhm({zjhm}) sfzh,
                    format_lineID({hbh},{hbrq}) airlineid,
                    format_data({hbh}) hbh,          
                    cast(date2timestampstr({hbrq}) as bigint) hbrq,
        			cast({yjqfsj} as bigint) as yjqfsj,
                    cast({yjddsj} as bigint) as yjddsj,			
                    format_data({sfd}) as sfd,
                    format_data({mdd}) as mdd,
                    '{tablename}' tablename
                    from {tablename} where 
                    verify_sfz({zjhm})=1 and trim({hbh}) !='' and valid_hbh({hbh}) = 1 '''

    for info in table_list:
        if info['tablename'] == tablename:
            sql = tmp_sql.format(**info)
            init_history(spark,tablename,import_times=import_times,if_write=False)
            df = spark.sql(sql)
            # 新增航班号以及航班日期，以及去重
            df2 = df.selectExpr("sfzh", "airlineid", "hbh", "hbrq", "sfd", "mdd","yjqfsj","yjddsj", "tablename",
                                "row_number() over(partition by sfzh,airlineid order by hbrq desc) num") \
                .where("num=1").drop("num").repartition(20)

            write_orc(df2, add_incr_path(info['path'], cp=cp))
            logger.info('%s down'%info['path'])

    for info in table_list:
        if info['tablename'] == tablename:
            sql = tmp_sql.format(**info)
            init_history(spark,tablename,import_times=import_times,if_write=False)
            df = spark.sql(sql)
            # 新增航班号以及航班日期，以及去重
            df2 = df.selectExpr("sfzh", "airlineid", "hbh", "hbrq", "sfd", "mdd","yjqfsj","yjddsj", "tablename",
                                "row_number() over(partition by sfzh,airlineid order by hbrq desc) num") \
                .where("num=1").drop("num").repartition(20)

            write_orc(df2, add_incr_path(info['path'], cp=cp))
            logger.info('%s down'%info['path'])

def edge_person_airline():
    '''
    航班数据处理，将三种关系合并为一个
    '''
    cols = ["sfzh", "airlineid", "hbh", "hbrq", "sfd", "mdd","yjqfsj","yjddsj","tablename"]
    dfs = []
    for info in table_list:
        df = read_orc(spark,add_incr_path(info['path'])).where('valid_hbh(hbh)=1').selectExpr(*cols)
        dfs.append(df)
    exprs = ['*','case tablename when "ods_soc_civ_avia_rese" then "订票" when "ods_soc_civ_avia_leaport_data" then "安检" else "到港" end as type_name ']
    reduce(lambda a,b:a.unionAll(b),dfs).selectExpr(*exprs).createOrReplaceTempView('tmp')
    sql = '''
        select sfzh,airlineid,max(hbh) hbh, max(hbrq) hbrq, concat_ws("|",collect_set(type_name)) type_name, 
        max(sfd) sfd, max(mdd) mdd,max(yjqfsj) yjqfsj, max(yjddsj) yjddsj , max(yjqfsj) start_time, 
        max(yjddsj) end_time from tmp group by sfzh,airlineid
    '''
    res = spark.sql(sql)
    write_orc(res,add_save_path('edge_person_reserve_airline',root=save_root))
    logger.info('edge_person_reserve_airline down')

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    if len(sys.argv) == 4:
        import_times = sys.argv[1].split(',')
        cp = str(sys.argv[2])
        tablename = str(sys.argv[3])
        edge_person_airline_day(tablename,import_times,cp)
    else:
        edge_person_airline()
        vertex_airline_from_edge()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))