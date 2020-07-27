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


def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix,'parquet/',path))
    return df
def write_parquet(df,path):
    '''写 parquet'''
    df.write.mode("overwrite").parquet(path=os.path.join(path_prefix,'parquet/',path))


def write_orc(df, path):
    df.write.format('orc').mode('overwrite').save(path)
    logger.info('write success')

def read_orc(path):
    df = spark.read.orc(path)
    return df

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
    '''航班节点 '''
    for info in table_list:
        df = read_orc(add_incr_path(info['path']))
        df = df.withColumn('table_sort',lit(info['table_sort']))
        df.createOrReplaceTempView("tmp_view")
        sql1 = ''' select airlineid,hbh,hbrq,yjqfsj,sfd,table_sort,tablename,
                            row_number() over(partition by airlineid order by yjqfsj asc) as num from tmp_view '''
        sql2 = ''' select airlineid airline_id2,yjddsj,mdd,
                            row_number() over(partition by airlineid order by yjddsj desc) as num from tmp_view '''
        df1 = spark.sql(sql1).where('num=1').drop('num')
        df2 = spark.sql(sql2).where('num=1').drop('num')
        df3 = df1.join(df2, df1.airlineid == df2.airline_id2, 'left') \
            .select(df1.airlineid, df1.hbh, df1.hbrq, df1.yjqfsj, df2.yjddsj, df1.sfd, df2.mdd, df1.tablename,df1.table_sort)
        write_parquet(df3, 'pt/%s' % info['path'])
    dfs = []
    for info in table_list:
        df = read_parquet('pt/%s' % info['path'])
        dfs.append(df)

    union_df = reduce(lambda x,y:x.unionAll(y),dfs)
    union_df.createOrReplaceTempView("tmp_view")
    sql = ''' select airlineid,hbh,hbrq,yjqfsj,yjddsj,sfd,mdd,tablename,
                row_number() over(partition by airlineid order by table_sort asc) as num from tmp_view '''

    res = spark.sql(sql).where('num=1').drop('num')
    write_orc(res, add_save_path('vertex_airline'))
    logger.info('vertex_airline down')

def edge_person_airline_day(tablename,import_times,cp):
    '''
    每天的数据提取
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
                    verify_sfz({zjhm})=1 and trim({hbh}) !='' '''

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
    人订机票
    1、ODS_SOC_CIV_AVIA_RESE	民航订票
        CRED_NUM	证件号码
        CARR_ORGA_AIRLINE_CODE	承运单位_航空公司代码
        CARR_ORGA_AIRLINE_NAME	承运单位_航空公司名称
        FLINUM	航班号
        FLINUM_POFIX	航班号后缀
        FLY_INIT_NRT_AIRPORT_CODE	起飞机场_机场代码
        FLY_INIT_NRT_AIRPORT_NAME	起飞机场_机场名称
        STAOFF_DATE	出发日期
        STAOFF_TIME	出发时间
        ARR_AT_AIR_AIRPORT_CODE	到达机场_机场代码
        ARR_AT_AIR_AIRPORT_NAME	到达机场_机场名称
        ARR_DATE	到达日期
        ARR_TIME	到达时间
        BOOTIC_NO	订票编号
    :return:
    '''
    cols = ["sfzh", "airlineid", "hbh", "hbrq", "sfd", "mdd","yjqfsj","yjddsj",
            "tablename","yjqfsj start_time","yjddsj end_time",
            "row_number() over(partition by sfzh,airlineid order by hbrq desc) num"]
    for info in table_list:
        df = read_orc(add_incr_path(info['path']))
        df2 = df.selectExpr(*cols).where("num=1").drop("num")
        target = info['path'][:-7]
        write_orc(df2,add_save_path(target))
        logger.info('%s down'%target)

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