# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 11:22
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : hotel.py
# @Software: PyCharm

from pyspark import SparkConf
from pyspark.sql import SparkSession
# from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time, copy, re, math
from datetime import datetime, timedelta,date
import os,sys
import logging

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

warehouse_location = '/user/hive/warehouse/'
conf=SparkConf().set('spark.driver.maxResultSize', '30g')
# conf.set('spark.yarn.am.cores', 5)
conf.set('soark.shuffle.partitions',800)
conf.set('spark.executor.memory', '10g')
conf.set('spark.executor.instances', 100)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')
conf.set("spark.sql.warehouse.dir", warehouse_location)

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commonUtil = CommonUdf(spark)
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/hotel'
##todo:all
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
    try:
        df = spark.read.orc(path)
        return df
    except:
        return None

def vertex_hotel():
    '''
    提取旅馆酒店节点的基本信息
    1、ODS_POL_SEC_INBORD_PERS_LODG	境内人员住宿信息
        HOTEL_NO	旅馆_编号
        HOTE_NAME	旅馆名称
        HOTEL_BUSIN_LICE_NO	旅馆_营业执照号
        HOTEL_ADDR_ADDR_NAME	旅馆地址_地址名称
    2、ODS_POL_SEC_OVERSEA_PERS_LODG	境外人员住宿信息
        HOTEL_ID	旅馆_ID
        HOTEL_NO	旅馆_编号
        HOTE_NAME	旅馆名称
    :return:
    '''
    table_list = [
        {'lgdm':'hotel_no','qymc':'hote_name','yyzz':'hotel_busin_lice_no',
         'xxdz':'hotel_addr_addr_name','tablename':'ods_pol_sec_inbord_pers_lodg',
         'cond': 'verify_sfz(coalesce(cert_no,cred_num))=1 and format_lgdm(hotel_no) != "" and trim(hote_name) !=""'},
        {'lgdm': 'hotel_no', 'qymc': 'hote_name', 'yyzz': 'hotel_busin_lice_no',
         'xxdz': 'hotel_addr_addr_name', 'tablename': 'ods_pol_sec_oversea_pers_lodg',
         'cond':'verify_zjhm(cred_num)=1 and format_lgdm(hotel_no) != "" and trim(hote_name) !=""'},
    ]

    tmp_sql = '''
        select format_lgdm({lgdm}) lgdm, {qymc} qiyemc, {yyzz} yyzz, {xxdz} xiangxidizhi,
        '{tablename}' tablename from {tablename} 
        where format_lgdm({lgdm}) != ''
    '''

    union_df = create_uniondf(spark,table_list,tmp_sql)

    res = union_df.drop_duplicates(['lgdm'])

    write_orc(res,add_save_path('vertex_hotel'))
    logger.info('vertex_hotel down')

def edge_person_stay_hotel_detail_inbord():
    '''
    国内旅客住宿增量
    '''
    sql = '''
        select format_zjhm(coalesce(cert_no,cred_num)) sfzh, format_lgdm(hotel_no) lgdm, trim(room_no) zwmc,
        cast(format_timestamp(admi_time) as bigint) start_time,cast(format_timestamp(chout_time) as bigint) end_time,
        'ods_pol_sec_inbord_pers_lodg' tablename from ods_pol_sec_inbord_pers_lodg
        where verify_sfz(coalesce(cert_no,cred_num)) = 1 and format_lgdm(hotel_no) != ''
    '''

    init_history(spark,'ods_pol_sec_inbord_pers_lodg',import_times=import_times,if_write=False)
    df = spark.sql(sql).repartition(10)
    write_orc(df,add_incr_path('edge_person_stay_inbord_detail',cp=cp))

def edge_person_stay_hotel_detail_oversea():
    '''
    国外旅客住宿增量
    '''
    sql = '''
                select format_zjhm(cred_num) sfzh, format_lgdm(hotel_no) lgdm, trim(room_no) zwmc,
                cast(format_timestamp(admi_time) as bigint) start_time,cast(format_timestamp(chout_time) as bigint) end_time,
                'ods_pol_sec_oversea_pers_lodg' tablename from ods_pol_sec_oversea_pers_lodg
                where verify_zjhm(cred_num) = 1 and format_lgdm(hotel_no) != ''
            '''

    init_history(spark, 'ods_pol_sec_oversea_pers_lodg', import_times=import_times, if_write=False)
    df = spark.sql(sql).repartition(10)
    write_orc(df, add_incr_path('edge_person_stay_oversea_detail', cp=cp))

def edge_person_stay_hotel_day():
    '''
    聚合海内，海外的住宿明细，为每天的明细表增量
    '''

    columns = ["sfzh", "lgdm","zwmc", "start_time", "end_time","tablename"]
    files = ['edge_person_stay_inbord_detail','edge_person_stay_oversea_detail']
    ## 国内外住宿明细
    dfs = []
    for file in files:
        df = read_orc(add_incr_path(file,cp=cp))
        if df and df.take(1):
            dfs.append(df.selectExpr(*columns))
    if dfs:
        all_df = reduce(lambda a,b:a.unionAll(b),dfs)
        ## 宾馆明细
        res = all_df.selectExpr("sfzh", "lgdm", "zwmc", "start_time", "end_time","tablename",
                        "row_number() over (partition by sfzh,lgdm,zwmc,start_time order by end_time desc) num").where("num=1").drop("num")
        res = res.repartition(200)
        write_orc(res,add_incr_path('edge_person_stay_hotel_detail',cp=cp))
        logger.info('edge_person_stay_hotel_detail down')

def edge_person_stay_hotel_detail():
    '''
    聚合每天的住宿明细，为总明细表
    '''
    columns = ["sfzh", "lgdm","zwmc", "start_time", "end_time","tablename"]

    ## 国内外住宿明细
    df1 = read_orc(add_incr_path('edge_person_stay_inbord_detail')).selectExpr(*columns)
    df2 = read_orc(add_incr_path('edge_person_stay_oversea_detail')).selectExpr(*columns)

    ## 宾馆明细
    df = df1.unionAll(df2)
    res = df.selectExpr("sfzh", "lgdm", "zwmc", "start_time", "end_time","tablename",
                    "row_number() over (partition by sfzh,lgdm,zwmc,start_time order by end_time desc) num").where("num=1").drop("num")
    res = res.repartition(200)
    write_orc(res,add_save_path('edge_person_stay_hotel_detail'))
    logger.info('edge_person_stay_hotel_detail down')

def edge_person_stay_hotel():
    ##聚合住宿明细
    create_tmpview_table(spark,'edge_person_stay_hotel_detail')

    sql = '''
        select sfzh,lgdm, min(start_time) as start_time, max(end_time) as end_time, count(sfzh) as num 
        from edge_person_stay_hotel_detail group by sfzh,lgdm
    '''
    logger.info(sql)

    df = spark.sql(sql).repartition(200)
    write_orc(df,add_save_path('edge_person_stay_hotel'))

    logger.info('edge_person_stay_hotel down')


def handle_data(tablename):

    switch = {
        'ods_pol_sec_inbord_pers_lodg':edge_person_stay_hotel_detail_inbord,
        'ods_pol_sec_oversea_pers_lodg':edge_person_stay_hotel_detail_oversea,
    }

    switch.get(tablename)()

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    if len(sys.argv) == 4:
        import_times = sys.argv[1].split(',')
        cp = str(sys.argv[2])
        tablename = str(sys.argv[3])
        handle_data(tablename)
    elif len(sys.argv) == 2:
        cp = sys.argv[1]
        edge_person_stay_hotel_day()
    else:
        edge_person_stay_hotel_detail()
        edge_person_stay_hotel()
        vertex_hotel()
    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))