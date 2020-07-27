# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 15:35
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : phonenumber.py
# @Software: PyCharm

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
from pyspark import SparkConf
import os,sys

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

conf=SparkConf().set('spark.driver.maxResultSize', '15g')
conf.set('spark.yarn.am.cores', 4)
conf.set('soark.shuffle.partitions',2000)
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/phonenumber'
save_root = "relation_theme_extenddir"

zj_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/zj_location.csv'
sj_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/sj_location.csv'

def vertex_phonenumber():

    table_list = [
        {'tablename': 'edge_groupcall', 'phone': 'start_phone'},
        {'tablename': 'edge_groupcall', 'phone': 'end_phone'},
        {'tablename': 'edge_groupmsg', 'phone': 'start_phone'},
        {'tablename': 'edge_groupmsg', 'phone': 'end_phone'},
        {'tablename': 'edge_phone_call_alert', 'phone': 'phone'},
        {'tablename': 'edge_phone_send_package', 'phone': 'phone'},
        {'tablename': 'edge_phone_receive_package', 'phone': 'phone'},
        {'tablename': 'edge_phone_sendpackage_phone', 'phone': 'start_phone'},
        {'tablename': 'edge_phone_sendpackage_phone', 'phone': 'end_phone'},
        {'tablename': 'edge_person_open_phone', 'phone': 'phone'},
        {'tablename': 'edge_person_link_phone', 'phone': 'phone'},
        {'tablename': 'edge_phone_link_qq', 'phone': 'phone'},
        {'tablename': 'edge_phone_link_wechat', 'phone': 'phone'},
        {'tablename': 'edge_phone_link_sinablog', 'phone': 'phone'},
        {'tablename': 'edge_phonenumber_use_imei', 'phone': 'phone'},
        {'tablename': 'edge_phonenumber_use_imsi', 'phone': 'phone'},
    ]


    tmp_sql = ''' select {phone} phone,"{tablename}" tablename from {tablename} '''

    dfs = []
    for info in table_list:
        sql = tmp_sql.format(**info)
        if create_tmpview_table(spark, info['tablename'],root=save_root):
            df = spark.sql(sql)
            dfs.append(df)

    union_df = reduce(lambda x, y: x.unionAll(y), dfs)

    union_df = union_df.drop_duplicates(['phone'])

    df = union_df.selectExpr('phone', 'tablename','substr(phone,1,7) as area',
                             'substr(phone,1,1) as pre1','substr(phone,1,3) as pre3',
                             'substr(phone,1,3) as pre4')

    sj_location = spark.read.csv(sj_path,header=True,sep='\t')
    df_sj = df.where('pre1 != "0"')
    df_sj_res = df_sj.join(sj_location, df_sj.area == sj_location.pre7, 'left') \
        .select(df_sj.phone, sj_location.province, sj_location.city,df_sj.tablename)
    write_parquet(df_sj_res,path_prefix,'df_sj_res')

    # 座机的数据-010 020 021 022 023 024 025 027 028 029  3位区号的
    df_zj1 = df.where("pre1=='0' and pre3 in('010','020','021','022','023','024','025','027','028','029')")

    zj_location = spark.read.csv(zj_path,header=True,sep='\t')

    df_zj1_res = df_zj1.join(zj_location, df_zj1.pre3 == zj_location.qhao, 'left') \
        .select(df_zj1.phone, zj_location.province, zj_location.city, df_zj1.tablename)
    df_zj1_res = df_zj1_res.na.fill({'province': '', 'city': ''})

    write_parquet(df_zj1_res, path_prefix,'df_zj1_res')
    logger.info(' df_zj1_res down')

    # 座机4位区号的
    df_zj2 = df.where("pre1=='0' and pre3 not in('010','020','021','022','023','024','025','027','028','029')")
    df_zj2_res = df_zj2.join(zj_location, df_zj2.pre4 == zj_location.qhao, 'left') \
        .select(df_zj2.phone, zj_location.province, zj_location.city, df_zj2.tablename)
    df_zj2_res = df_zj2_res.na.fill({'province': '', 'city': ''})
    write_parquet(df_zj2_res, path_prefix, 'df_zj2_res')
    logger.info(' df_zj2_res down')

    df_zj2_res = read_parquet(spark,path_prefix,'df_zj2_res')
    df_zj1_res = read_parquet(spark,path_prefix,'df_zj1_res')
    df_sj_res = read_parquet(spark,path_prefix,'df_sj_res')

    res_tmp = df_sj_res.unionAll(df_zj1_res).unionAll(df_zj2_res).drop_duplicates(['phone'])
    smz = read_orc(spark,add_save_path('edge_person_smz_phone_top',root=save_root))

    ## 添加实名制姓名
    res_tmp = res_tmp.join(smz,res_tmp.phone==smz.end_phone,'left') \
            .selectExpr('phone','start_person','province','city')

    write_parquet(res_tmp,path_prefix,'res_tmp')
    cols = ['phone','"" xm','province','city']
    person = read_orc(spark,add_save_path('vertex_person',root=save_root))
    res1 = read_parquet(spark,path_prefix,'res_tmp').where('start_person is null').selectExpr(*cols)
    res_tmp = read_parquet(spark,path_prefix,'res_tmp').where('start_person is not null')

    res2 = res_tmp.join(person,res_tmp.start_person==person.zjhm,'left') \
            .selectExpr('phone','xm','province','city').na.fill({'xm':''})
    res3 = read_orc(spark,add_save_path('vertex_phonenumber_zp',root=save_root)).selectExpr('phone','xm','province','city')
    res = res1.unionAll(res2).unionAll(res3).drop_duplicates(['phone'])


    write_orc(res,add_save_path('vertex_phonenumber',root=save_root))
    logger.info('vertex_phonenumber down')


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    vertex_phonenumber()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
