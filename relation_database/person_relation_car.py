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

##todo:all
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'

def write_orc(df, path,mode='overwrite'):
    df.write.format('orc').mode(mode).save(path)
    logger.info('write success')

def read_orc(path):
    try:
        df = spark.read.orc(path)
        return df
    except:
        return None



def edge_person_own_car():

    # nb_app_dws_per_res_his_dcveh	车与人关系汇总表
    # rel_type	关系类型
    # cert_type	证件类型
    # cert_num	证件号码
    # veh_no	车架号
    # drive_lic_tcode	机动车号牌种类代码
    # veh_plate_num	机动车号牌号码
    # last_dis_place	发现地
    # first_time	首次发现时间
    # last_time	最后发现时间
    # total_count	累计发现次数


    sql = '''
        select format_zjhm(cert_num) sfzh, valid_hphm(veh_plate_num,drive_lic_tcode) autolpn,
        'nb_app_dws_per_res_his_dcveh' tablename from nb_app_dws_per_res_his_dcveh
        where verify_sfz(cert_num) = 1 and verify_hphm(veh_plate_num) = 1 and rel_type = 'DH01'
    '''

    v_sql = '''
        select valid_hphm(veh_plate_num,drive_lic_tcode) autolpn, drive_lic_tcode hpdm,
        'nb_app_dws_per_res_his_dcveh' tablename from nb_app_dws_per_res_his_dcveh
        where verify_sfz(cert_num) = 1 and verify_hphm(veh_plate_num) = 1
    '''

    init(spark,'nb_app_dws_per_res_his_dcveh',if_write=False)
    df = spark.sql(sql).drop_duplicates(['sfzh','autolpn'])
    write_orc(df,add_save_path('edge_person_own_car',root=save_root))
    logger.info('edge_person_own_autolpn down')


    v_df = spark.sql(v_sql)
    hpzl_df = get_all_dict(spark).where('lb="hpzl_type"').select('dm','mc')

    v_res = v_df.join(hpzl_df,v_df['hpdm']==hpzl_df['dm']).selectExpr('autolpn','mc hpzl','tablename')
    write_orc(v_res,add_save_path('vertex_autolpn',root=save_root))
    logger.info('vertex_autolpn down')

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    edge_person_own_car()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))