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
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
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

def create_tmpview_table(tablename,cp=''):
    '''
    读取所有内容
    :param tablename:
    :param cp: cp='' 读取所有内容
    :return:
    '''
    tmp_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/{}/*'
    if not cp:
        df = read_orc(tmp_path.format(tablename))
        df.createOrReplaceTempView(tablename)
        logger.info('create tableview %s down'%tablename)
    else:
        df = read_orc(add_save_path(tablename, cp))
        df.createOrReplaceTempView(tablename)
        logger.info('create tableview %s cp=%s down' % (tablename,cp))

def vertex_company():
    '''
    公司节点信息
    1、ODS_GOV_IND_INDCOMPR_REGINF	工商主体登记信息
        IACR_NUM	工商注册号码
        ORG_CODE	组织机构代码
        ORG_NAME	组织机构名称
        SOC_CREDIT_CODE	统一社会信用代码
        COMP_NAME	单位名称
        ESTBL_DATE	成立日期
        REG_CAPIITAL	注册资本
        COM_COMFOR_DESC	单位_组成形式描述
        PRAC_PER_NUM	从业_人数
        BUSIN_RANGE_MAIN	经营范围（主营）
        BUSIN_LIMIT_TIME_DUE_DATE	经营期限_截止日期
        BUSIN_LIMIT_TIME_START_DATE	经营期限_起始日期
    2、ODS_GOV_SSE_SOCSEC_PAYFEE_INFO	社保缴费信息
        COMP_NAME	单位名称
    :return:
    '''

    sql = '''
    		select	MD5(trim(comp_name)) as company,
                    trim(comp_name) as dwmc,
    				comp_ra_addr_name as dwdz,
    				org_code as zzjgdm,
    				soc_credit_code as tyshdm,
                    valid_datetime(estbl_date) as clrq,
                    busin_state_name as jyzt,
    				format_data(busin_range_main) as jyfw,
    				unit_tname as dwlx,
    				cast(trim(reg_capiital) as bigint)  as zczb,
    				cast(trim(prac_per_num) as bigint)  as cyrs,
    				trim(ind_tname) as hylb,
    				produc_busin_addr_addr_name as dwscdz,
    				indcompr_chaite_name as bgxq,
    				chan_date as bgrq,
    				'ods_gov_ind_indcompr_reginf' as tablename
    		   from ods_gov_ind_indcompr_reginf
              where format_data(comp_name) != ''
             '''

    init(spark, 'ods_gov_ind_indcompr_reginf', if_write=False)
    df = spark.sql(sql)

    df_result = df.selectExpr("*", "row_number() over(partition by company order by clrq desc) as num") \
                .where('num=1').drop('num')

    res = df_result.selectExpr('company', "dwmc", "dwdz", 'zzjgdm', "tyshdm", 'clrq', 'jyzt', 'jyfw', 'dwlx',
                               "zczb", "cyrs","hylb", "dwscdz", "bgxq", "bgrq", "tablename").na.fill({"zczb":0,"cyrs":0})

    write_orc(res, add_save_path('vertex_company'))

    ##todo 增加其他来源的公司信息
    pass


def edge_person_work_com():
    '''
    任职关系表
    1、ODS_GOV_SSE_SOCSEC_PAYFEE_INFO	社保缴费信息
    CERT_NO	公民身份号码
    ENTRY_TIME	入职时间
    COMP_NAME	单位名称
    :return:
    '''
    pass

def edge_person_legal_com():
    '''
    法定代表人
    1、ODS_GOV_IND_INDCOMPR_REGINF	工商主体登记信息
        LAW_REPR_CRED_NUM	法定代表人_证件号码
        IACR_NUM	工商注册号码
        ORG_CODE	组织机构代码
        ORG_NAME	组织机构名称
        SOC_CREDIT_CODE	统一社会信用代码
        COMP_NAME	单位名称
    :return:
    '''
    init(spark, 'ods_gov_ind_indcompr_reginf', if_write=False)
    sql = '''
            select format_zjhm(law_repr_cred_num) as start_person, MD5(trim(comp_name)) as end_company,valid_datetime(estbl_date) start_time, 0 end_time,
              'ods_gov_ind_indcompr_reginf' tablename,
              row_number() over(partition by format_zjhm(law_repr_cred_num),trim(comp_name) order by valid_datetime(estbl_date) desc) as num
              from ods_gov_ind_indcompr_reginf
             where law_repr_cred_num is not null and verify_sfz(law_repr_cred_num) = 1
        '''
    df_result = spark.sql(sql).where('num=1').drop('num')
    write_orc(df_result,add_save_path('edge_person_legal_com'))
    logger.info('edge_person_legal_com down')

def edge_person_finanical_com():
    '''
    财务负责人
    1、ODS_GOV_IND_INDCOMPR_REGINF	工商主体登记信息
        FINAN_PRIN_PER_COMM_CERT_CODE	财务_负责人_常用证件代码
        FINAN_PRIN_PER_COMM_CERT_NAME	财务_负责人_常用证件名称
        IACR_NUM	工商注册号码
        ORG_CODE	组织机构代码
        ORG_NAME	组织机构名称
        SOC_CREDIT_CODE	统一社会信用代码
        COMP_NAME	单位名称
    :return:
    '''
    ## todo: 财务负责人为空暂时不做
    pass

def edge_workmate():
    '''
    同事关系表
    前置
        任职关系，实名制
    1、ODS_GOV_SSE_SOCSEC_PAYFEE_INFO	社保缴费信息
    CERT_NO	公民身份号码
    ENTRY_TIME	入职时间
    COMP_NAME	单位名称
    :return:
    '''
    ## todo 社保缴费为空，无法提取同事
    pass



if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    vertex_company()
    edge_person_legal_com()
    edge_person_finanical_com()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))