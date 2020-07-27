# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : vertex_school.py
# @Software: PyCharm
# @content : 公司节点信息

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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'

def vertex_company():

    # ods_gov_ind_actu_unit	实有单位信息
    # com_no	单位编号
    # supe_unit_no	上级单位ID
    # soc_credit_code	统一社会信用代码
    # comp_name	单位名称
    # ind_tname	行业类别
    # unit_tname	单位类别
    # legal_name	法人姓名
    # legal_cert_no	法人身份证号
    # com_admin_name	单位地址行政区划
    # com_addi_detail_addr	单位详址
    # com_door_no	单位门楼牌号
    # com_door_deta_addr	单位门楼牌详址
    # fore_unit_judge_flag	是否外资单位代码
    # busin_lice_no	营业执照号
    # reg_capiital	注册资金
    # org_code	组织机构代码
    # tax_reg_no	税务登记号码
    # busin_range_main	经营范围（主营）
    # busin_meth_name	经营方式
    # start_busi_date	开业日期
    # close_date	停业日期
    # canc_judge_flag	注销标志
    # canc_reas	注销原因

    init(spark,'ods_gov_ind_actu_unit',if_write=False)

    sql = '''
    		select	md5(format_data(comp_name) ) as company,
                    format_data(comp_name)  as dwmc,
    				com_addi_detail_addr as dwdz,
    				org_code as zzjgdm,
    				soc_credit_code as tyshdm,
                    valid_datetime(start_busi_date) as clrq,
                    busin_meth_name as jyzt,
    				format_data(busin_range_main) as jyfw,
    				unit_tname as dwlx,
    				trim(reg_capiital) as zczb,
    				'' as cyrs,
    				trim(ind_tname) as hylb,
    				com_addi_detail_addr as dwscdz,
    				'' as bgxq,
    				'' as bgrq,
    				'ods_gov_ind_indcompr_reginf' as tablename
    		   from ods_gov_ind_actu_unit
              where format_data(comp_name) != ''  and verify_company_name(format_data(comp_name)) = 1
             '''
    df = spark.sql(sql)

    df_result = df.selectExpr("*", "row_number() over(partition by company order by clrq desc) as num") \
                .where('num=1').drop('num')

    res1 = df_result.selectExpr('company', "dwmc", "dwdz", 'zzjgdm', "tyshdm", 'clrq', 'jyzt', 'jyfw', 'dwlx',
                               "zczb", "cyrs","hylb", "dwscdz", "bgxq", "bgrq", "tablename","1 table_sort")

    ## 自己提取的原始数据vertex_company_from_source
    write_orc(res1, add_save_path('vertex_company_fh',root=save_root))
    res1 = read_orc(spark,add_save_path('vertex_company_fh',root=save_root))
    res2 = read_orc(spark,add_save_path('vertex_company_bbd',root=save_root))
    res3 = read_orc(spark,add_save_path('vertex_company_work',root=save_root))
    res4 = read_orc(spark,add_save_path('vertex_company_case',root=save_root))
    res = res1.unionAll(res2).unionAll(res3).unionAll(res4) \
        .selectExpr('*','row_number() over(partition by company order by table_sort) as num') \
        .where('num=1').drop('num').drop('table_sort')
    write_orc(res, add_save_path('vertex_company',root=save_root))

def vertex_company_from_source():
    sql = '''
    		select	MD5(format_data(comp_name) ) as company,
                    format_data(comp_name)  as dwmc,
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
              where format_data(comp_name) != '' and verify_company_name(format_data(comp_name)) = 1
             '''

    init(spark, 'ods_gov_ind_indcompr_reginf', if_write=False)
    df = spark.sql(sql)

    df_result = df.selectExpr("*", "row_number() over(partition by company order by clrq desc) as num") \
                .where('num=1').drop('num')

    res = df_result.selectExpr('company', "dwmc", "dwdz", 'zzjgdm', "tyshdm", 'clrq', 'jyzt', 'jyfw', 'dwlx',
                               "zczb", "cyrs","hylb", "dwscdz", "bgxq", "bgrq", "tablename","2 table_sort").na.fill({"zczb":0,"cyrs":0})

    write_orc(res, add_save_path('vertex_company_bbd',root=save_root))

def vertex_company_from_work():
    sql = '''
    		select	MD5(format_data(company_name)) as company,
                    format_data(company_name) as dwmc,
    				"" as dwdz,
    				"" as zzjgdm,
    				"" as tyshdm,
                    0 as clrq,
                    "" as jyzt,
    				"" as jyfw,
    				"" as dwlx,
    				0 as zczb,
    				0  as cyrs,
    				"" as hylb,
    				"" as dwscdz,
    				"" as bgxq,
    				"" as bgrq,
    				'nb_app_dws_per_org_his_dccpy' as tablename,
    				3 as table_sort
    		   from nb_app_dws_per_org_his_dccpy
              where format_data(company_name) != '' and verify_company_name(format_data(company_name)) = 1
             '''

    init(spark, 'nb_app_dws_per_org_his_dccpy', if_write=False,is_relate=True)
    df = spark.sql(sql).dropDuplicates(['company'])
    write_orc(df, add_save_path('vertex_company_work',root=save_root))

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    vertex_company_from_source()
    vertex_company_from_work()
    vertex_company()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))