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
conf.set('spark.executor.memoryOverhead', '2g')
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
    df = spark.read.orc(path)
    return df

def read_csv(file):
    path = 'file:///opt/workspace/sx_graph/wa_tmp/company_case/bbd_company/%s.csv'%file
    df = spark.read.csv(path,header=True,sep='\t')
    if df.take(1):
        return df

def write_csv(df, file, header=False, delimiter='\t'):
    '''写csv文件'''
    path = 'file:///opt/workspace/sx_graph/wa_tmp/company_case/bbd_company/%s.csv'%file
    df.write.mode('overwrite').csv(path, header=header, sep=delimiter, quote='"',escape='"')




def company_shareholder():
    share_cols = ['md5(format_com_name(company)) company','md5(format_com_name(share_company)) share_company',
                  'format_com_name(company) company_name','format_com_name(share_company) share_company_name','is_history','start_time','end_time']
    new_sharer_cols = ['company','shareholder_name share_company','0 is_history','0 start_time','0 end_time']
    all_com_share = read_csv('all_com_shareholder').selectExpr(*new_sharer_cols)
    his_share_share = read_csv('all_his_share_sub_shareholder').selectExpr(*new_sharer_cols)
    bank_share = read_csv('all_bank_sharer').selectExpr(*new_sharer_cols)
    bank_sub_3 = read_csv('yh_sub3_shareholder').selectExpr(*new_sharer_cols)
    bank_sub_2 = read_csv('yh_sub2_shareholder').selectExpr(*new_sharer_cols)
    dk_sub = read_csv('dk_sub_shareholder').selectExpr(*new_sharer_cols)

    his_sharer_cols = ['entname company','shareholder_name share_company',
                       '1 is_history',
                       'valid_datetime(entdate) start_time','0 end_time']
    all_com_his_sharer = read_csv('all_com_his_shareholder').selectExpr(*his_sharer_cols)
    dfs = [all_com_share,his_share_share,bank_share,bank_sub_3,bank_sub_2,all_com_his_sharer,dk_sub]
    all_sharers = reduce(lambda a,b:a.unionAll(b),dfs) \
            .where("verify_company_name(company)=1 and verify_company_name(share_company)=1").selectExpr(*share_cols) \
            .dropDuplicates(['company','share_company','is_history'])
    write_orc(all_sharers,add_save_path('edge_com_shareholder_com',root=save_root))


def company_law_judg_doc():
    judg_doc_cols = ['md5(concat(case_code,sentence_date)) doc_id','case_code','title','valid_datetime(sentence_date) sentence_date','trial_court','case_type',
                     'accuser','defendant','case_results','format_data(notice_content) notice_content']
    all_com_doc = read_csv('all_com_law_jud_doc')
    all_his_share_doc = read_csv('all_his_shareholder_law_jud_doc')
    all_his_sub2_doc = read_csv('all_his_sharer_sub_sharer_law_jud_doc')
    dfs = [all_com_doc,all_his_share_doc,all_his_sub2_doc]
    all_sharers = reduce(lambda a,b:a.unionAll(b),dfs).selectExpr(*judg_doc_cols).dropDuplicates(['doc_id'])
    write_orc(all_sharers,add_save_path('vertex_law_document',root=save_root))


def accuser_defendant_link_doc():

    def niv_cols(row):
        ret = []
        splits = row.data.split("_")
        doc_id = splits[0]
        start_time = splits[1]
        if len(splits) > 2:
            coms = udf_format_data_string(splits[2]).split(';')
            coms = filter(lambda x:udf_verify_company_name_int(x),coms)
            if coms:
                for com in coms:
                    ret.append([doc_id,com,start_time])
        if not ret:
            ret.append(['','',''])
        return ret

    docs = read_orc(add_save_path('vertex_law_docment',root=save_root))
    docs.persist()

    accuser_cols = ['doc_id','md5(format_com_name(accuser_company)) accuser_company',
                    'format_com_name(accuser_company) accuser_company_name','cast(start_time as bigint) start_time','0 end_time']
    docs.selectExpr('concat_ws("|",doc_id,sentence_date,accuser) data').show(truncate=False)
    accuser_rdd = docs.selectExpr('concat_ws("_",doc_id,sentence_date,accuser) data').rdd.flatMap(niv_cols)
    accuser_df = spark.createDataFrame(accuser_rdd,['doc_id','accuser_company','start_time']) \
        .selectExpr(*accuser_cols).where('doc_id != "" and accuser_company !="" ').dropDuplicates(['doc_id','accuser_company'])
    write_orc(accuser_df,add_save_path('edge_accusercom_link_doc',root=save_root))

    defendant_cols = ['doc_id','md5(format_com_name(defendant_company)) defendant_company',
                    'format_com_name(defendant_company) defendant_company_name','cast(start_time as bigint) start_time','0 end_time']

    accuser_rdd = docs.selectExpr('concat_ws("_",doc_id,sentence_date,defendant) data').rdd.flatMap(niv_cols)
    defendant_df = spark.createDataFrame(accuser_rdd,['doc_id','defendant_company','start_time']) \
        .selectExpr(*defendant_cols).where('doc_id != "" and defendant_company != "" ').dropDuplicates(['doc_id','defendant_company'])
    write_orc(defendant_df,add_save_path('edge_defendantcom_link_doc',root=save_root))


def accuser_link_defendant():
    #lawsuit
    accuser = read_orc(add_save_path('edge_accusercom_link_doc',root=save_root))
    defendant = read_orc(add_save_path('edge_defendantcom_link_doc',root=save_root)).drop('start_time')
    cols = ['accuser_company','defendant_company','accuser_company_name','defendant_company_name','start_time','0 end_time']
    res_tmp = accuser.join(defendant,'doc_id','full').selectExpr(*cols).where('accuser_company is not null and defendant_company is not null')
    res =res_tmp.dropDuplicates(['accuser_company','defendant_company'])
    write_orc(res,add_save_path('edge_com_lawsuit_com',root=save_root))

def com_xindai_com():
    cols = ['md5(format_com_name(start_company)) start_company','md5(format_com_name(end_company)) end_company',
            'format_com_name(start_company) start_company_name','format_com_name(end_company) end_company_name','valid_datetime(start_time) start_time','valid_datetime(end_time) end_time']
    all_xindai  = read_csv('xindai').selectExpr(*cols).dropDuplicates(['start_company','end_company'])
    write_orc(all_xindai,add_save_path('edge_com_loan_com',root=save_root))

def vertex_company():
    table_list = [
        {'tablename':'edge_com_shareholder_com','company':'company_name'},
        {'tablename':'edge_com_shareholder_com','company':'share_company_name'},
        {'tablename':'edge_accusercom_link_doc','company':'accuser_company_name'},
        {'tablename':'edge_defendantcom_link_doc','company':'defendant_company_name'},
        {'tablename':'edge_com_loan_com','company':'start_company_name'},
        {'tablename':'edge_com_loan_com','company':'end_company_name'},
    ]
    dfs = []
    for info in table_list:
        tablename = info['tablename']
        col_name = info['company']
        df = read_orc(add_save_path(tablename,root=save_root))
        res = df.selectExpr('%s company_name'%col_name)
        dfs.append(res)
    union_df = reduce(lambda a,b:a.unionAll(b),dfs).dropDuplicates()
    cols = ['md5(format_com_name(company_name)) company', 'format_com_name(company_name) dwmc', '"" dwdz', '"" zzjgdm', '"" tyshdm', '"" clrq', '"" jyzt', '"" jyfw',
                                       '"" dwlx', '"" zczb', '0 cyrs', '"" hylb', '"" dwscdz', '"" bgxq', '"" bgrq','"wa_data" tablename',"4 table_sort"]

    v_company = union_df.selectExpr(*cols).where('verify_company_name(company_name)=1').dropDuplicates(['company'])
    write_orc(v_company,add_save_path('vertex_company_case',root=save_root))
    write_csv(v_company,add_save_path('call_com_name',root=save_root))

def add_save_jg_path(tablename,standby=False):
    ## jg文件保存地址
    if standby:
        return '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_data_standby/{}'.format(tablename.lower()+"_jg")
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_data/{}'.format(tablename.lower()+"_jg")

def jg_company():
    cols = ['jid','company', 'dwmc', 'dwdz', 'zzjgdm', 'tyshdm', 'clrq', 'jyzt', 'jyfw',
                                       'dwlx', 'zczb', 'cyrs', 'hylb', 'dwscdz', 'bgxq', 'bgrq','1 table_sort']
    cols2 = ['row_number() over (order by company) jid','company', 'dwmc', 'dwdz', 'zzjgdm', 'tyshdm', 'clrq', 'jyzt', 'jyfw',
                                       'dwlx', 'zczb', 'cyrs', 'hylb', 'dwscdz', 'bgxq', 'bgrq','2 table_sort']
    cols3 = ['(jid+max_jid) jid','company', 'dwmc', 'dwdz', 'zzjgdm', 'tyshdm', 'clrq', 'jyzt', 'jyfw',
                                       'dwlx', 'zczb', 'cyrs', 'hylb', 'dwscdz', 'bgxq', 'bgrq','2 table_sort']
    com_jg = read_orc(add_save_jg_path('vertex_company')).selectExpr(*cols)
    max_jid = com_jg.selectExpr('max(jid)').collect()[0][0]
    com = read_orc(add_save_path('vertex_company_case',root=save_root)).selectExpr(*cols2)
    com = com.withColumn('max_jid',lit(max_jid))
    com = com.selectExpr(*cols3)

    all_com = com_jg.unionAll(com).selectExpr('*','row_number() over (partition by company order by table_sort asc) num') \
            .where('num=1').drop('num').drop('table_sort')
    write_orc(all_com,add_save_jg_path('vertex_company_case'))



if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # company_shareholder()
    # company_law_judg_doc()
    # accuser_defendant_link_doc()
    # accuser_link_defendant()
    # com_xindai_com()
    vertex_company()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))