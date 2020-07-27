# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : qq_msg.py
# @Software: PyCharm
# @content : qq相关信息

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
conf=SparkConf().set('spark.driver.maxResultSize', '20g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '20g')
conf.set('spark.executor.instances', 40)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')
#conf.set("spark.sql.warehouse.dir", warehouse_location)


spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/qq'
root_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/touyou_case/'

from common import *
commonUtil = CommonUdf(spark)

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

def write_csv(df, path,mode='overwrite'):
    df.write.mode(mode).csv(path=path, sep='\t', header=None)


def add_save_path(tablename, cp='', root='extenddir'):
    '''
    保存hive外表文件，分区默认为cp=2020，root为extenddir
    :param tablename:
    :param cp:
    :return:
    '''
    if cp:
        tmp_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/{}/{}/cp={}'
        return tmp_path.format(root, tablename.lower(), cp)
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/{}/{}/cp=2020'.format(root, tablename.lower())

def read_orc(path):
    try:
        df = spark.read.orc(path)
        if df.take(1):
            return df
    except:
        return None

def read_csv(file):
    path = '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/company_case/'+file
    try:
        df = spark.read.csv(path,header=True,sep='\t')
        if df.take(1):
            return df
    except:
        return None

def deal_common_relate(res):
    ret = []
    relate_sfzh,sfzhs = res
    if len(sfzhs)>1:
        try:
            sfzhs = sorted(sfzhs)
            for index,value in enumerate(sfzhs):
                for i in range(index+1,len(sfzhs)):
                    ret.append((value,sfzhs[i],relate_sfzh))
        except:
            pass
    return ret

save_root = 'relation_theme_extenddir'

def get_groupcall_info():
    sfzh1 = read_csv('/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/csv/company_info').selectExpr('_c0 sfzh').dropDuplicates()
    sm_company = read_orc(add_save_path('vertex_company')).where('dwmc like "%商贸%" or dwmc like "%经贸%" or dwmc like "%贸易%" or dwmc like "%科贸%"')
    legal = read_orc(add_save_path('edge_person_legal_com')).selectExpr('start_person','end_company')
    sm_info = sm_company.join(legal,sm_company['company']==legal['end_company'],'inner').selectExpr('start_person sfzh','end_company company').dropDuplicates()
    write_orc(sm_info,add_save_path('sm_info',root='wa_data'))
    # sfzh2 = read_csv('/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/csv/sm_company_info').selectExpr('_c0 sfzh').dropDuplicates()
    sfzh2 = sm_info.selectExpr('sfzh').dropDuplicates()
    # sfzhs = sfzh1.union(sfzh2)
    call = read_orc(add_save_path('edge_person_groupcall_detail',cp='*',root='person_relation_detail'))
    sfzh1.createOrReplaceTempView('sfzh1')
    sfzh2.createOrReplaceTempView('sfzh2')
    call.createOrReplaceTempView('call')

    sql1 = '''
        select a.* from call a
        inner join sfzh1 b
        inner join sfzh2 c
        on a.start_person = b.sfzh and a.end_person = c.sfzh
    '''

    res1 = spark.sql(sql1)

    sql2 = '''
        select a.* from call a
        inner join sfzh1 b
        inner join sfzh2 c
        on a.start_person = c.sfzh and a.end_person = b.sfzh
    '''
    res2 = spark.sql(sql2)
    res= res1.unionAll(res2)
    write_orc(res,add_save_path('call_info',root='wa_data'))

    ###任职
def sm_work():
    work = read_orc(add_save_path('edge_person_work_com',root=save_root))
    sm_info = read_orc(add_save_path('sm_info',root='wa_data'))
    cols = ['start_perosn','end_company','company_name','start_time','end_time']
    # sm_work = work.join(sm_info,work['end_company']==sm_info['company'],'inner').selectExpr(*cols)
    # write_orc(sm_work,add_save_path('sm_work',root='wa_data'))
    sm_work = read_orc(add_save_path('sm_work',root='wa_data'))
    df = sm_work.groupby(['end_company','company_name']).agg(count('start_perosn').alias('num'))
    df = df.where('num < 10').withColumnRenamed('end_company','company').drop('company_name')
    # cols = ['start_person', 'end_company', 'start_time', 'end_time', 'tablename']
    legal = read_orc(add_save_path('edge_person_legal_com')).selectExpr('start_person','end_company')
    # sm_legal = df.join(legal,df[''])
    sm_work = work.join(df,work['end_company']==df['company'],'inner').selectExpr(*cols)
    sm_workers = sm_work.selectExpr('start_perosn sfzh').dropDuplicates()

    all_work = work.join(sm_workers,work['start_perosn']==sm_workers['sfzh'],'inner').selectExpr(*cols)
    com = read_orc(add_save_path('vertex_company'))
    ## 6000家公司 所有任职信息
    # all_work = all_work.join(com,all_work['end_company']==com['company'],'inner').selectExpr(*(cols+['dwmc']))
    # res = all_work.where('start_time > 1483200000').groupby
    all_work.createOrReplaceTempView('tmp')

    sql = '''
        select start_perosn,collect_set(end_company) company from tmp group by start_perosn
    '''
    res = spark.sql(sql).where('list_length(company) > 1')
    tmp = spark.createDataFrame(res.rdd.flatMap(deal_common_relate), ['com1', 'com2','sfzh'])
    # write_orc(res,add_save_path('sm_work_all',root='wa_data'))
    # write_orc(tmp,add_save_path('sm_work_all_com',root='wa_data'))
    coms = tmp.selectExpr('com1 com').union(tmp.selectExpr('com2 com'))
    sm_coms = df.selectExpr('company com').dropDuplicates()
    res_coms = coms.intersect(sm_coms)
    cols = ['company', 'dwmc', 'dwdz', 'zzjgdm', 'tyshdm', 'clrq', 'jyzt', 'jyfw',
                                       'dwlx', 'zczb', 'cyrs', 'hylb', 'dwscdz', 'bgxq', 'bgrq']
    res_company = res_coms.join(com,com['company']==res_coms['com'],'inner').selectExpr(*cols)
    write_orc(res_company,add_save_path('sm_special_com',root='wa_data'))



def sm_legal():
    work = read_orc(add_save_path('edge_person_work_com',root=save_root))
    sm_work = read_orc(add_save_path('sm_work',root='wa_data'))
    df = sm_work.groupby(['end_company','company_name']).agg(count('start_perosn').alias('num'))
    df = df.where('num < 10').withColumnRenamed('end_company','company').drop('company_name')
    cols = ['start_person', 'end_company', 'start_time', 'end_time']
    cols2 = ['start_perosn', 'end_company', 'start_time', 'end_time']
    legal = read_orc(add_save_path('edge_person_legal_com')).selectExpr('start_person','end_company','start_time','end_time')
    # sm_legal = df.join(legal,df[''])
    sm_work = legal.join(df,legal['end_company']==df['company'],'inner').selectExpr(*cols)
    sm_legals = sm_work.selectExpr('start_person sfzh').dropDuplicates()

    all_work = work.join(sm_legals,work['start_perosn']==sm_legals['sfzh'],'inner').selectExpr(*cols2)
    com = read_orc(add_save_path('vertex_company'))
    ## 6000家公司 所有任职信息
    # all_work = all_work.join(com,all_work['end_company']==com['company'],'inner').selectExpr(*(cols+['dwmc']))
    # res = all_work.where('start_time > 1483200000').groupby
    all_work.createOrReplaceTempView('tmp')

    sql = '''
        select start_perosn,collect_set(end_company) company from tmp group by start_perosn
    '''
    res = spark.sql(sql).where('list_length(company) > 1')
    tmp = spark.createDataFrame(res.rdd.flatMap(deal_common_relate), ['com1', 'com2','sfzh'])
    # write_orc(res,add_save_path('sm_work_all',root='wa_data'))
    # write_orc(tmp,add_save_path('sm_work_all_com',root='wa_data'))
    coms = tmp.selectExpr('com1 com').union(tmp.selectExpr('com2 com'))
    sm_coms = df.selectExpr('company com').dropDuplicates()
    res_coms = coms.intersect(sm_coms)
    cols = ['company', 'dwmc', 'dwdz', 'zzjgdm', 'tyshdm', 'clrq', 'jyzt', 'jyfw',
                                       'dwlx', 'zczb', 'cyrs', 'hylb', 'dwscdz', 'bgxq', 'bgrq']
    res_company = res_coms.join(com,com['company']==res_coms['com'],'inner').selectExpr(*cols)
    res_company = res_company.dropDuplicates(['company'])
    write_orc(res_company,add_save_path('sm_special_legal_com',root='wa_data'))



def com_info():
    # source_sfzh = read_csv('/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/csv/company_info').selectExpr('_c0 sfzh').dropDuplicates()
    call = read_orc(add_save_path('call_info',root='wa_data'))
    sfzh1 = call.selectExpr('start_person sfzh')
    sfzh2 = call.selectExpr('end_person sfzh')

    sfzhs = sfzh1.union(sfzh2).dropDuplicates()
    sm_info = read_orc(add_save_path('sm_info',root='wa_data'))
    sm_info = sm_info.join(sfzhs,'sfzh','inner').select(sm_info.company.alias('companyid'))
    company = read_orc(add_save_path('vertex_company'))
    cols = ['company', 'dwmc', 'dwdz', 'zzjgdm', 'tyshdm', 'clrq', 'jyzt', 'jyfw',
                                       'dwlx', 'zczb', 'cyrs', 'hylb', 'dwscdz', 'bgxq', 'bgrq']
    res = company.join(sm_info,company['company']==sm_info['companyid'],'inner').selectExpr(*cols)
    write_csv(res,add_save_path('call_sm_company_info_csv',root='wa_data'))
    write_orc(res,add_save_path('call_sm_company_info_orc',root='wa_data'))


def sm_legal_271():
    com_271 = read_orc(add_save_path('call_sm_company_info_orc',root='wa_data'))
    cols = ['start_person','end_company','start_time','end_time']
    cols2 = ['start_perosn start_person','end_company','start_time','end_time']
    legal = read_orc(add_save_path('edge_person_legal_com')).selectExpr(*cols)
    legal_271 = com_271.join(legal,com_271['company']==legal['end_company'],'inner').selectExpr('start_person sfzh').dropDuplicates()
    work = read_orc(add_save_path('edge_person_work_com',root=save_root)).selectExpr(*cols2)

    all = legal.union(work)

    res = all.join(legal_271,all['start_person']==legal_271['sfzh'],'inner').selectExpr(*cols)

    com = read_orc(add_save_path('vertex_company'))
    ## 6000家公司 所有任职信息
    # all_work = all_work.join(com,all_work['end_company']==com['company'],'inner').selectExpr(*(cols+['dwmc']))
    # res = all_work.where('start_time > 1483200000').groupby
    res.createOrReplaceTempView('tmp')

    sql = '''
        select start_person,collect_set(end_company) company from tmp group by start_person
    '''
    res = spark.sql(sql).where('list_length(company) > 1')
    tmp = spark.createDataFrame(res.rdd.flatMap(deal_common_relate), ['com1', 'com2','sfzh'])
    # write_orc(res,add_save_path('sm_work_all',root='wa_data'))
    # write_orc(tmp,add_save_path('sm_work_all_com',root='wa_data'))
    coms = tmp.selectExpr('com1 com').union(tmp.selectExpr('com2 com'))
    cols = ['company', 'dwmc', 'dwdz', 'zzjgdm', 'tyshdm', 'clrq', 'jyzt', 'jyfw',
                                       'dwlx', 'zczb', 'cyrs', 'hylb', 'dwscdz', 'bgxq', 'bgrq']
    res_company = coms.join(com,com['company']==coms['com'],'inner').selectExpr(*cols)
    res_company = res_company.dropDuplicates(['company'])
    write_orc(res_company,add_save_path('sm_special_legal_com_271',root='wa_data'))
    write_csv(res_company,add_save_path('sm_special_legal_com_271_csv',root='wa_data'))

def get_all_legal():
    df1 = read_orc(add_save_path('sm_special_legal_com',root='wa_data'))
    df2 = read_orc(add_save_path('sm_special_legal_com_271',root='wa_data'))
    df = df1.unionAll(df2).dropDuplicates(['company'])
    cols = ['start_person','end_company','start_time','end_time']
    legal = read_orc(add_save_path('edge_person_legal_com')).selectExpr(*cols)
    sm_legal = legal.join(df,legal['end_company']==df['company'],'inner').selectExpr('start_person').dropDuplicates()
    p_cols = ['zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                                      'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz']
    person = read_orc(add_save_path('vertex_person'))
    res = person.join(sm_legal,person['zjhm']==sm_legal['start_person'],'inner') \
            .selectExpr(*p_cols).dropDuplicates(['zjhm'])
    write_csv(res,add_save_path('all_legal_info',root='wa_data'))

def deep_find_com():
    #   all_loans_com.csv 贷款公司
    #   all_shareholder_info.csv 股东人或股东公司信息
    #   legal_defendant.csv 文书信息
    defend = read_csv('legal_defendant.csv')
    shareholder = read_csv('all_shareholder_info.csv')
    sm_271 = read_csv('sm_special_legal_com_271.csv')
    loan_com = read_csv('all_loans_com.csv')
    all_com = read_orc(add_save_path('vertex_company',root=save_root))
    sm_com = all_com.where('dwmc like "%商贸%" or dwmc like "%贸易%" or dwmc like "%经贸%" or dwmc like "%科贸%" ')
    all_person = read_orc(add_save_path('vertex_person',root=save_root))
    legal = read_orc(add_save_path('edge_person_legal_com',root=save_root))
    defend.createOrReplaceTempView('defend')
    loan_com.createOrReplaceTempView('loan')
    all_com.createOrReplaceTempView('all_com')
    sm_com.createOrReplaceTempView('sm_com')

    ## 诉讼贷款交集
    sql = ''' select b.*,a.* from loan a inner join defend b on b.defendant like concat('%',a.com,'%') '''
    loan_in_law = spark.sql(sql)
    # write_csv(loan_in_law,add_save_path('loan_in_law',root='wa_data/company_case'))

    ##诉讼商贸公司
    sql = ''' select b.*,a.company,a.dwmc as com from sm_com a inner join defend b on b.defendant like concat('%',format_data(a.dwmc),'%') '''
    law_sm_in_all_com = spark.sql(sql)
    # write_csv(law_sm_in_all_com,add_save_path('law_sm_in_all_com',root='wa_data/company_case'))

    ## 同诉讼商贸公司  贷款公司
    cols = ['case_code','accuser','defendant','com']
    union_law = loan_in_law.selectExpr(*cols).unionAll(law_sm_in_all_com.selectExpr(*cols))
    union_law.createOrReplaceTempView('union_law')
    sql = ''' select case_code, collect_set(com) coms from union_law group by  case_code'''
    same_law = spark.createDataFrame(spark.sql(sql).rdd.flatMap(deal_common_relate),['com1','com2','case_code'])
    # write_csv(same_law,add_save_path('same_law',root='wa_data/company_case'))

    ##诉讼商贸公司法人
    law_sm_in_all_com_legal = legal.join(law_sm_in_all_com,legal.end_company==law_sm_in_all_com.company,'inner') \
        .selectExpr('start_perosn start_person','end_company','company_name')
    # write_csv(law_sm_in_all_com_legal,add_save_path('law_sm_in_all_com_legal',root='wa_data/company_case'))

    ##贷款公司法人
    loan_legal= legal.join(loan_com,legal.company_name==loan_com.com,'inner') \
        .selectExpr('start_perosn start_person','end_company','company_name')
    write_csv(loan_legal,add_save_path('loan_legal',root='wa_data/company_case'))
    p_cols = ['zjhm', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg','whcd', 'hyzk', 'hkszdxz', 'sjjzxz']
    load_legal_detail= loan_legal.join(all_person,loan_legal.start_person==all_person.zjhm,'inner') \
            .selectExpr(*p_cols)
    write_csv(load_legal_detail,add_save_path('loan_legal_detail',root='wa_data/company_case'))

    ## 诉讼商贸公司法人与贷款公司法人通联
    call = read_orc(add_save_path('edge_person_groupcall_detail',cp='*',root='person_relation_detail'))
    law_sm_in_all_com_legal.selectExpr('start_person sfzh','company_name').createOrReplaceTempView('sfzh1')
    loan_legal.selectExpr('start_person sfzh','company_name').createOrReplaceTempView('sfzh2')
    call.createOrReplaceTempView('call')

    sql1 = '''
        select a.start_person, a.end_person,b.company_name as start_com, c.company_name as end_com, '1' cp from call a
        inner join sfzh1 b
        inner join sfzh2 c
        on a.start_person = b.sfzh and a.end_person = c.sfzh
    '''

    res1 = spark.sql(sql1)

    sql2 = '''
        select a.start_person, a.end_person,c.company_name as start_com, b.company_name as end_com ,'1' cp from call a
        inner join sfzh1 b
        inner join sfzh2 c
        on a.start_person = c.sfzh and a.end_person = b.sfzh
    '''
    res2 = spark.sql(sql2)
    law_loan_legal_call_tmp = res1.unionAll(res2)
    law_loan_legal_call = law_loan_legal_call_tmp.groupby(['start_person','end_person','start_com','end_com']).agg(count("cp").alias('num'))
    write_csv(law_loan_legal_call,add_save_path('law_loan_legal_call',root='wa_data/company_case'))



if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # get_groupcall_info()
    # com_info()
    # sm_legal()
    # sm_legal_271()
    # get_all_legal()
    deep_find_com()


    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))