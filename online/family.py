# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 18:37
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : family.py
# @Software: PyCharm
import sys, os
import math
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
conf.set('spark.yarn.executor.memoryOverhead', '10g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '20g')
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
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/family'

def deal_common_relate(res):
    ret = []
    relate_sfzh,sfzhs = res
    if len(sfzhs)>1:
        try:
            for index,value in enumerate(sfzhs):
                for i in range(index+1,len(sfzhs)):
                    ret.append((value,sfzhs[i],relate_sfzh))
        except:
            pass
    return ret

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


def get_spouse_from_comm():

    spouse_schema = [
        {'tablename':'ods_gov_edu_stud_info','sfzhnan':'father_cert_no','sfzhnv':'mother_cert_no','hqsj':'birth_date','table_sort':2},
        {'tablename':'ods_gov_nca_bircer_info','sfzhnan':'father_cert_no','sfzhnv':'mother_cert_no','hqsj':'sign_date','table_sort':2},
    ]

    tmp_sql = '''  select 
                case when verify_man({sfzhnan})=1 then format_zjhm({sfzhnan}) else format_zjhm({sfzhnv}) end sfzhnan,
                case when verify_man({sfzhnan})=1 then format_zjhm({sfzhnv}) else format_zjhm({sfzhnan}) end sfzhnv,
                '0' as  start_time,'0' as end_time,date2timestampstr({hqsj}) as hqsj, '{tablename}' tablename,{table_sort} table_sort
                from {tablename}
                where 
                verify_sfz({sfzhnan})=1 and verify_sfz({sfzhnv})=1 and upper(trim({sfzhnan}))!= upper(trim({sfzhnv}))
                '''
    dfs = []
    for item in spouse_schema:
        tablename = item['tablename']
        init(spark,tablename,if_write=False)
        sql = tmp_sql.format(**item)
        df = spark.sql(sql)
        dfs.append(df)

    common_spouse = reduce(lambda x,y:x.unionAll(y),dfs)
    common_spouse.createOrReplaceTempView('tmp')
    sql = '''select sfzhnan,sfzhnv,start_time,end_time, hqsj, tablename,row_number() over(partition by sfzhnan,sfzhnv order by table_sort asc) num from tmp'''
    df = spark.sql(sql).where('num=1').drop('num')

    write_parquet(df,'spouse/common')
    logger.info('common success')

def get_spouse_from_hydj():
    ## 婚姻登记处获得的夫妻数据
    tablename = 'ods_gov_mca_civadm_marreg'
    init(spark,tablename,if_write=False)
    sql_jiehun = ''' select format_zjhm(man_cert_no) sfzhnan,format_zjhm(woman_cert_no) sfzhnv,
            date2timestampstr(reg_date) AS start_time,
            '0' as end_time, date2timestampstr(reg_date) hqsj,
            'ods_gov_mca_civadm_marreg' tablename
            from ods_gov_mca_civadm_marreg  WHERE 
            verify_sfz(man_cert_no)=1 and verify_sfz(woman_cert_no)=1 
            and (marr_regbus_clas_name like '%结婚%' OR marr_regbus_clas_name like '%新婚%')
            and upper(trim(man_cert_no))!=upper(trim(woman_cert_no))
           '''
    tmp = spark.sql(sql_jiehun)
    tmp.createOrReplaceTempView('tmp')
    df = spark.sql('''select sfzhnan,sfzhnv,min(if(start_time!='0',start_time,null)) start_time,end_time, max(hqsj) hqsj, tablename 
            from tmp group by sfzhnan,sfzhnv,end_time,tablename ''')
    write_parquet(df,'spouse/jiehun_from_hydj')

    sql_lihun = ''' select format_zjhm(man_cert_no) sfzhnan,format_zjhm(woman_cert_no) sfzhnv,
                date2timestampstr(reg_date) AS end_time,
                '0' as start_time, date2timestampstr(reg_date) hqsj,
                'ods_gov_mca_civadm_marreg' tablename
                from ods_gov_mca_civadm_marreg  WHERE 
                verify_sfz(man_cert_no)=1 and verify_sfz(woman_cert_no)=1 
                and (marr_regbus_clas_name like '%离婚%')
                and upper(trim(man_cert_no))!=upper(trim(woman_cert_no))
               '''

    tmp = spark.sql(sql_lihun)
    tmp.createOrReplaceTempView('tmp')
    df = spark.sql('''select SFZHNAN,SFZHNV,START_TIME,max(END_TIME) END_TIME, max(hqsj) hqsj,tablename 
            from tmp group by SFZHNAN,SFZHNV,START_TIME,tablename ''')
    write_parquet(df, "spouse/lihun_from_hydj")
    logger.info('hydj success')

def build_edge_spouse():
    '''构建配偶'''
    df1 = read_parquet("spouse/common")
    df2 = read_parquet("spouse/jiehun_from_hydj")
    df3 = read_parquet("spouse/lihun_from_hydj")
    all_df = df1.unionAll(df2).unionAll(df3)
    all_df.createOrReplaceTempView('tmp')
    df2 = spark.sql('''select sfzhnan,sfzhnv,min(cast(if(start_time!='0',start_time,null) as bigint)) start_time,
                        max(cast(end_time as bigint)) end_time, max(cast(hqsj as bigint)) hqsj, min(tablename) tablename from tmp group by sfzhnan,sfzhnv''')

    df3 = df2.na.fill({'start_time': 0, 'end_time': 0, 'hqsj':0})
    save_path = add_save_path('edge_person_spouse_person')
    write_orc(df3,save_path)
    logger.info(' build_edge_spouse down')

def get_parents():
    '''
    1、ODS_GOV_NCA_BIRCER_INFO 出生证信息
        MOTHER_CERT_NO	母亲_公民身份号码
        FATHER_CERT_NO	父亲_公民身份号码
        CERT_NO	公民身份号码
        BIRTH_DATE	出生日期
    2、ODS_GOV_EDU_STUD_INFO	大中小学生信息
        FATHER_NAME	父亲_姓名
        FATHER_CERT_NO	父亲_公民身份号码
        FATHER_CTCT_TEL	父亲_联系电话
        MOTHER_NAME	母亲_姓名
        MOTHER_CERT_NO	母亲_公民身份号码
        MOTHER_CTCT_TEL	母亲_联系电话
    :return:
    '''
    parent_schema = [
        {'tablename': 'ods_gov_edu_stud_info', 'sfzh': 'cred_num', 'fqsfzh': 'father_cert_no',
         'mqsfzh': 'mother_cert_no', 'table_sort': 2},
        {'tablename': 'ods_gov_nca_bircer_info', 'sfzh': 'cert_no', 'fqsfzh': 'father_cert_no',
         'mqsfzh': 'mother_cert_no', 'table_sort': 1},
    ]

    fq_sql = '''  select format_zjhm({sfzh}) sfzh,format_zjhm({fqsfzh}) fqsfzh,
            format_starttime_from_sfzh({sfzh}) start_time,'0' end_time,'{tablename}'  tablename,{table_sort} table_sort
            from {tablename}   WHERE 
            verify_sfz({sfzh})=1 and verify_sfz({fqsfzh})=1 and verify_man({fqsfzh})=1
            '''

    mq_sql = '''  select format_zjhm({sfzh}) sfzh,format_zjhm({mqsfzh}) mqsfzh,
            format_starttime_from_sfzh({sfzh}) start_time,'0' end_time,'{tablename}'  tablename,{table_sort} table_sort
            from {tablename}  WHERE 
            verify_sfz({sfzh})=1 and verify_sfz({mqsfzh})=1 and verify_man({mqsfzh})=0
            '''
    fq_df = []
    mq_df = []
    for info in parent_schema:
        logger.info(info['tablename'])
        init(spark,info["tablename"],if_write=False)
        sql1 = fq_sql.format(**info)
        sql2 = mq_sql.format(**info)
        tmp_df1 = spark.sql(sql1)
        tmp_df2 = spark.sql(sql2)
        fq_df.append(tmp_df1)
        mq_df.append(tmp_df2)

    df1 = reduce(lambda x,y:x.unionAll(y),fq_df)
    df2 = reduce(lambda x,y:x.unionAll(y),mq_df)

    df1.createOrReplaceTempView('tmp')
    sql = '''select sfzh,fqsfzh,cast(start_time as bigint) start_time,cast(end_time as bigint) end_time,tablename,
                row_number() over(partition by sfzh,fqsfzh order by table_sort asc) num from tmp
                where substr(sfzh,7,4) > substr(fqsfzh,7,4)  '''
    tmp = spark.sql(sql).where('num=1').drop('num')
    res = tmp.na.fill({'start_time': 0, 'end_time': 0})
    write_orc(res,add_save_path('edge_person_fatheris_person'))
    logger.info(' edge_person_fatherIs_person down')

    df2.createOrReplaceTempView('tmp')
    sql = '''select sfzh,mqsfzh,cast(start_time as bigint) start_time,cast(end_time as bigint) end_time,tablename,
                    row_number() over(partition by sfzh,mqsfzh order by table_sort asc) num from tmp
                    where  substr(sfzh,7,4) > substr(mqsfzh,7,4) '''
    tmp = spark.sql(sql).where('num=1').drop('num')
    res = tmp.na.fill({'start_time': 0, 'end_time': 0})
    write_orc(res, add_save_path('edge_person_motheris_person'))
    logger.info(' edge_person_motherIs_person down')

def get_guardian():
    '''
    1、ODS_GOV_EDU_KINDERGARTEN_STU	幼儿园学生信息
        CERT_NO	公民身份号码
        START_TIME format_starttime_from_sfzh(CERT_NO)
        GUAR1_CERT_NO	监护人一_公民身份号码
        GUAR2_CERT_NO	监护人二_公民身份号码
    2、ODS_GOV_EDU_STUD_INFO	大中小学生信息
        CERT_NO	公民身份号码
        CRED_NUM	证件号码
        GUAR_NAME	监护人_姓名
        GUAR_CERT_NO	监护人_公民身份号码
        GUAR_CTCT_TEL	监护人_联系电话

    :return:
    '''
    guard_schema = [
        {'tablename': 'ods_gov_edu_kindergarten_stu', 'sfzh': 'cert_no', 'jhrsfzh': 'guar1_cert_no', 'table_sort': 1},
        {'tablename': 'ods_gov_edu_kindergarten_stu', 'sfzh': 'cert_no', 'jhrsfzh': 'guar2_cert_no', 'table_sort': 1},
        {'tablename': 'ods_gov_edu_stud_info', 'sfzh': 'cred_num', 'jhrsfzh': 'guar_cert_no', 'table_sort': 2},
    ]
    ## 初始化幼儿园
    # init(spark,'ods_gov_edu_kindergarten_stu')

    tmp_sql = '''  select format_zjhm({sfzh}) sfzh, format_zjhm({jhrsfzh}) jhrsfzh,
            format_starttime_from_sfzh({sfzh}) start_time ,'0' end_time,'{tablename}'  tablename,{table_sort} table_sort
            from {tablename}  WHERE   
            verify_sfz({sfzh})=1 and verify_sfz({jhrsfzh})=1  and  substr({sfzh},7,4) > substr({jhrsfzh},7,4)  '''
    df = None
    for info in guard_schema:
        tablename = info['tablename']
        sql = tmp_sql.format(**info)
        init(spark,tablename,if_write=False)
        tmp_df = spark.sql(sql)
        if not df:
            df = tmp_df
        else:
            df = df.unionAll(tmp_df)
    df.createOrReplaceTempView('tmp')
    sql = '''select sfzh,jhrsfzh,cast(start_time as bigint) start_time,cast(end_time as bigint) end_time,tablename,
            row_number() over(partition by sfzh,jhrsfzh order by table_sort asc) num from tmp'''
    df2 = spark.sql(sql).where('num=1').drop('num')
    res = df2.na.fill({'start_time': 0, 'end_time': 0})
    write_orc(res,add_save_path('edge_person_guardianis_person'))
    logger.info(' edge_person_guardianIs_person down')

def get_hukou_info():
    # +------------+------------+--+
    # | hou_tcode | hou_tname |
    # +------------+------------+--+
    # | | |
    # | 10 | 非农业家庭户口 |
    # | 30 | 非农业集体户口 |
    # | 50 | 自理口粮户口 |
    # +------------+------------+--+
    hukou_schema = {'table_name':'tb_gaw_psmis_t_rk_jbxx','sfzh':'SFZHM','hkszddzid':'HKSZDDZID','hkszdhh':'HKSZDHH',
                    'yhzgj':'YHZGJ','flrq':'""','isfl':0},

    pass
def find_parent():
    '''
    通过配偶和父母关系推理
    :return:
    '''
    mother = read_orc(add_save_path('edge_person_motheris_person')).select('sfzh','mqsfzh')
    father = read_orc(add_save_path('edge_person_fatheris_person')).select('sfzh','fqsfzh')

    create_tmpview_table(spark,'edge_person_spouse_person')
    sql3 = '''select sfzhnv,max(sfzhnan) sfzhnan,count(sfzhnan) as num 
                from edge_person_spouse_person group by  sfzhnv '''
    # 母亲只有一个配偶的数据
    motherHasOneFather = spark.sql(sql3).where('num=1')
    # 缺失父亲的数据
    tmp = mother.join(father, 'sfzh', 'left').drop(father.sfzh)
    lose_father = tmp.where('fqsfzh is null')
    # 获取到父亲
    find_father = lose_father.join(motherHasOneFather, lose_father.mqsfzh == motherHasOneFather.sfzhnv, 'inner') \
        .select(lose_father.sfzh, motherHasOneFather.sfzhnan.alias('fqsfzh')).dropDuplicates()
    find_father = find_father.selectExpr('sfzh','fqsfzh','format_starttime_from_sfzh(sfzh) start_time','0 end_time')
    write_orc(find_father, add_save_path('edge_find_father'))
    logger.info('edge_find_father down')

    # 找母亲 A以父亲关联B，B仅以一条配偶关系关联C
    # 父亲只有一个配偶的数据
    create_tmpview_table(spark,'edge_person_spouse_person')
    sql4 = '''select  sfzhnan,max(sfzhnv) sfzhnv,count(sfzhnv) as num  
                from edge_person_spouse_person group by  sfzhnan '''
    faherHasOneMother = spark.sql(sql4).where('num=1')

    tmp = father.join(mother, 'sfzh', 'left').drop(mother.sfzh)
    lose_mother = tmp.where('mqsfzh is null')
    # 获取到母亲
    find_mother = lose_mother.join(faherHasOneMother, lose_mother.fqsfzh == faherHasOneMother.sfzhnan, 'inner') \
        .select(lose_mother.sfzh, faherHasOneMother.sfzhnv.alias('mqsfzh')).dropDuplicates()

    find_mother = find_mother.selectExpr('sfzh','mqsfzh','format_starttime_from_sfzh(sfzh) start_time','0 end_time')

    write_orc(find_mother,add_save_path('edge_find_mother'))
    logger.info('edge_find_mother down')


def find_spouse():
    # 补全配偶 A以一条父亲关系关联B且同时以一条母亲关联C
    mother = read_orc(add_save_path('edge_person_motheris_person')).select('sfzh', 'mqsfzh')
    father = read_orc(add_save_path('edge_person_fatheris_person')).select('sfzh', 'fqsfzh')

    # 一个人关联的父亲母亲
    df = mother.join(father, 'sfzh', 'inner').select(mother.sfzh, mother.mqsfzh, father.fqsfzh)
    create_tmpview_table(spark,'edge_person_spouse_person')
    spouse = spark.sql('''select  sfzhnan, sfzhnv from edge_person_spouse_person ''')
    # 关联父亲的原有配偶
    df1 = df.join(spouse, df.fqsfzh == spouse.sfzhnan, 'left') \
            .select(df.fqsfzh,df.mqsfzh,spouse.sfzhnv)

    # 关联母亲的的原有配偶
    create_tmpview_table(spark,'edge_person_spouse_person')
    spouse2 = spark.sql('''select  sfzhnan, sfzhnv from edge_person_spouse_person ''')
    df2 = df1.join(spouse2,df1.mqsfzh==spouse2.sfzhnv,'left') \
        .select(df1.fqsfzh.alias('sfzhnan'),df1.mqsfzh.alias('sfzhnv'),df1.sfzhnv.alias('sfzh1'),spouse2.sfzhnan.alias('sfzh2'))
    # 2个关联 的原有配偶 都为空时 判断为配偶关系
    find_spouse = df2.where('sfzh1 is null and sfzh2 is null').drop('sfzh1').drop('sfzh2')
    write_orc(find_spouse,add_save_path('edge_find_spouse'))
    logger.info('edge_find_spouse down')

def find_common_parents():
    '''
    推理同一个父亲，同一个母亲
    :return:
    '''
    mother = read_orc(add_save_path('edge_person_motheris_person')).select('sfzh','mqsfzh')
    father = read_orc(add_save_path('edge_person_fatheris_person')).select('sfzh','fqsfzh')

    find_mohter = read_orc(add_save_path('edge_find_mother')).select('sfzh','mqsfzh')
    find_father = read_orc(add_save_path('edge_find_father')).select('sfzh','fqsfzh')

    father_all = father.unionAll(find_father)
    father_all.createOrReplaceTempView('tmp')
    # 同父 需要加上 找到的父亲母亲
    suns = spark.sql('''select fqsfzh,collect_set(sfzh) as  sfzhs from tmp group by  fqsfzh''')
    df = spark.createDataFrame(suns.rdd.flatMap(deal_common_relate), ['sfzh1', 'sfzh2', 'fqsfzh'])
    common_father = df.select(when(df.sfzh1 > df.sfzh2, df.sfzh1).otherwise(df.sfzh2).alias('sfzh1'), \
                              when(df.sfzh1 > df.sfzh2, df.sfzh2).otherwise(df.sfzh1).alias('sfzh2')).dropDuplicates()
    common_father = common_father \
            .selectExpr('sfzh1','sfzh2','format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time')
    write_orc(common_father, add_save_path('edge_find_common_father'))
    logger.info('edge_find_common_father down')

    # 同母 需要加上 找到的父亲母亲
    mother_all = mother.unionAll(find_mohter)
    mother_all.createOrReplaceTempView('tmp')
    suns = spark.sql('''select mqsfzh,collect_set(sfzh) as sfzhs from tmp group by  mqsfzh''')

    df = spark.createDataFrame(suns.rdd.flatMap(deal_common_relate), ['sfzh1', 'sfzh2', 'mqsfzh'])
    common_mother = df.select(when(df.sfzh1 > df.sfzh2, df.sfzh1).otherwise(df.sfzh2).alias('sfzh1'), \
                              when(df.sfzh1 > df.sfzh2, df.sfzh2).otherwise(df.sfzh1).alias('sfzh2')).dropDuplicates()
    common_mother = common_mother \
                    .selectExpr('sfzh1','sfzh2','format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time')
    write_orc(common_mother, add_save_path('edge_find_common_mother'))
    logger.info('common_mother down')

def find_common_grand():
    '''
    父亲的父亲，父亲的母亲，母亲的父亲，母亲的母亲
    父亲的父亲的父亲，父亲的父亲的母亲，母亲的父亲的父亲，母亲的母亲的父亲
    父亲的母亲的父亲，父亲的母亲的母亲，母亲的父亲的母亲，母亲的母亲的母亲

    结果集去掉 同父，同母的数据
    :return:  结果
    '''

    tmp_mother = read_orc(add_save_path('edge_person_motheris_person')).select('sfzh','mqsfzh')
    find_mohter = read_orc(add_save_path('edge_find_mother')).select('sfzh','mqsfzh')
    mother = tmp_mother.unionAll(find_mohter).dropDuplicates()

    tmp_father = read_orc(add_save_path('edge_person_fatheris_person')).select('sfzh', 'fqsfzh')
    find_father = read_orc(add_save_path('edge_find_father')).select('sfzh', 'fqsfzh')
    father = tmp_father.unionAll(find_father).dropDuplicates()

    write_parquet(father, 'tmp/father')
    father = read_parquet('tmp/father')
    write_parquet(mother, 'tmp/mother')
    mother = read_parquet('tmp/mother')

    # 合并所有人的身份证
    tmp = father.select('sfzh') \
        .unionAll(father.select('fqsfzh').alias('sfzh')) \
        .unionAll(mother.select('mqsfzh').alias('sfzh')) \
        .unionAll(mother.select('sfzh')).dropDuplicates()
    source = tmp.withColumnRenamed('sfzh', 'sfzh1')

    # 上一代 儿子的父母
    # 一个人 父亲的数据
    fu_df = source.join(father, source.sfzh1 == father.sfzh, 'left') \
        .select(source.sfzh1, father.fqsfzh)
    # 一个人 父亲+母亲
    fm_df = fu_df.join(mother, fu_df.sfzh1 == mother.sfzh, 'left') \
        .select(fu_df.sfzh1, fu_df.fqsfzh, mother.mqsfzh)

    p_df = fm_df.where('fqsfzh is not null or mqsfzh is not null')
    write_parquet(p_df, 'tmp/parents')
    p_df = read_parquet('tmp/parents')

    # 上二代 儿子的父母的父母
    # 父亲的父亲
    father_father = p_df.join(father, p_df.fqsfzh == father.sfzh, 'left') \
        .select(p_df.sfzh1, father.fqsfzh.alias('grandsfzh'))
    # 父亲的母亲
    father_mother = p_df.join(mother, p_df.fqsfzh == mother.sfzh, 'left') \
        .select(p_df.sfzh1, mother.mqsfzh.alias('grandsfzh'))
    # 母亲的父亲
    mother_father = p_df.join(father, p_df.mqsfzh == father.sfzh, 'left') \
        .select(p_df.sfzh1, father.fqsfzh.alias('grandsfzh'))
    # 母亲的母亲
    mother_mother = p_df.join(mother, p_df.mqsfzh == mother.sfzh, 'left') \
        .select(p_df.sfzh1, mother.mqsfzh.alias('grandsfzh'))

    # 保存中间结果
    write_parquet(father_father, 'tmp/father_father')
    write_parquet(father_mother, 'tmp/father_mother')
    write_parquet(mother_father, 'tmp/mother_father')
    write_parquet(mother_mother, 'tmp/mother_mother')

    father_father = read_parquet('tmp/father_father')
    father_mother = read_parquet('tmp/father_mother')
    mother_father = read_parquet('tmp/mother_father')
    mother_mother = read_parquet('tmp/mother_mother')

    grandAll = father_father.unionAll(father_mother).unionAll(mother_father).unionAll(mother_mother)
    grandAll.createOrReplaceTempView('tmp')

    # 计算祖父母关系
    tmp1 = spark.sql(''' select sfzh1 sfzh, grandsfzh from tmp where grandsfzh is not null''').dropDuplicates()
    tmp1 = tmp1.selectExpr('sfzh','grandsfzh','format_starttime_from_sfzh(sfzh) start_time','0 end_time')
    write_orc(tmp1,add_save_path('edge_find_grandparents'))

    # 计算 同祖父母
    grandAll.createOrReplaceTempView('tmp')
    tmp2 = spark.sql(
        '''select grandsfzh,collect_set(sfzh1) from tmp  where grandsfzh is not null  group by grandsfzh ''')
    df = spark.createDataFrame(tmp2.rdd.flatMap(deal_common_relate), ['sfzh1', 'sfzh2', 'grandsfzh']).drop('grandsfzh')

    common_grand = df.select(when(df.sfzh1 > df.sfzh2, df.sfzh1).otherwise(df.sfzh2).alias('sfzh1'),
                             when(df.sfzh1 > df.sfzh2, df.sfzh2).otherwise(df.sfzh1).alias('sfzh2')).dropDuplicates()

    common_mother = read_orc(add_save_path('edge_find_common_mother')).select('sfzh1','sfzh2')
    common_father = read_orc(add_save_path('edge_find_common_father')).select('sfzh1','sfzh2')
    # 排除同父同母情况
    find_common_grand = common_grand.subtract(common_mother).subtract(common_father)
    find_common_grand = find_common_grand \
        .selectExpr('sfzh1','sfzh2','format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time')
    write_orc(find_common_grand, add_save_path('edge_find_common_grand'))
    logger.info('edge_find_common_grand down')

    # ================曾祖父母
    father_father = read_parquet('tmp/father_father').where('grandsfzh is not null')
    father_mother = read_parquet('tmp/father_mother').where('grandsfzh is not null')
    mother_father = read_parquet('tmp/mother_father').where('grandsfzh is not null')
    mother_mother = read_parquet('tmp/mother_mother').where('grandsfzh is not null')

    # 父亲的父亲的父亲
    father_father_father = father_father.join(father, father_father.grandsfzh == father.sfzh, 'left') \
        .select(father_father.sfzh1, father.fqsfzh.alias('greatgrandsfzh'))
    # 父亲的父亲的母亲
    father_father_mother = father_father.join(mother, father_father.grandsfzh == mother.sfzh, 'left') \
        .select(father_father.sfzh1, mother.mqsfzh.alias('greatgrandsfzh'))

    # 父亲的母亲的父亲
    father_mother_father = father_mother.join(father, father_mother.grandsfzh == father.sfzh, 'left') \
        .select(father_mother.sfzh1, father.fqsfzh.alias('greatgrandsfzh'))
    # 父亲的母亲的母亲
    father_mother_mother = father_mother.join(mother, father_mother.grandsfzh == mother.sfzh, 'left') \
        .select(father_mother.sfzh1, mother.mqsfzh.alias('greatgrandsfzh'))

    # 母亲的父亲的父亲
    mother_father_father = mother_father.join(father, mother_father.grandsfzh == father.sfzh, 'left') \
        .select(mother_father.sfzh1, father.fqsfzh.alias('greatgrandsfzh'))
    # 母亲的父亲的母亲
    mother_father_mother = mother_father.join(mother, mother_father.grandsfzh == mother.sfzh, 'left') \
        .select(mother_father.sfzh1, mother.mqsfzh.alias('greatgrandsfzh'))

    # 母亲的母亲 的父亲
    mother_mother_father = mother_mother.join(father, mother_mother.grandsfzh == father.sfzh, 'left') \
        .select(mother_mother.sfzh1, father.fqsfzh.alias('greatgrandsfzh'))
    # 母亲的母亲 的母亲
    mother_mother_mother = mother_mother.join(mother, mother_mother.grandsfzh == mother.sfzh, 'left') \
        .select(mother_mother.sfzh1, mother.mqsfzh.alias('greatgrandsfzh'))

    write_parquet(father_father_father, 'tmp/father_father_father')
    write_parquet(father_father_mother, 'tmp/father_father_mother')
    write_parquet(mother_father_father, 'tmp/mother_father_father')
    write_parquet(mother_father_mother, 'tmp/mother_father_mother')
    write_parquet(father_mother_father, 'tmp/father_mother_father')
    write_parquet(father_mother_mother, 'tmp/father_mother_mother')
    write_parquet(mother_mother_father, 'tmp/mother_mother_father')
    write_parquet(mother_mother_mother, 'tmp/mother_mother_mother')

    father_father_father = read_parquet('tmp/father_father_father').where('greatgrandsfzh is not null')
    father_father_mother = read_parquet('tmp/father_father_mother').where('greatgrandsfzh is not null')
    mother_father_father = read_parquet('tmp/mother_father_father').where('greatgrandsfzh is not null')
    mother_father_mother = read_parquet('tmp/mother_father_mother').where('greatgrandsfzh is not null')
    father_mother_father = read_parquet('tmp/father_mother_father').where('greatgrandsfzh is not null')
    father_mother_mother = read_parquet('tmp/father_mother_mother').where('greatgrandsfzh is not null')
    mother_mother_father = read_parquet('tmp/mother_mother_father').where('greatgrandsfzh is not null')
    mother_mother_mother = read_parquet('tmp/mother_mother_mother').where('greatgrandsfzh is not null')

    greatgrandAll = father_father_father.unionAll(father_father_mother) \
        .unionAll(mother_father_father).unionAll(mother_father_mother) \
        .unionAll(father_mother_father).unionAll(father_mother_mother) \
        .unionAll(mother_mother_father).unionAll(mother_mother_mother)
    greatgrandAll.createOrReplaceTempView('tmp')

    # 计算曾祖父母关系
    tmp1 = spark.sql(''' select sfzh1 sfzh, greatgrandsfzh from tmp where greatgrandsfzh is not null''').dropDuplicates()
    tmp1 = tmp1.selectExpr('sfzh', 'greatgrandsfzh', 'format_starttime_from_sfzh(sfzh) start_time', '0 end_time')
    write_orc(tmp1, add_save_path('edge_find_greatgrands'))

    # 推理同曾祖父母关系
    greatgrandAll.createOrReplaceTempView('tmp')
    tmp2 = spark.sql(
        '''select greatgrandsfzh,collect_set(sfzh1) from tmp  where greatgrandsfzh is not null  group by greatgrandsfzh ''')
    df = spark.createDataFrame(tmp2.rdd.flatMap(deal_common_relate),
                               ['sfzh1', 'sfzh2', 'greatgrandsfzh']).drop('greatgrandsfzh')

    common_greatgrand = df.select(when(df.sfzh1 > df.sfzh2, df.sfzh1).otherwise(df.sfzh2).alias('sfzh1'),
                                    when(df.sfzh1 > df.sfzh2, df.sfzh2).otherwise(df.sfzh1).alias('sfzh2')).dropDuplicates()

    find_common_greatgrand = common_greatgrand.subtract(common_mother).subtract(common_father)
    res = find_common_greatgrand \
            .selectExpr('sfzh1', 'sfzh2', 'format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time')
    write_orc(res,add_save_path('edge_find_common_greatgrand'))
    logger.info('edge_find_common_greatgrand down')

if __name__ == "__main__":
    ''' 步骤2 对步骤1 具有强依赖 不可调换顺序执行 '''
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    get_spouse_from_comm()
    get_spouse_from_hydj()
    build_edge_spouse()
    get_parents()
    get_guardian()
    find_parent()
    find_spouse()
    find_common_parents()
    find_common_grand()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))