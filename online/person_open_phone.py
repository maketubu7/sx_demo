# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 19:01
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : person_open_phone.py
# @Software: PyCharm
# @content : 身份证号码开户信息，暂时为实名制
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os,sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')


conf=SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.yarn.am.cores', 5)
conf.set('soark.shuffle.partitions',800)
conf.set('spark.executor.memory', '20g')
conf.set('spark.executor.instances', 50)
conf.set('spark.executor.cores', 8)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')


spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
from person_link_phone_info import person_link_phone
commonUtil = CommonUdf(spark)

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/open_phone'

##todo:all
def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix,'parquet/',path))
    return df
def write_parquet(df,path,method='overwrite'):
    '''写 parquet'''
    df.write.mode(method).parquet(path=os.path.join(path_prefix,'parquet/',path))


def write_orc(df, path,mode='overwrite'):
    df.write.format('orc').mode(mode).save(path)
    logger.info('write success')


def read_orc(path):
    df = spark.read.orc(path)
    return df

def edge_person_open_phone():
    '''
    1、ODS_SOC_SBD_PHONE_OWNER	机主表
        COLL_TIME	采集时间
        MOB	移动电话
        CRED_NUM	证件号码
        LOSEFF_TIME	失效时间
        ACCEP_TIME	受理时间
    :return:
    '''
    ## 所有的开户数据 时间取得是入网时间
    init(spark,'ods_soc_sbd_phone_owner',if_write=False)
    sql1 = '''
        select format_zjhm(cred_num) sfzh, format_phone(mob) phone,
        cast(format_timestamp(ente_net_time) as bigint) start_time,
        'ods_soc_sbd_phone_owner' tablename,
        row_number() over (partition by format_zjhm(cred_num),format_phone(mob) order by proc_time desc) num
        from ods_soc_sbd_phone_owner 
        where verify_sfz(cred_num) = 1 and verify_phonenumber(mob) = 1
    '''

    df1 = spark.sql(sql1).where('num=1').drop('num')

    ## 注销的数据 默认取用户状态中包含 %拆% %销% %停% 的开户数据
    ## 时间和前面一样取得都是 入网时间
    init(spark, 'ods_soc_sbd_phone_owner', if_write=False)
    sql2 = '''
            select format_zjhm(cred_num) sfzh, format_phone(mob) phone,
            cast(format_timestamp(ente_net_time) as bigint) end_time,
            '是' is_xiaohu,
            'ods_soc_sbd_phone_owner' tablename,
            row_number() over (partition by format_zjhm(cred_num),format_phone(mob) order by proc_time desc) num
            from ods_soc_sbd_phone_owner 
            where verify_sfz(cred_num) = 1 and verify_phonenumber(mob) = 1 and 
            (user_stat_stat_name like '%拆%' or user_stat_stat_name like '%销%' or user_stat_stat_name like '%停%')
        '''

    df2 = spark.sql(sql2).where('num=1').drop('num')

    ## 所有数据,加上注销标识码

    df = df1.join(df2,['sfzh','phone'],'left') \
            .select(df1.sfzh, df1.phone, df1.start_time, df2.end_time, df2.is_xiaohu, df1.tablename)\
            .na.fill({'end_time':0,'is_xiaohu':u'否'})

    write_orc(df,add_save_path('edge_person_open_phone'))
    logger.info('edge_person_open_phone down')

def get_smz_info():
    '''
    通过开户信息表，提取实名制信息，提取规则为
    时间最新，即为当前最新实名制
    todo: 开户表取未注销的数据 默认为实名制， 后期加入实名制
    '''
    create_tmpview_table(spark,'edge_person_open_phone')

    df = spark.sql('select * from edge_person_open_phone where is_xiaohu="否"')

    smz = df.selectExpr('sfzh start_person','phone end_phone','start_time','end_time')
    write_orc(smz,add_save_path('edge_person_smz_phone'))

    ## 以电话号为准 将 start_time 最晚的那个作为最高关联实名制
    smz_top = df.selectExpr('sfzh start_person','phone end_phone','start_time','end_time',
                             'row_number() over (partition by phone order by start_time desc) num') \
                .where('num=1').drop('num')

    write_orc(smz_top,add_save_path('edge_person_smz_phone_top'))

def edge_person_link_phone():
    '''人关联电话'''
    tmp_sql = '''
        select format_zjhm({zjhm}) sfzh,format_phone({phone}) phone,cast({link_time} as bigint) linktime,
        '{tablename}' as tablename from {tablename} 
        where verify_sfz({zjhm})=1 and verify_phonenumber({phone})=1  '''

    union_df = create_uniondf(spark,person_link_phone,tmp_sql)
    union_df.createOrReplaceTempView('tmp')
    sql = '''
        select sfzh, phone,min(linktime) start_time, max(linktime) end_time, count(1) as num
        from tmp group by sfzh, phone
    '''

    res = spark.sql(sql)
    write_orc(res,add_save_path('edge_person_link_phone'))

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    edge_person_open_phone()
    get_smz_info()
    edge_person_link_phone()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))