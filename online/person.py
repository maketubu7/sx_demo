# -*- coding: utf-8 -*-
# @Time    : 2020/3/14 11:02
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : person.py
# @Software: PyCharm
import logging
import os
# from pyspark.sql import functions as fun
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

conf = SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memoryOverhead', '10g')
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
from person_schema_info import inbord_person_schema, oversea_person_schema

commonUtil = CommonUdf(spark)

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/person'


##todo:all
def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix, 'parquet/', path))
    return df


def write_parquet(df, path, method='overwrite'):
    '''写 parquet'''
    df.write.mode(method).parquet(path=os.path.join(path_prefix, 'parquet/', path))


def write_orc(df, path, mode='overwrite'):
    df.write.format('orc').mode(mode).save(path)
    logger.info('write success')


def read_orc(path):
    df = spark.read.orc(path)
    return df

def format_whcd(whcd):

    def match_word(whcd,types):
        res = ""
        if not whcd:
            return u'其他'
        for words in types:
            for key in words:
                if key in whcd:
                    res = "%s(%s)"%(words[0],whcd)
                    return res
        return res if res else u'其他(%s)'%whcd

    primary_words = [u'小学']
    junior_words = [u'初中',u'初级中学']
    senior_words = [u'高中',u'中等',u'中技',u'技工']
    college_words = [u'大学']
    graduate_words = [u'研究生',u'硕士',u'博士']

    types = [primary_words,junior_words,senior_words,college_words,graduate_words]
    try:
        return match_word(whcd,types)
    except:
        return u'其他'

spark.udf.register('format_whcd',format_whcd,StringType())

def format_hyzk(hyzk):
    married = [u'复婚',u'已婚',u'初婚',u'再婚',u'丧偶']
    unmarried = [u'离婚',u'未婚']
    try:
        if not hyzk:
            return u'其他'
        elif hyzk in married:
            return u'已婚(%s)'%hyzk
        elif hyzk in unmarried:
            return u'未婚(%s)'%hyzk
        else:
            return u'其他(%s)'%hyzk
    except:
        return u'其他'

spark.udf.register('format_hyzk',format_hyzk,StringType())

def person_etl():
    '''人节点清洗'''
    # row_number() over (partition by format_zjhm({zjhm}) order by {rank_time} desc) as num
    inboard_sql = ''' select format_zjhm({zjhm}) zjhm,'身份证' zjlx,{gj} gj,{xm} xm,
        {ywxm} ywxm,{zym} zym,format_xb({zjhm}) xb,
        format_csrq_from_sfzh({zjhm}) csrq, {mz} mz,{jg} jg,{whcd} whcd,{hyzk} hyzk,
        {zzmm} zzmm,{hkszdxz} hkszdxz,{sjjzxz} sjjzxz,
        '{tablename}' as tablename, {table_sort} table_sort
        from {tablename} where verify_sfz({zjhm})=1
        '''
    #
    for item in inbord_person_schema[19:]:
        sql = inboard_sql.format(**item)
        logger.info(sql)
        tablename = item['tablename']
        if tablename.startswith('edge'):
            create_tmpview_table(spark,tablename)
        else:
            init(spark, item['tablename'], if_write=False)
            df = spark.sql(sql)
            write_parquet(df, item['tablename'] + '-' + item['zjhm'])
            logger.info('%s down' % item['tablename'])

    ## 外国人 row_number() over(partition by format_zjhm({zjhm}) order by {rank_time} desc) as num
    over_sql = '''
            select format_zjhm({zjhm}) zjhm, '身份证' zjlx, {gj} gj, {xm} xm,
            {ywxm} ywxm, {zym} zym, format_xb({zjhm}) xb,format_csrq_from_sfzh({zjhm}) csrq,
            {mz} mz, {jg} jg, {whcd} whcd, {hyzk} hyzk, {zzmm} zzmm, {hkszdxz} hkszdxz, {sjjzxz} sjjzxz,
            '{tablename}' tablename, {table_sort} table_sort
            from {tablename} where verify_zjhm({zjhm}) = 1 and verify_sfz({zjhm}) = 0
            '''

    for item in oversea_person_schema:
        sql = over_sql.format(**item)
        logger.info(sql)
        init(spark, item['tablename'], if_write=False)
        df = spark.sql(sql)
        write_parquet(df, item['tablename'] + '-' + item['zjhm'])
        logger.info('%s down' % item['tablename'])

    paths = [info['tablename'] + '-' + info['zjhm'] for info in inbord_person_schema + oversea_person_schema]
    #
    union_df = None
    for path in paths:
        df = read_parquet(path)
        if not union_df:
            union_df = df
        else:
            union_df = union_df.unionAll(df)

    res_tmp = union_df.selectExpr('*', 'row_number() over (partition by zjhm order by table_sort asc) num') \
        .where('num=1').drop('num').drop('table_sort')

    ## todo 标准化文化程度，婚姻状况
    tranfrom_schema = ['zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                                      'format_whcd(whcd) whcd', 'format_whcd(hyzk) hyzk', 'zzmm', 'hkszdxz', 'sjjzxz']
    res = res_tmp.selectExpr(*tranfrom_schema)
    write_orc(res, add_save_path('vertex_person'))
    logger.info('vertex_person down')


def test():
    df = read_orc(add_save_path('vertex_person'))
    df.where('')


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    person_etl()


    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
