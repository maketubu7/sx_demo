# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 19:01
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : person_open_phone.py
# @Software: PyCharm
# @content : 身份证号码开户信息，暂时为实名制
from pyspark import SparkConf,StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os,sys
import logging
from pyspark.sql.types import FloatType,IntegerType
from person_link_phone_info import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')


conf=SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.yarn.am.cores', 4)
conf.set('soark.shuffle.partitions',800)
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/open_phone'
save_root = 'relation_theme_extenddir'

def attenua_num(num):
    '''对关联次数多的 进行惩罚系数加持'''
    if num <= 10:
        return num
    elif num <= 50:
        return int(num*1.5)
    elif num <=100:
        return num*2
    elif num <=200:
        return num*3
    else:
        return num*8

spark.udf.register('attenua_num',attenua_num,IntegerType())


def person_relation_phone():
    # cert_type 证件类型
    # cert_num 证件号码
    # mob 手机号
    # rel_type 关系类型
    # last_dis_place 发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    # totle_count 累计发现次数
    # totle_days 累计发现天数
    init(spark, 'nb_app_dws_per_res_his_dcmobile', if_write=False, is_relate=True)
    sql = '''
        select cert_num sfzh, format_phone(mob) phone,rel_type type, cast(first_time as bigint) start_time,
        cast(last_time as bigint) end_time from nb_app_dws_per_res_his_dcmobile
        where verify_sfz(cert_num) = 1 and verify_simple_phonenumber(mob) = 1
    '''

    source = spark.sql(sql).dropDuplicates(['sfzh','phone','type'])

    zx = source.where('type = "DM03"').selectExpr('sfzh', 'phone', '"是" is_xiaohu', 'end_time')
    tmp = source.where('type = "DM04"')

    cols = ['sfzh', 'phone', 'if(start_time>0,start_time,0) start_time', 'if(end_time>0,end_time,0) end_time','is_xiaohu']
    open = tmp.join(zx, ['sfzh', 'phone'], 'left') \
        .select(tmp.sfzh, tmp.phone, tmp.start_time, coalesce(tmp.end_time, zx.end_time).alias('end_time'),
                zx.is_xiaohu) \
        .selectExpr(*cols).na.fill({'is_xiaohu': "否"})

    write_orc(open, add_save_path('edge_person_open_phone', root=save_root))

    open = read_orc(spark,add_save_path('edge_person_open_phone', root=save_root))
    cols = ['sfzh start_person','phone end_phone','start_time','end_time']
    smz_tmp = open.where('is_xiaohu="否"').selectExpr(*cols)
    smz_tmp.persist(StorageLevel.MEMORY_AND_DISK)
    smz = smz_tmp.selectExpr('start_person','end_phone','start_time','end_time',
                             'row_number() over (partition by end_phone order by start_time) weight')


    smz_count = smz.groupby('end_phone').agg(count('start_person').alias('num'))
    smz = smz.join(smz_count,'end_phone','inner').selectExpr('start_person','end_phone','start_time','end_time','round((weight/attenua_num(num))*80,2) weight')
    write_orc(smz, add_save_path('edge_person_smz_phone', root=save_root))

    smz_top = smz_tmp.selectExpr('start_person','end_phone','start_time','end_time',
                             'row_number() over (partition by end_phone order by start_time desc) num') \
                                .where('num=1').drop('num')


    write_orc(smz_top,add_save_path('edge_person_smz_phone_top',root=save_root))
    smz_tmp.unpersist()

def person_rlink_phone():
    tmp_sql = '''
        select format_zjhm({zjhm}) sfzh,format_phone({phone}) phone,cast({link_time} as bigint) linktime,
        '{tablename}' as tablename from {tablename} 
        where verify_sfz({zjhm})=1 and verify_phonenumber({phone})=1  '''

    cols1 = ['phone','sfzh','linktime']
    cols2 = ['phone','sfzh','start_time linktime']
    union_df1 = create_uniondf(spark,person_link_phone,tmp_sql).selectExpr(*cols1)
    union_df2 = read_orc(spark,add_save_path('edge_person_link_phone_zp',root=save_root)).selectExpr(*cols2)
    union_df1.unionAll(union_df2).createOrReplaceTempView('tmp')
    sql = '''
        select sfzh, phone,min(linktime) start_time, max(linktime) end_time, count(1) as num
        from tmp group by sfzh, phone
    '''
    res = spark.sql(sql)
    write_orc(res,add_save_path('edge_person_link_phone',root=save_root))


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    person_relation_phone()
    person_rlink_phone()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))