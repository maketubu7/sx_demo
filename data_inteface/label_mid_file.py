# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 19:01
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : person_open_phone.py
# @Software: PyCharm
# @content : 身份证号码开户信息，暂时为实名制
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import *
import os,sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')


conf=SparkConf()
conf.set('spark.driver.maxResultSize', '10g')
conf.set('spark.yarn.am.cores', 4)
conf.set('spark.executor.memoryOverhead', '10g')
conf.set('spark.shuffle.partitions',2000)
conf.set('spark.default.parallelism',2000)
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/lable_mid_file'
root_home = 'advance_filter_midfile'
save_root = 'relation_theme_extenddir'

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
    try:
        df = spark.read.orc(path)
        return df
    except:
        return None

def person_kh_phone_num():
    '''
    计算每个证件号码开户的电话的数量
    电话关联人的数量
    '''
    own_phone = read_orc(add_save_path('edge_person_open_phone',root=save_root)) \
        .groupby('sfzh').agg(count('phone').alias('phone_num'))

    write_orc(own_phone,add_save_path('edge_person_own_phone',root=root_home))

    link_person_1 = read_orc(add_save_path('edge_person_open_phone',root=save_root)).select('sfzh','phone')
    link_person_2 = read_orc(add_save_path('edge_person_link_phone',root=save_root)).select('sfzh','phone')
    link_person_all = link_person_1.union(link_person_2).drop_duplicates()
    link_person = link_person_all.groupby('phone').agg(count('sfzh').alias('person_num'))
    write_orc(link_person,add_save_path('edge_phone_link_person',root=root_home))



def person_call_detail():
    '''
    根据通话明细计算人与人的通话明细，以及格式化计算字段，减少前置筛选等待时间

    :return:
    '''
    create_tmpview_table(spark,'edge_groupcall_detail',root=save_root)
    create_tmpview_table(spark,'edge_person_smz_phone_top',root=save_root)

    # timestamp2hourtype(a.start_time) start_hour, timestamp2hourtype(a.end_time) end_hour

    sql = '''  
        select /*+ BROADCAST (edge_person_smz_phone_top)*/ b.start_person, c.start_person as end_person, a.homearea start_area, 
         a.relatehomeac end_area, a.start_time, a.end_time, a.call_duration
        from edge_groupcall_detail a
        left join edge_person_smz_phone_top b
        on a.start_phone=b.end_phone
        left join edge_person_smz_phone_top c
        on a.end_phone=c.end_phone
        where b.start_person is not null or c.start_person is not null
    '''

    res = spark.sql(sql)
    write_orc(res,add_save_path('edge_groupcall_person',root=root_home))

def phone_location_detail():
    cols = ['start_phone','end_phone','homearea start_area','relatehomeac end_area','start_time cur_time']
    call = read_orc(add_save_path('edge_groupcall_detail',root=save_root)) \
        .where('homearea != "" or relatehomeac != ""').selectExpr(*cols)
    write_parquet(call,"phone_location_tmp")
    call = read_parquet("phone_location_tmp")
    call.persist(StorageLevel.MEMORY_AND_DISK)
    call1 = call.where('start_area != ""').selectExpr('start_phone phone','start_area area','cur_time')
    write_parquet(call1,'call1')
    call2 = call.where('end_area != ""').selectExpr('end_phone phone','end_area area','cur_time')
    write_parquet(call2,'call2')
    call1= read_parquet('call1')
    call2= read_parquet('call2')
    phone_location = call1.unionAll(call2)
    write_orc(phone_location,add_save_path('phone_location_detail',root=root_home))


def phone_link_detail():
    cols = ['phone','1 tag']
    link_zjhm1 = read_orc(add_save_path('edge_person_link_phone',root=save_root)).selectExpr(*cols)
    link_zjhm2 = read_orc(add_save_path('edge_person_open_phone',root=save_root)).selectExpr(*cols)
    link_zjhm = link_zjhm1.union(link_zjhm2).dropDuplicates()

    link_qq = read_orc(add_save_path('edge_phone_link_qq',root=save_root)).selectExpr(*cols).dropDuplicates()
    link_wechat = read_orc(add_save_path('edge_phone_link_qq',root=save_root)).selectExpr(*cols).dropDuplicates()
    link_package1 = read_orc(add_save_path('edge_phone_receive_package',root=save_root)).selectExpr(*cols)
    link_package2 = read_orc(add_save_path('edge_phone_send_package',root=save_root)).selectExpr(*cols)
    link_package = link_package1.union(link_package2).dropDuplicates()


    all_phone = reduce(lambda a,b:a.union(b),[link_zjhm,link_qq,link_wechat,link_package]).dropDuplicates()
    res = all_phone.join(link_zjhm,'phone','left') \
            .join(link_qq,'phone','left') \
            .join(link_wechat,'phone','left') \
            .join(link_package,'phone','left') \
            .select(all_phone.phone,link_zjhm.tag.alias('is_zj'),link_qq.tag.alias('is_qq'),link_wechat.tag.alias('is_we'),link_package.tag.alias('is_pg')) \
            .na.fill(0)
    write_orc(res,add_save_path('phone_link_detail',root=root_home))


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # person_kh_phone_num()
    # person_call_detail()
    phone_location_detail()
    # phone_link_detail()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))