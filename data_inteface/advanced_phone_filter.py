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
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time, copy, re, math
from datetime import datetime, timedelta,date
import argparse
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')
#warehouse_location = '/user/hive/warehouse/'
conf=SparkConf().set('spark.driver.maxResultSize', '20g')
conf.set('spark.executor.memoryOverhead', '15g')
conf.set('spark.shuffle.partitions',2000)
conf.set('spark.default.parallelism',2000)
conf.set('spark.yarn.am.cores', 4)
conf.set('spark.executor.memory', '15g')
conf.set('spark.executor.instances', 100)
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

labels_dict = {
    'l01':1,  # 昼伏夜出
    'l02':2,  # 频繁跨区域移动
    'l03':3,  #
    'l04':4,
    'l05':5,
    'l06':6,
}


path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/person_filter'
midlabel_root_home = 'advance_filter_midfile'  ##提前计算的中间结果表
save_root = 'relation_theme_extenddir'

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

def read_csv(file,schema):
    path= '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/kill_case/'+file
    return spark.read.csv(path, schema=schema,header=False)

def write_jdbc(df,tablename,prop,mode='overwrite'):
    df.write.jdbc(url=prop['url'],mode=mode,table='bbd_task'+tablename,properties=prop)

def get_upload_phones(upload_id,label='phone'):
    prop = {
        "url": 'jdbc:mysql://24.2.26.44:3307/graphspace_sx_all?characterEncoding=UTF-8',
        "user": "bbd",
        "password": "bbdtnb",
        "driver": "com.mysql.jdbc.Driver",
        "dbtable": "(SELECT index_key phone FROM tb_upload_nodes WHERE upload_id = '%s' and label='%s') tmp"%(upload_id,label)
    }
    return spark.read.format('jdbc').options(**prop).load().distinct()


def handle_args():
    parser = argparse.ArgumentParser(description='args manual')
    parser.add_argument('--task_id', type=str, default='')
    parser.add_argument('--is_all', type=int, default=1)
    parser.add_argument('--upload_id', type=int, default=1)

    phone_basic_conds = ['home_location','pos_location','pos_start_time','pos_end_time']
    for arg in phone_basic_conds:
        # 电话基本属性
        parser.add_argument('--%s'%arg, type=str, default='')

    call_basic_conds = ['min_person_num','max_person_num','start_area','end_area','start_date','end_date','start_hour','end_hour',
                   'start_time','end_time','min_call_times','max_call_times','min_call_duration','max_call_duration']

    for arg in call_basic_conds:
        # 通联基本属性
        parser.add_argument('--%s'%arg, type=str, default='')

    link_basic_conds = ['is_link_zjhm','is_link_package','is_link_email','is_link_wechat','is_link_qq']
    for arg in link_basic_conds:
        # 通联基本属性
        parser.add_argument('--%s'%arg, type=int, default=0)

    args = vars(parser.parse_args())
    task_id = args.get('task_id')
    is_all = args.get('is_all')
    upload_id = args.get('upload_id')

    phone_args, call_args, link_args,labels_args = {},{},{},{}

    phone_args['home_location'] = args.get('home_location')
    phone_args['pos_location'] = args.get('pos_location')
    phone_args['pos_start_time'] = args.get('pos_start_time')
    phone_args['pos_end_time'] = args.get('pos_end_time')

    call_args['min_person_num'] = args.get('min_person_num')
    call_args['max_person_num'] = args.get('max_person_num')
    call_args['start_area'] = args.get('start_area')
    call_args['end_area'] = args.get('end_area')
    call_args['start_date'] = args.get('start_date')
    call_args['end_date'] = args.get('end_date')
    call_args['start_hour'] = args.get('start_hour')
    call_args['end_hour'] = args.get('end_hour')
    call_args['min_call_times'] = args.get('min_call_times')
    call_args['max_call_times'] = args.get('max_call_times')
    call_args['min_call_duration'] = args.get('min_call_duration')
    call_args['max_call_duration'] = args.get('max_call_duration')

    link_args['is_link_zjhm'] = args.get('is_link_zjhm')
    link_args['is_link_package'] = args.get('is_link_package')
    link_args['is_link_email'] = args.get('is_link_email')
    link_args['is_link_wechat'] = args.get('is_link_wechat')
    link_args['is_link_qq'] = args.get('is_link_qq')

    labels_args['find_type'] = int(args.get('find_type')) if args.get('find_type') else 0
    labels_args['labels'] = args.get('labels')

    return task_id, is_all, upload_id, phone_args,call_args,link_args,labels_args

def check_all_conds(conds):
    for k,v in conds.items():
        if v:
            return True
    return False

def build_cond(col_name,cond,operator,args):
    '''
    构建条件参数
    :param col_name: 字段名称
    :param cond: 条件参数名
    :param operator: 连接运算符
    :param args: 参数字典
    :return:
    '''
    if operator == 'in':
        return '%s %s %s' % (col_name, operator, "('" + "','".join(args.get(cond).split(',')) + "')") if args.get(cond) else ''
    return '%s %s %s'%(col_name,operator,args.get(cond)) if args.get(cond) else ''

def union_cond(union_type,*conds):
    conds = filter(lambda x: x != '',conds)
    if conds:
        return union_type.join(conds)
    return ''

def check_exists_res(df,msg):
    if not df.take(1,):
        logger.info(msg)
        sys.exit(0)
    pass

def phone_filter():
    task_id, is_all, upload_id, phone_args, call_args, link_args,labels_args = handle_args()
    phone_basic_cols = ['phone','concat(province,city) home_location']
    basic_phone = read_orc(add_save_path('vertex_phonenumber',root=save_root)).selectExpr(*phone_basic_cols)
    phones = basic_phone.selectExpr('phone').where('substr(phone,1,1)=1 and length(phone)=11').dropDuplicates()

    if check_all_conds(phone_args):
        '''1 归属地筛选 2 定位区域筛选 '''
        ## 手机定位信息
        phone_pos = read_orc(add_save_path('phone_location_detail',root=midlabel_root_home))
        home_cond = build_cond('home_location','home_location','in',phone_args)
        logger.info('home_cond : %s'%home_cond)
        phones = basic_phone.where(home_cond).select('phone') if home_cond else phones

        cond_pos_loc = build_cond('area','pos_location','in',phone_args)
        cond_pos_start = build_cond('cur_time','pos_start_time','>=',phone_args)
        cond_pos_end = build_cond('cur_time','pos_end_time','<=',phone_args)
        cond_pos = union_cond(' and ',cond_pos_loc,cond_pos_start,cond_pos_end)

        if cond_pos:
            pos_phones = phone_pos.where(cond_pos).select('phone')
            phones = phones.intersect(pos_phones).dropDuplicates()
            check_exists_res(phones,'home_or_pos exit!!')

        if not is_all:
            upload_phone = get_upload_phones(upload_id)
            phones = phones.intersect(upload_phone).dropDuplicates()
            check_exists_res(phones,'intersect_upload exit!!')


    if check_all_conds(link_args):
        cond_zj = build_cond('is_zj', 'is_link_zjhm', '=', link_args)
        cond_qq = build_cond('is_qq', 'is_link_qq', '=', link_args)
        cond_we = build_cond('is_we', 'is_link_wechat', '=', link_args)
        cond_pg = build_cond('is_pg', 'is_link_package', '=', link_args)
        cond_link = union_cond(' and ',cond_zj,cond_qq,cond_we,cond_pg)
        if cond_link:
            logger.info('cond_link %s'%cond_link)
            link_source = read_orc(add_save_path('phone_link_detail',root=midlabel_root_home))
            link_phones = link_source.where(cond_link).selectExpr('phone')
            phones = phones.intersect(link_phones)
            phones.show()
            check_exists_res(phones,'call_link exit!!')

    if check_all_conds(call_args):
        min_person_num = build_cond('person_num', 'min_person_num', '>=', call_args)
        max_person_num = build_cond('person_num', 'max_person_num', '<=', call_args)
        cond_person_num = union_cond(' and ', min_person_num, max_person_num)

        if cond_person_num:
            ''' 电话关联人筛选 '''
            logger.info('cond_person_num %s' % cond_person_num)
            link_person_phone = read_orc(add_save_path('edge_phone_link_person', root='advance_filter_midfile')) \
                .where(cond_person_num).selectExpr('phone')
            phones = link_person_phone.intersect(phones).dropDuplicates()
            check_exists_res(phones,'phone_link_person exit!!')


        call_basic_cols = ['start_phone', 'end_phone', 'homearea start_area', 'relatehomeac end_area',
                           'start_time', 'end_time', 'call_duration']
        call_info = read_orc(add_save_path('edge_groupcall_detail',root=save_root)).selectExpr(*call_basic_cols)

        start_date = build_cond('start_time', 'start_time', '>=', phone_args)
        end_date = build_cond('end_time', 'end_time', '<=', phone_args)
        start_hour = build_cond('start_hour', 'start_hour', '>=', phone_args)
        end_hour = build_cond('end_hour', 'end_hour', '<=', phone_args)
        cond_date = union_cond(' and ', start_date, end_date)
        cond_hour = union_cond(' and ', start_hour, end_hour)

        if cond_date:
            ''' 通话时间筛选 '''
            logger.info('cond_date %s' % cond_date)
            call_info = call_info.filter(cond_date)
            date_phones = call_info.selectExpr('start_phone phone').unionAll(call_info.selectExpr('end_phone phone'))
            phones = date_phones.intersect(phones).dropDuplicates()
            check_exists_res(phones,'call_duartion_date exit!!')


        if cond_hour:
            expr = ['*', 'timestamp2hourtype(start_time) start_hour', 'timestamp2hourtype(end_time) end_hour']
            call_info = call_info.selectExpr(*expr).filter(cond_hour)
            hour_phones = call_info.selectExpr('start_phone phone').unionAll(call_info.selectExpr('end_phone phone'))
            phones = hour_phones.intersect(phones).dropDuplicates()
            check_exists_res(phones,'call_duartion_hour exit!!')


        start_area = build_cond('start_area', 'start_area', '=', phone_args)
        end_area = build_cond('end_area', 'end_area', '=', phone_args)

        if start_area or end_area:
            ''' 主被叫地区筛选 '''
            call_info.createOrReplaceTempView('call_info')
            phones.createOrReplaceTempView('phones')

            cols1 = ['start_phone phone','start_area','end_area','start_time','end_time','call_duration']
            cols2 = ['end_phone phone','start_area','end_area','start_time','end_time','call_duration']

            calling_sql = ''' select /*+ BROADCAST (phones)*/ a.* from call_info a
                        inner join phones b on a.start_phone = b.phone 
                    '''

            called_sql = ''' select /*+ BROADCAST (phones)*/ a.* from call_info a
                        inner join phones b on a.end_phone = b.phone 
                    '''

            calling_df = spark.sql(calling_sql).selectExpr(*cols1)
            called_df = spark.sql(called_sql).selectExpr(*cols2)

            call_info = calling_df.unionAll(called_df)
            check_exists_res(call_info,'calling_intersect exit!!')

            if start_area:
                logger.info('cond_start_area %s'%start_area)
                calling_df = calling_df.where(start_area)

            if end_area:
                logger.info('cond_end_area %s'%end_area)
                called_df = called_df.where(end_area)
            call_info = calling_df.unionAll(called_df)
            area_phones = call_info.where('phone is not null').selectExpr('phone')
            phones = area_phones.intersect(phones).dropDuplicates()
            check_exists_res(phones, 'called_area exit!!')

        min_call_times = build_cond('num', 'min_call_times', '>=', phone_args)
        max_call_times = build_cond('num', 'max_call_times', '<=', phone_args)
        min_call_duration = build_cond('call_duration', 'min_call_duration', '>=', phone_args)
        max_call_duration = build_cond('call_duration', 'max_call_duration', '<=', phone_args)
        cond_group = union_cond(' and ', min_call_times, max_call_times, min_call_duration, max_call_duration)

        if cond_group:
            '''通话总次数和通话总时间筛选'''
            logger.info('cond_group %s'%cond_group)
            sql = '''
                select phone, count(call_duration) as num, sum(call_duration) as call_duration 
                from tmp where phone is not null group by phone
            '''
            if 'phone' in [col[0] for col in call_info.dtypes]:
                call_info.createOrReplaceTempView('tmp')
                call_info = spark.sql(sql).where(cond_group)
            else:
                cols1 = ['start_phone phone', 'call_duration']
                cols2 = ['end_phone phone', 'call_duration']
                call_info.selectExpr(*cols1).unionAll(call_info.selectExpr(*cols2)) \
                        .where('phone is not null').createOrReplaceTempView('tmp')
                call_info = spark.sql(sql).where(cond_group)
            phones = call_info.selectExpr('phone').intersect(phones).dropDuplicates()
            check_exists_res(phones,'call_total_times_duration exit!!')

    if check_all_conds(labels_args):
        '''标签组合判断'''
        df = read_orc(add_save_path('phone_labels', root='xxx'))
        find_type = labels_args.pop('find_type')
        labels_index = [labels_dict[_] for _ in sorted(labels_args.pop('labels').split(','))]
        res = []
        for i in labels_index:
            res.append('substr(labels,%s,1)' % str(i))
        # 组合判断条件
        label_cond = 'filter_labels(concat(%s),%s)' % (','.join(res), find_type)
        label_phone = df.filter(label_cond).select('phone').distinct()
        phones = phones.intersect(label_phone)
        check_exists_res(phones,'phone_labels exit!!')

    res = phones.withColumn('task_id',lit(task_id)).limit(2000)
    res = res.withColumnRenamed('phone','node_num')
    write_jdbc(res,task_id,prop)

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    prop = {
        "url": 'jdbc:mysql://24.2.26.44:3307/graphspace_sx_all?characterEncoding=UTF-8',
        "user": "bbd",
        "password": "bbdtnb",
        "driver": "com.mysql.jdbc.Driver",
        "ip": "24.2.26.44",
        "port": "3307",
        "db_name":"graphspace_sx_all",
        "mode": 'overwrite'}
    phone_filter()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))