# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : qq_msg.py
# @Software: PyCharm
# @content : qq相关信息

import sys, os
from pyspark import SparkConf,StorageLevel
from pyspark.sql import SparkSession,dataframe
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
conf.set('spark.yarn.am.cores', 4)
conf.set('spark.shuffle.partitions',4000)
conf.set('spark.default.parallelism',4000)
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
save_root = 'relation_theme_extenddir'

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/person_filter'

labels_dict = {
    'l01':1,  # 昼伏夜出
    'l02':2,  # 频繁跨区域移动
    'l03':3,  #
    'l04':4,
    'l05':5,
    'l06':6,
}

def filter_labels(col,find_type):
    if find_type:
        # 任意为1
        return '1' in col
    if not find_type:
        # 全为1
        return not '0' in col
spark.udf.register('filter_labels',filter_labels,BooleanType())

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
    logger.info('%s readding'%path)
    df = spark.read.orc(path)
    return df

def read_csv(file,schema):
    path= '/phoebus/_fileservice/users/slmp/shulianmingpin/wa_data/kill_case/'+file
    return spark.read.csv(path, schema=schema,header=False)

def write_jdbc(df,tablename,prop,mode='overwrite'):
    df.write.jdbc(url=prop['url'],mode=mode,table='bbd_task'+tablename,properties=prop)

def handle_args():
    ###   解析参数列表
    parser = argparse.ArgumentParser(description='args manual')

    ##人物基础信息筛选
    parser.add_argument('--task_id', type=str, default='')
    parser.add_argument('--is_all', type=int, default=1)
    parser.add_argument('--upload_id', type=int, default=1)

    person_conds = ['hjdm','hjjd','sjjzxz','xb','mz','whcd','hyzk','start_csrq','end_csrq']
    for arg in person_conds:
        parser.add_argument('--%s'%arg, type=str, default='')

    ## 人实名制top1电话信息
    phone_conds = ['min_phone_num','max_phone_num','start_area','end_area','start_date','end_date','start_time','end_time',
                   'start_hour','end_hour','min_call_times','max_call_times','min_call_duration','max_call_duration']
    for arg in phone_conds:
        parser.add_argument('--%s'%arg, type=str, default='')

    ## 人标签筛选
    parser.add_argument('--find_type', type=int, default=0)
    parser.add_argument('--labels', type=str, default='')
    args = vars(parser.parse_args())

    task_id = args.get('task_id')
    is_all = args.get('is_all')
    upload_id = args.get('upload_id')

    person_args, phone_args, labels_args = {}, {}, {}

    person_args['hjdm'] = ["('"+"','".join(filter(lambda x:x,args.get('hjdm').split(',')))+"')",'in'] if args.get('hjdm') else []
    person_args['hjjd'] = args.get('hjjd').replace("，",",").split(',') if args.get('hjjd') else []
    person_args['sjjzxz'] = args.get('sjjzxz').replace("，",",").split(',') if args.get('sjjzxz') else []
    person_args['xb'] = [args.get('xb'),'='] if args.get('xb') else []
    person_args['mz'] = [args.get('mz'),'='] if args.get('mz') else []
    person_args['whcd'] = ['"%'+args.get('whcd')+'%"','like'] if args.get('whcd') else []
    person_args['hyzk'] = ['"%'+args.get('hyzk')+'%"','like'] if args.get('hyzk') else []
    person_args['start_csrq'] = [args.get('start_csrq'),'>='] if args.get('start_csrq') else []
    person_args['end_csrq'] = [args.get('end_csrq'),'<='] if args.get('end_csrq') else []

    phone_args['min_phone_num'] = int(args.get('min_phone_num')) if args.get('max_phone_num') else 0
    phone_args['max_phone_num'] = int(args.get('max_phone_num')) if args.get('max_phone_num') else 0
    phone_args['start_area'] = args.get('start_area')
    phone_args['end_area'] = args.get('end_area')
    phone_args['start_time'] = args.get('start_time')
    phone_args['end_time'] = args.get('end_time')
    phone_args['start_hour'] = args.get('start_hour')
    phone_args['end_hour'] = args.get('end_hour')
    phone_args['min_call_times'] = args.get('min_call_times')
    phone_args['max_call_times'] = args.get('max_call_times')
    phone_args['min_call_duration'] = args.get('min_call_duration')
    phone_args['max_call_duration'] = args.get('max_call_duration')


    labels_args['find_type'] = int(args.get('find_type')) if args.get('find_type') else 0
    labels_args['labels'] = args.get('labels')


    return task_id,is_all,upload_id,person_args,phone_args,labels_args

def check_all_conds(conds):
    for k,v in conds.items():
        if v:
            return True
    return False

def build_multi_cond(col_name,conds_key,operator="like",is_all= ' or '):
    '''
    多条件模糊匹配
    :param col_name: 条件对应列名
    :param operator: 运算符
    :param conns_key: 模糊匹配目标值[v1,v2]
    :param is_all: or 任一满足 and 全部满足
    :return:
    '''
    res = []
    if conds_key:
        # return col_name + " rlike('" + '|'.join(conds_key) +"')"
        for cond in conds_key:
            res.append(" %s %s '%%%s%%' "%(col_name,operator,cond))
        return ' ('+is_all.join(res)+') '
    return ''



def build_cond(col_name,cond,operator,args):
    '''
    构建条件参数
    :param col_name: 字段名称
    :param cond: 条件参数名
    :param operator: 连接运算符
    :param args: 参数字典
    :return:
    '''
    return ''' %s %s %s '''%(col_name,operator,args.get(cond)) if args.get(cond) else ''

def union_cond(union_type,*conds):
    conds = filter(lambda x: x != '',conds)
    if conds:
        return union_type.join(conds)
    return ''

def get_upload_zjhms(upload_id,label='person'):
    prop = {
        "url": 'jdbc:mysql://24.2.26.44:3307/graphspace_sx_all?characterEncoding=UTF-8',
        "user": "bbd",
        "password": "bbdtnb",
        "driver": "com.mysql.jdbc.Driver",
        "dbtable": "(SELECT index_key zjhm FROM tb_upload_nodes WHERE upload_id = '%s' and label='%s') tmp"%(upload_id,label)
    }
    return spark.read.format('jdbc').options(**prop).load().distinct()

def check_exists_res(df,msg):
    if not df.take(1):
        logger.info(msg)
        sys.exit(0)
    pass


def person_filter():
    task_id,is_all,upload_id,person_args, phone_args, labels_args = handle_args()

    basic_cols = ['zjhm','substr(zjhm,0,6) hjdm','csrq start_csrq','csrq end_csrq','xb','mz','whcd','hyzk','sjjzxz','jg']
    basic_person = read_orc(add_save_path('vertex_person',root=save_root)).selectExpr(*basic_cols)
    zjhms = basic_person.selectExpr('zjhm').dropDuplicates()
    if check_all_conds(person_args):
        '''对传入参数进行条件拼接'''
        conds = []
        logger.info('person_args_dict')
        logger.info(person_args)
        sjjzxz = person_args.pop('sjjzxz')  ## 多条件模糊匹配
        hjjd = person_args.pop('hjjd')  ## 多条件模糊匹配
        cond_sjjzxz = build_multi_cond('sjjzxz',sjjzxz)
        cond_hjjd = build_multi_cond('jg',hjjd)
        for col, cond in person_args.items():
            if cond:
                value = cond[0]
                operator = cond[1]
                if operator == '=':
                    conds.append(''' {} {} '{}' '''.format(col,operator,value))
                elif value and value != '"%"' and value != '"%%"':
                    conds.append(' {} {} {} '.format(col,operator,value))

        if cond_sjjzxz:conds.append(cond_sjjzxz)
        if cond_hjjd:conds.append(cond_hjjd)
        cond_str = 'and '.join(conds)
        logger.info(conds)
        logger.info('person_args %s'%cond_str)
        zjhms = basic_person.filter(cond_str).select('zjhm').distinct()
        logger.info('basic show')
        zjhms.show()

    check_exists_res(zjhms,'cond_person_basic exit!!')

    if not is_all:
        upload_zjhms = get_upload_zjhms(upload_id)
        logger.info('upload_zjhms show')
        upload_zjhms.show()
        zjhms = zjhms.intersect(upload_zjhms)
        check_exists_res(zjhms, 'upload_intersect exit!!')

    if check_all_conds(phone_args):
        min_phone_num = build_cond('phone_num', 'min_phone_num', '>=', phone_args)
        max_phone_num = build_cond('phone_num', 'max_phone_num', '<=', phone_args)
        cond_phone_num = union_cond(' and ', min_phone_num, max_phone_num)
        if cond_phone_num:
            ''' 拥有电话筛选 '''
            logger.info('phone_num %s'%cond_phone_num)
            own_phone = read_orc(add_save_path('edge_person_own_phone',root='advance_filter_midfile')) \
                    .where(cond_phone_num).selectExpr('sfzh jzhm')
            logger.info('own_phone.show()')
            own_phone.show()
            zjhms = zjhms.intersect(own_phone).dropDuplicates()
            check_exists_res(zjhms,'cond_phone_num exit!!')

        start_date = build_cond('start_time', 'start_time', '>=', phone_args)
        end_date = build_cond('end_time', 'end_time', '<=', phone_args)
        cond_date = union_cond(' and ', start_date, end_date)

        start_hour = build_cond('start_hour','start_hour','>=',phone_args)
        end_hour = build_cond('end_hour','end_hour','<=',phone_args)
        cond_hour = union_cond(' and ', start_hour,end_hour)

        source_call_info = read_orc(add_save_path('edge_groupcall_person', root='advance_filter_midfile'))
        source_call_info.persist(StorageLevel.MEMORY_AND_DISK)
        call_info = source_call_info
        logger.info('cond_date %s' % cond_date)
        if cond_date:
            ''' 通话时间筛选 '''
            logger.info('enter cond_date %s' % cond_date)
            call_info = call_info.filter(cond_date)
            date_zjhm = call_info.selectExpr('start_person zjhm').unionAll(call_info.selectExpr('end_person zjhm')) \
                .where('zjhm is not null')
            zjhms = date_zjhm.intersect(zjhms).dropDuplicates()
            check_exists_res(zjhms, 'call_duration_date_time exit!!')

        if cond_hour:
            expr = ['*','timestamp2hourtype(start_time) start_hour', 'timestamp2hourtype(end_time) end_hour']
            logger.info('cond_hour %s' % cond_hour)
            call_info = call_info.selectExpr(*expr).filter(cond_hour)
            hour_zjhm = call_info.selectExpr('start_person zjhm').unionAll(call_info.selectExpr('end_person zjhm')) \
                .where('zjhm is not null')
            zjhms = hour_zjhm.intersect(zjhms).dropDuplicates()
            check_exists_res(zjhms, 'call_duration_hour_time exit!!')

        start_area = build_cond('start_area','start_area','=',phone_args)
        end_area = build_cond('end_area','end_area','=',phone_args)
        cond_area = union_cond(' and ',start_area,end_area)


        if start_area or end_area:
            ''' 主被叫地区筛选 '''
            logger.info('cond_area %s'%cond_area)

            call_info.createOrReplaceTempView('call_info')
            zjhms.createOrReplaceTempView('zjhms')

            cols1 = ['start_person zjhm','start_area','end_area','start_time','end_time','call_duration']
            cols2 = ['end_person zjhm','start_area','end_area','start_time','end_time','call_duration']

            calling_sql = ''' select /*+ BROADCAST (zjhms)*/ a.* from 
                        (select * from call_info where start_person != null) a
                        inner join zjhms b on a.start_person = b.zjhm 
                    '''

            called_sql = ''' select /*+ BROADCAST (zjhms)*/ a.* from 
                        (select * from call_info where end_person != null) a
                        inner join zjhms b on a.end_person = b.zjhm 
                    '''

            calling_df = spark.sql(calling_sql).selectExpr(*cols1)
            called_df = spark.sql(called_sql).selectExpr(*cols2)

            call_info = calling_df.unionAll(called_df)
            check_exists_res(call_info,'calling_intersect exit!!')

            if start_area:
                calling_df = calling_df.where(start_area)

            if end_area:
                called_df = called_df.where(end_area)

            call_info = calling_df.unionAll(called_df).where('zjhm is not null')
            area_zjhms = call_info.selectExpr('zjhm')
            zjhms = area_zjhms.intersect(zjhms).dropDuplicates()
            check_exists_res(zjhms, 'called_area exit!!')


        min_call_times = build_cond('num','min_call_times','>=',phone_args)
        max_call_times = build_cond('num','max_call_times','<=',phone_args)
        min_call_duration = build_cond('call_duration','min_call_duration','>=',phone_args)
        max_call_duration = build_cond('call_duration','max_call_duration','<=',phone_args)
        cond_group = union_cond(' and ',min_call_times,max_call_times,min_call_duration,max_call_duration)

        if cond_group:
            '''通话总次数和通话总时间筛选'''
            logger.info('cond_group %s'%cond_group)
            sql = '''
                select zjhm, count(call_duration) as num, sum(call_duration) as call_duration 
                from tmp where zjhm is not null group by zjhm
            '''
            if 'zjhm' in [col[0] for col in call_info.dtypes]:
                call_info.createOrReplaceTempView('tmp')
                call_info = spark.sql(sql)
            else:
                cols1 = ['start_person zjhm','call_duration']
                cols2 = ['end_person zjhm','call_duration']
                call_info.selectExpr(*cols1).unionAll(call_info.selectExpr(*cols2)).createOrReplaceTempView('tmp')
                call_info = spark.sql(sql)
            zjhms = call_info.filter(cond_group).selectExpr('zjhm').distinct()
            check_exists_res(zjhms,'call_total_times_duration exit!!')
        call_info.unpersist()

    if check_all_conds(labels_args):
        '''标签组合判断'''
        df = read_orc(add_save_path('person_labels',root='xxx'))
        find_type = labels_args.pop('find_type')
        labels_index = [labels_dict[_] for _ in sorted(labels_args.pop('labels').split(','))]
        res = []
        for i in labels_index:
            res.append('substr(labels,%s,1)'%str(i))
        # 组合判断条件
        label_cond = 'filter_labels(concat(%s),%s)'%(','.join(res),find_type)
        label_zjhm = df.filter(label_cond).select('zjhm').distinct()
        zjhms = zjhms.intersect(label_zjhm)
        check_exists_res(zjhms,'person_labels exit!!')

    res = zjhms.withColumn('task_id',lit(task_id)).limit(2000)
    res = res.withColumnRenamed('zjhm', 'node_num')

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
    person_filter()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))