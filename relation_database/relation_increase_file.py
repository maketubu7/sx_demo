# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 15:35
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : call_msg.py
# @Software: PyCharm

import logging
import os
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import random
from collections import OrderedDict

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

conf=SparkConf().set('spark.driver.maxResultSize', '10g')
conf.set('spark.executor.memoryOverhead', '5g')
conf.set('spark.driver.memory','20g')
# conf.set('spark.sql.shuffle.partitions',800)
# conf.set('spark.default.parallelism',800)
conf.set('spark.executor.memory', '25g')
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
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/call_msg'
save_root = 'relation_incrdir'
zj_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/zj_location.csv'
sj_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/sj_location.csv'

##todo:all
def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix, 'parquet/', path))
    return df


def write_parquet(df, path):
    '''写 parquet'''
    df.write.mode("overwrite").parquet(path=os.path.join(path_prefix, 'parquet/', path))


def write_csv(df, path):
    '''写 parquet'''
    df.write.mode("overwrite").csv(path=os.path.join(path_prefix, 'csv/', path), sep='\t', header=None)


def write_orc(df, path, mode='overwrite'):
    df.write.format('orc').mode(mode).save(path)
    logger.info('write success')

def read_orc(path):
    try:
        df = spark.read.orc(path)
        return df
    except:
        return None


def add_save_jg_path(tablename,standby=False):
    ## jg文件保存地址
    if standby:
        return '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_data_standby/{}'.format(tablename.lower()+"_jg")
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_data/{}'.format(tablename.lower()+"_jg")


def filter_service_phone(df):

    schema = StructType([
        StructField("phone", StringType(), True),
    ])

    service_phone = spark.read.csv('/phoebus/_fileservice/users/slmp/shulianmingpin/service_phones.csv',schema=schema)
    df.createOrReplaceTempView('call')
    service_phone.createOrReplaceTempView('service')

    sql = ''' select * from call where (select count(1) as num from service where call.end_phone = service.phone) = 0
            and (select count(1) as num from service where call.start_phone = service.phone) = 0 '''
    res = spark.sql(sql)
    logger.info('filter success')
    return res



def relation_traffic():
    '''  每天的交通出行增量文件 '''

    ## 乘坐火车
    cols = ['sfzh', 'trianid', 'cc', 'fcrq', 'cxh', 'zwh', 'fz', 'dz','fcrq start_time','fcrq+18000 end_time']
    df1 = read_orc(add_incr_path('edge_person_reserve_trainline_detail',cp=cp))
    df2 = read_orc(add_save_path('edge_person_enter_trainline_detail',cp=cp))
    dfs = filter(lambda a: a, [df1,df2] )
    if dfs:
        train_df = reduce(lambda a,b:a.union(b),dfs).selectExpr(*cols).dropDuplicates(['sfzh','trianid'])
        write_orc(train_df,add_save_path('edge_person_reserve_trainline_jg',cp=cp,root=save_root))
        logger.info('edge_person_reserve_trainline_day down')
        ## 火车节点
        v_cols = ['trianid trainid','cc','fcrq','fz sfd','dz mdd']
        vertex_train = train_df.selectExpr(*v_cols).dropDuplicates(['trainid'])
        write_orc(vertex_train,add_save_path('vertex_trainline',cp=cp,root=save_root))

    ##乘坐飞机
    files = ['edge_person_checkin_airline_detail','edge_person_reserve_airline_detail','edge_person_arrived_airline_detail']

    cols = ["sfzh", "airlineid", "hbh", "hbrq", "sfd", "mdd","yjqfsj","yjddsj","tablename"]
    dfs = []
    for file in files:
        df = read_orc(add_incr_path(file,cp=cp))
        if df:
            df = df.where('valid_hbh(hbh)=1').selectExpr(*cols)
            dfs.append(df)
    exprs = ['*','case tablename when "ods_soc_civ_avia_rese" then "订票" when "ods_soc_civ_avia_leaport_data" then "安检" else "到港" end as type_name ']
    if dfs:
        reduce(lambda a,b:a.unionAll(b),dfs).selectExpr(*exprs).createOrReplaceTempView('tmp')
        sql = '''
            select sfzh,airlineid,max(hbh) hbh, max(hbrq) hbrq, concat_ws("|",collect_set(type_name)) type_name, 
            max(sfd) sfd, max(mdd) mdd,max(yjqfsj) yjqfsj, max(yjddsj) yjddsj , max(yjqfsj) start_time, 
            max(yjddsj) end_time from tmp group by sfzh,airlineid
        '''
        res = spark.sql(sql)
        write_orc(res,add_save_path('edge_person_reserve_airline_jg',cp=cp,root=save_root))
        logger.info('edge_person_reserve_airline down')

    ## 航班节点
    v_dfs = []
    for index,file in enumerate(files):
        df = read_orc(add_incr_path(file,cp=cp))
        if df and df.take(1):
            df = df.withColumn('table_sort',lit(index+1))
            df.createOrReplaceTempView("tmp_view")
            sql1 = ''' select airlineid,hbh,hbrq,yjqfsj,sfd,table_sort,tablename,
                                row_number() over(partition by airlineid order by yjqfsj asc) as num from tmp_view where valid_hbh(hbh) = 1 '''
            sql2 = ''' select airlineid airline_id2,yjddsj,mdd,
                                row_number() over(partition by airlineid order by yjddsj desc) as num from tmp_view where valid_hbh(hbh) = 1 '''
            df1 = spark.sql(sql1).where('num=1').drop('num')
            df2 = spark.sql(sql2).where('num=1').drop('num')
            df3 = df1.join(df2, df1.airlineid == df2.airline_id2, 'left') \
                .select(df1.airlineid, df1.hbh, df1.hbrq, df1.yjqfsj, df2.yjddsj, df1.sfd, df2.mdd, df1.tablename,df1.table_sort)
            v_dfs.append(df3)
    if v_dfs:
        union_df = reduce(lambda x,y:x.unionAll(y),v_dfs)
        union_df.createOrReplaceTempView("tmp_view")
        sql = ''' select airlineid,hbh,hbrq,yjqfsj,yjddsj,sfd,mdd,tablename,
                    row_number() over(partition by airlineid order by table_sort asc) as num from tmp_view '''

        res = spark.sql(sql).where('num=1').drop('num')
        write_orc(res, add_save_path('vertex_airline',cp=cp,root=save_root))
        logger.info('vertex_airline %s down'%cp)


def relation_internet():
    ''' 每天的上网关系 '''
    df = read_orc(add_incr_path('edge_person_surfing_internetbar_detail',cp=cp))
    if df and df.take(1):
        df.createOrReplaceTempView('tmp')
        sql = '''
            select sfzh,siteid,min(start_time) start_time, max(end_time) end_time,
            count(1) as num from tmp group by sfzh,siteid
        '''
        df = spark.sql(sql)
        write_orc(df,add_save_path('edge_person_surfing_internetbar_jg',cp=cp,root=save_root))
        logger.info('edge_person_surfing_internetbar down')


def relation_call_msg():
    ''' 每天的通话 短信 '''

    ## 通话
    cols = ['start_phone','end_phone','start_time','end_time','call_duration','homearea','relatehomeac']
    df = read_orc(add_incr_path('edge_groupcall_detail',cp=cp)).selectExpr(*cols)
    if df and df.take(1):
        df.createOrReplaceTempView('call_tmp')
        sql = '''
        select start_phone,end_phone,min(start_time) start_time,
        max(end_time) end_time, sum(call_duration) call_total_duration , 
        count(1) call_total_times from call_tmp group by start_phone, end_phone
    '''
        write_orc(spark.sql(sql),add_save_path('edge_groupcall_jg',cp=cp,root=save_root))
        logger.info('edge_groupcall %s down'%cp)

    ## 短信
    cols = ['start_phone','end_phone','start_time','end_time','message','homearea','relatehomeac']
    df = read_orc(add_incr_path('edge_groupmsg_detail',cp=cp)).selectExpr(*cols)
    if df and df.take(1):
        filter_service_phone(df).createOrReplaceTempView('msg_tmp')
        sql = '''
            select format_phone(start_phone) start_phone, format_phone(end_phone) end_phone,min(start_time) start_time,
            max(end_time) end_time,count(*) message_number from msg_tmp group by start_phone,end_phone 
            '''
        write_orc(spark.sql(sql), add_save_path('edge_groupmsg_jg',cp=cp,root=save_root))
        logger.info('edge_groupmsg %s down'%cp)


def relation_hotel():
    df = read_orc(add_incr_path('edge_person_stay_hotel_detail',cp=cp))
    sql = '''
        select sfzh,lgdm, min(start_time) as start_time, max(end_time) as end_time, count(sfzh) as num 
        from edge_person_stay_hotel_detail group by sfzh,lgdm
    '''
    if df and df.take(1):
        df.createOrReplaceTempView('edge_person_stay_hotel_detail')
        res = spark.sql(sql)
        write_orc(res,add_save_path('edge_person_stay_hotel_jg',cp=cp,root=save_root))

def vertex_phone():
    phone_tables = [
        {'tablename':'edge_groupcall','phone':'start_phone'},
        {'tablename':'edge_groupcall','phone':'end_phone'},
        {'tablename':'edge_groupmsg','phone':'start_phone'},
        {'tablename':'edge_groupmsg','phone':'end_phone'},
    ]
    tmp_sql = ''' select {phone} phone from {tablename} '''

    dfs = []
    for info in phone_tables:
        sql = tmp_sql.format(**info)
        create_tmpview_table(spark, info['tablename'],cp=cp,root=save_root)
        df = spark.sql(sql)
        dfs.append(df)

    union_df = reduce(lambda x, y: x.unionAll(y), dfs)

    union_df = union_df.drop_duplicates(['phone'])

    df = union_df.selectExpr('phone','substr(phone,1,7) as area',
                             'substr(phone,1,1) as pre1','substr(phone,1,3) as pre3',
                             'substr(phone,1,3) as pre4')

    sj_location = spark.read.csv(sj_path,header=True,sep='\t')
    df_sj = df.where('pre1 != "0"')
    df_sj_res = df_sj.join(sj_location, df_sj.area == sj_location.pre7, 'left') \
        .select(df_sj.phone, sj_location.province, sj_location.city)
    write_parquet(df_sj_res,'df_sj_res')

    # 座机的数据-010 020 021 022 023 024 025 027 028 029  3位区号的
    df_zj1 = df.where("pre1=='0' and pre3 in('010','020','021','022','023','024','025','027','028','029')")

    zj_location = spark.read.csv(zj_path,header=True,sep='\t')

    df_zj1_res = df_zj1.join(zj_location, df_zj1.pre3 == zj_location.qhao, 'left') \
        .select(df_zj1.phone, zj_location.province, zj_location.city)
    df_zj1_res = df_zj1_res.na.fill({'province': '', 'city': ''})

    write_parquet(df_zj1_res, 'df_zj1_res')
    logger.info(' df_zj1_res down')

    # 座机4位区号的
    df_zj2 = df.where("pre1=='0' and pre3 not in('010','020','021','022','023','024','025','027','028','029')")
    df_zj2_res = df_zj2.join(zj_location, df_zj2.pre4 == zj_location.qhao, 'left') \
        .select(df_zj2.phone, zj_location.province, zj_location.city)
    df_zj2_res = df_zj2_res.na.fill({'province': '', 'city': ''})
    write_parquet(df_zj2_res, 'df_zj2_res')
    logger.info(' df_zj2_res down')

    df_zj2_res = read_parquet('df_zj2_res')
    df_zj1_res = read_parquet('df_zj1_res')
    df_sj_res = read_parquet('df_sj_res')

    res_tmp = df_sj_res.unionAll(df_zj1_res).unionAll(df_zj2_res).drop_duplicates(['phone'])
    smz = read_orc(add_save_path('edge_person_smz_phone_top',root='relation_theme_extenddir'))

    ## 添加实名制姓名
    res_tmp = res_tmp.join(smz,res_tmp.phone==smz.end_phone,'left') \
            .selectExpr('phone','start_person','province','city')

    write_parquet(res_tmp,'res_tmp')
    cols = ['phone','"" xm','province','city']
    person = read_orc(add_save_path('vertex_person',root='relation_theme_extenddir'))
    res1 = read_parquet('res_tmp').where('start_person is null').selectExpr(*cols)
    res_tmp = read_parquet('res_tmp').where('start_person is not null')

    res2 = res_tmp.join(person,res_tmp.start_person==person.zjhm,'left') \
            .selectExpr('phone','xm','province','city').na.fill({'xm':''})

    res = res1.unionAll(res2).drop_duplicates(['phone'])

    write_orc(res,add_save_path('vertex_phonenumber',cp=cp,root=save_root))
    logger.info('vertex_phonenumber %s down'%cp)

def vertex_person():

    person_tables = ['ods_soc_civ_avia_leaport_data','ods_soc_civ_avia_rese','ods_soc_traf_raista_entper',
                     'ods_soc_traf_raiway_saltic_data','ods_pol_sec_netbar_intper_info']

    dfs = []
    for file in person_tables:
        df = read_orc(add_save_path(file,cp=cp,root=save_root))
        dfs.append(df)

    dfs = filter(lambda a:a,dfs)
    if dfs:
        res = reduce(lambda a,b:a.unionAll(b),dfs).dropDuplicates(['zjhm'])
        write_orc(res,add_save_path('vertex_person',cp=cp,root=save_root))

def create_vertex_jg():

    def get_table_index(tablename):
        table_index = {
            'vertex_person': 0,	# 人 40亿空间
            'vertex_airline': 100000000,	# 航班班次' 10亿
            'vertex_trainline': 200000000,	# 火车班次' 10亿
            'vertex_phonenumber':300000000,	# 电话 20亿空间 永远在最后，增长最快
        }
        return table_index[tablename]

    vertex_table_info = OrderedDict()
    vertex_table_info['vertex_person'] = ['zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                                      'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz']
    vertex_table_info['vertex_phonenumber'] = ['phone', 'xm','province', 'city']
    vertex_table_info['vertex_airline'] = ['airlineid', 'hbh', 'hbrq', 'yjqfsj', 'yjddsj', 'sfd', 'mdd']
    vertex_table_info['vertex_trainline'] = ['trianid', 'cc', 'fcrq']

    def map_rdd(data):
        ret = []
        row, index = data
        cur_timestamp = int(time.time())
        jid = int(str(random.randint(91, 99)) + str(cur_timestamp + table_index) + str(index))
        ret.append(jid)
        row = list(row)
        for item in row:
            ret.append(item)
        return tuple(ret)
    vertex_zd = ['jid']
    for tablename in vertex_table_info:
        logger.info('dealing %s' % tablename)
        source_table_info = vertex_table_info[tablename]
        table_index = get_table_index(tablename)
        df = read_orc(add_save_path(tablename)).selectExpr(*source_table_info)
        rdd = df.rdd.zipWithIndex().map(map_rdd)
        new_schema = vertex_zd + source_table_info
        res = spark.createDataFrame(rdd, new_schema)
        write_orc(res, add_save_path(tablename+"_jg",root=save_root))
        logger.info('%s_jg down' % tablename)

def get_history():
    start_cp = ''
    end_cp = ''
    start_date = datetime.strptime(start_cp,'%Y%m%d00')
    end_date = datetime.strptime(end_cp,'%Y%m%d00')
    num = (end_date - start_date).days
    cps = [(start_date + timedelta(days=_)).strftime('%Y%m%d00') for _ in range(num)][::-1]
    for cp in cps:
        df = read_orc(add_incr_path("edge_groupmsg_detail", cp=cp))
        if df:
            df = df.coalesce(100)
            write_orc(df,add_incr_path('edge_groupmsg_detail_compact',cp=cp))
            logger.info('edge_groupmsg_detail_compact %s success')


if __name__ == "__main__":
    ''' 处理t-1的增量关系文件 每天的任务 '''
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    cp = sys.argv[1]

    relation_hotel()
    relation_traffic()
    relation_internet()
    relation_call_msg()
    vertex_phone()
    vertex_person()
    create_vertex_jg()



logger.info('========================end time:%s==========================' % (
    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
