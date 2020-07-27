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

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

conf=SparkConf().set('spark.driver.maxResultSize', '10g')
conf.set('spark.executor.memoryOverhead', '5g')
conf.set('spark.driver.memory','20g')
conf.set('spark.sql.shuffle.partitions',800)
conf.set('spark.default.parallelism',800)
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
    # df = spark.read.orc(path)
    # return df

def drop_duplicated_call(df):
    """
    电话去重
    :param df:
    :return:
    """

    def make_tuple(item):
        return (item.start_phone, item.end_phone, item.start_time, item.end_time, item.call_duration, item.homearea,
                item.relatehomeac, item.start_lac,item.start_lic,item.start_lon,item.start_lat,
                item.end_lac, item.end_lic, item.end_lon, item.end_lat,item.tablename)

    def distinct_call(frame):
        key, rows = frame
        rows = sorted(rows, key=lambda x: x.start_time)
        items = [make_tuple(rows[0])]
        last_one = rows[0]
        for info in rows[1:]:
            if info.start_time > last_one.end_time:  # 忽略掉时间有交叉的数据
                items.append(make_tuple(info))
                last_one = info
        return items
    cols = ["start_phone", "end_phone", "start_time", "end_time", "call_duration", "homearea",
                                  "relatehomeac", "start_lac","start_lic","start_lon","start_lat","end_lac","end_lic",
                                    "end_lon","end_lat","tablename"]
    df_select = df.selectExpr(*cols)
    rdd = df_select.rdd.map(lambda x: ((x.start_phone, x.end_phone), x)).groupByKey().flatMap(distinct_call)
    return spark.createDataFrame(rdd,cols)


def drop_duplicated_msg(df):
    """
    短信去重
    :param df:
    :return:
    """
    msg_schema = ["start_phone", "end_phone", "start_time", "end_time", "send_time", "message",
                    "homearea","relatehomeac","start_lac","start_lic","start_lon","start_lat","end_lac","end_lic",
                    "end_lon","end_lat", "tablename"]
    def make_tuple(item):
        return (
            item.start_phone, item.end_phone, item.start_time, item.end_time, item.send_time, item.message,
            item.homearea,item.relatehomeac, item.start_lac,item.start_lic,item.start_lon,item.start_lat,
                item.end_lac, item.end_lic, item.end_lon, item.end_lat,item.tablename)

    def distinct(frame):
        key, rows = frame
        rows = sorted(rows, key=lambda x: x.start_time)
        items = [make_tuple(rows[0])]
        last_one = rows[0]
        for item in rows[1:]:
            if item.start_time - last_one.start_time > 60:  # 忽略内容相同且时间差小于1min的数据
                items.append(make_tuple(item))
                last_one = item
        return items

    df_select = df.selectExpr(*msg_schema)
    message_rdd = df_select.rdd.map(lambda x: ((x.start_phone, x.end_phone, x.message), x)).groupByKey().flatMap(distinct)
    if message_rdd.take(1):
        return spark.createDataFrame(message_rdd,msg_schema)
    return None


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




def dc_edge_groupcall_source():
    '''每天各个表的通话明细'''
    tmp_sql = '''
            select 
                   format_phone({start_phone}) as start_phone, 
                   format_phone({end_phone}) as end_phone, 
                   {begintime} as start_time,
                   {endtime} as end_time,
                   {callduration} as call_duration,
                   trim({homearea}) as homearea,
                   trim({relatehomeac}) as relatehomeac,
                   trim({start_lac}) as start_lac,
                   trim({start_lic}) as start_lic,
                   trim({start_lon}) as start_lon,
                   trim({start_lat}) as start_lat,
                   trim({end_lac}) as end_lac,
                   trim({end_lic}) as end_lic,
                   trim({end_lon}) as end_lon,
                   trim({end_lat}) as end_lat,
                   '{table_name}' as tablename
             from  {table_name}
            where {phone_condittion}
              and {callduration} > 0 
              and {calltype_condition}
        '''

    # 语音固话 ods_pol_sec_fixlin_voic_busi
    table_list = [
        {'start_phone':''' case when call_type_code = '1' then charg_ctct_tel else  oppos_ctct_tel end ''',
        'end_phone':''' case when call_type_code = '1' then oppos_ctct_tel else charg_ctct_tel end ''',
        'homearea':'''case when call_type_code = '1' then main_call_no_ctct_tel_arcod else  be_call_no_ctct_tel_arcod end ''',
        'relatehomeac':'''case when call_type_code = '1' then be_call_no_ctct_tel_arcod else main_call_no_ctct_tel_arcod end ''',
        'begintime':'cast(star_time as bigint)',
        'callduration':'cast (talk_time as bigint)',
        'endtime':'cast(star_time as bigint) + cast(talk_time as bigint)',
        'start_lac': ''' ''  ''',
        'start_lic': ''' '' ''',
        'start_lon': ''' ''  ''',
        'start_lat': ''' '' ''',
        'end_lac': ''' ''  ''',
        'end_lic': ''' ''  ''',
        'end_lon': ''' ''  ''',
        'end_lat': ''' ''  ''',
        'calltype_condition':'''(call_type_code = '1' or call_type_code = '2') ''',
        'phone_condittion':''' verify_phonenumber(charg_ctct_tel)=1 and verify_phonenumber(oppos_ctct_tel)=1
                              and format_phone(charg_ctct_tel) != format_phone(oppos_ctct_tel) ''',
        'table_name':'ods_pol_sec_fixlin_voic_busi'},

        ## 1
        {'start_phone':''' case when call_type_code = '1' then charg_ctct_tel else  oppos_ctct_tel end ''',
        'end_phone':''' case when call_type_code = '1' then oppos_ctct_tel else charg_ctct_tel end ''',
        'homearea':'''case when call_type_code = '1' then main_call_no_ctct_tel_arcod else  be_call_no_ctct_tel_arcod end ''',
        'relatehomeac':'''case when call_type_code = '1' then be_call_no_ctct_tel_arcod else main_call_no_ctct_tel_arcod end ''',
        'begintime':'cast(star_time as bigint)',
        'callduration':'cast (talk_time as bigint)',
        'endtime':'cast(star_time as bigint) + cast(talk_time as bigint)',
        'start_lac':''' case when call_type_code = '1' then charg_curr_loc_area_code else oppos_curr_loc_area_code end  ''',
        'start_lic':''' case when call_type_code = '1' then charg_curr_houest_idecod else oppos_curr_houest_idecod end  ''',
        'start_lon':''' case when call_type_code = '1' then charg_curr_lon else oppos_curr_lon end  ''',
        'start_lat':''' case when call_type_code = '1' then charg_curr_lat else oppos_curr_lat end  ''',
        'end_lac': ''' case when call_type_code = '2' then charg_curr_loc_area_code else oppos_curr_loc_area_code end  ''',
        'end_lic': ''' case when call_type_code = '2' then charg_curr_houest_idecod else oppos_curr_houest_idecod end  ''',
        'end_lon': ''' case when call_type_code = '2' then charg_curr_lon else oppos_curr_lon end  ''',
        'end_lat': ''' case when call_type_code = '2' then charg_curr_lat else oppos_curr_lat end  ''',
        'calltype_condition':'''(call_type_code = '1' or call_type_code = '2') ''',
        'phone_condittion':''' verify_phonenumber(charg_ctct_tel)=1 and verify_phonenumber(oppos_ctct_tel)=1
                                  and format_phone(charg_ctct_tel) != format_phone(oppos_ctct_tel) ''',
        'table_name':'ods_pol_sec_gsm_voic_busi'},

        ## 1
        {'start_phone':''' case when call_type_code = '1' then charg_ctct_tel else  oppos_ctct_tel end ''',
        'end_phone':''' case when call_type_code = '1' then oppos_ctct_tel else charg_ctct_tel end ''',
        'homearea':'''case when call_type_code = '1' then charg_ctct_tel_arcod else  oppos_ctct_tel_arcod end ''',
        'relatehomeac':'''case when call_type_code = '1' then oppos_ctct_tel_arcod else charg_ctct_tel_arcod end ''',
        'begintime':'cast(star_time as bigint)',
        'callduration':'cast (talk_time as bigint)',
        'endtime':'cast(star_time as bigint) + cast(talk_time as bigint)',
        'start_lac': ''' case when call_type_code = '1' then charg_loc_area_code else oppos_loc_area_code end  ''',
        'start_lic': ''' case when call_type_code = '1' then charg_houest_idecod else oppos_houest_idecod end  ''',
        'start_lon': ''' case when call_type_code = '1' then charg_lon else oppos_lon end  ''',
        'start_lat': ''' case when call_type_code = '1' then charg_lat else oppos_lat end  ''',
        'end_lac': ''' case when call_type_code = '2' then charg_loc_area_code else oppos_loc_area_code end  ''',
        'end_lic': ''' case when call_type_code = '2' then charg_houest_idecod else oppos_houest_idecod end  ''',
        'end_lon': ''' case when call_type_code = '2' then charg_lon else oppos_lon end  ''',
        'end_lat': ''' case when call_type_code = '2' then charg_lat else oppos_lat end  ''',
        'calltype_condition':'''(call_type_code = '1' or call_type_code = '2') ''',
        'phone_condittion':''' verify_phonenumber(charg_ctct_tel)=1 and verify_phonenumber(oppos_ctct_tel)=1
                                       and format_phone(charg_ctct_tel) != format_phone(oppos_ctct_tel) ''',
        'table_name':'ods_pol_sec_vpmn_busi'},

        {'start_phone':''' case when zbj_bs = '65' then lxdh else  df_lxdh end ''',
        'end_phone':''' case when zbj_bs = '65' then df_lxdh else lxdh end ''',
        'homearea':'''case when zbj_bs = '65' then yh_gsd_dm else df_gsd_dm end ''',
        'relatehomeac':'''case when zbj_bs = '65' then df_gsd_dm else yh_gsd_dm end ''',
        'begintime':'cast(kssj as bigint)',
        'callduration':'cast (thsc as bigint)',
        'endtime':'cast(kssj as bigint) + cast(thsc as bigint)',
        'start_lac': ''' case when zbj_bs = '65' then th_ks1_wzq_dm else dd16_wzq_dm end  ''',
        'start_lic': ''' case when zbj_bs = '65' then th_ks1_xq4_bh else dd16_xq4_bh end  ''',
        'start_lon': ''' case when zbj_bs = '65' then th_ks1_jz2_dqjd else dd16_jz2_dqjd end  ''',
        'start_lat': ''' case when zbj_bs = '65' then th_ks1_jz2_dqwd else dd16_jz2_dqwd end  ''',
        'end_lac': ''' case when zbj_bs = '65' then th_ks1_wzq_dm else dd16_wzq_dm end  ''',
        'end_lic': ''' case when zbj_bs = '65' then th_ks1_xq4_bh else dd16_xq4_bh end  ''',
        'end_lon': ''' case when zbj_bs = '65' then th_ks1_jz2_dqjd else dd16_jz2_dqjd end  ''',
        'end_lat': ''' case when zbj_bs = '65' then th_ks1_jz2_dqwd else dd16_jz2_dqwd end  ''',
        'calltype_condition':'''(zbj_bs = '65' or zbj_bs = '66') ''',
        'phone_condittion':''' verify_phonenumber(lxdh)=1 and verify_phonenumber(df_lxdh)=1
                                           and format_phone(lxdh) != format_phone(df_lxdh) ''',
        'table_name':'ods_tb_cdr'},

        {'start_phone':''' case when call_type_code = '1' then charg_ctct_tel else  oppos_ctct_tel end ''',
        'end_phone':''' case when call_type_code = '1' then oppos_ctct_tel else charg_ctct_tel end ''',
        'homearea':'''case when call_type_code = '1' then main_call_no_ctct_tel_arcod else be_call_no_ctct_tel_arcod end ''',
        'relatehomeac':'''case when call_type_code = '1' then be_call_no_ctct_tel_arcod else main_call_no_ctct_tel_arcod end ''',
        'begintime':'cast(star_time as bigint)',
        'callduration':'cast (time_length as bigint)',
        'endtime':'cast(star_time as bigint) + cast(time_length as bigint)',
        'start_lac': ''' case when call_type_code = '1' then charg_loc_area_code else oppos_loc_area_code end  ''',
        'start_lic': ''' case when call_type_code = '1' then charg_houest_idecod else oppos_houest_idecod end  ''',
        'start_lon': ''' case when call_type_code = '1' then charg_lon else oppos_lon end  ''',
        'start_lat': ''' case when call_type_code = '1' then charg_lat else oppos_lat end  ''',
        'end_lac': ''' case when call_type_code = '2' then charg_loc_area_code else oppos_loc_area_code end  ''',
        'end_lic': ''' case when call_type_code = '2' then charg_houest_idecod else oppos_houest_idecod end  ''',
        'end_lon': ''' case when call_type_code = '2' then charg_lon else oppos_lon end  ''',
        'end_lat': ''' case when call_type_code = '2' then charg_lat else oppos_lat end  ''',
        'calltype_condition':'''(call_type_code = '1' or call_type_code = '2') ''',
        'phone_condittion':''' verify_phonenumber(charg_ctct_tel)=1 and verify_phonenumber(oppos_ctct_tel)=1
                                               and format_phone(charg_ctct_tel) != format_phone(oppos_ctct_tel) ''',
        'table_name':'ods_pol_sec_mobile_roam_busi'},
    ]

    for info in table_list:
        if info['table_name'] == tablename:
            init_dw_history(spark,tablename,import_times=import_times,if_write=False)
            logger.info(spark.sql('select * from %s'%tablename).dtypes)
            sql = tmp_sql.format(**info)
            df = spark.sql(sql)
            res = df.repartition(100)
            write_orc(res,add_incr_path(tablename+'_new',cp=cp))
            logger.info('%s %s down'% (tablename,cp))

def dc_edge_groupmsg_day():
    '''每天电围短信明细'''
    tmp_sql = '''
            select 
                   {start_phonenumber} as start_phone, 
                   {end_phonenumber} as end_phone,
                   {message} as message,
                   {homearea} as homearea,
                   {relatehomeac} as relatehomeac,
                   {begintime} start_time,
                   {sendtime} send_time,
                   {start_lac} start_lac,
                   {start_lic} start_lic,
                   {start_lon} start_lon,
                   {start_lat} start_lat,
                   {end_lac} end_lac,
                   {end_lic} end_lic,
                   {end_lon} end_lon,
                   {end_lat} end_lat,
                   '{table_name}' as tablename
             from  {table_name}
            where {phone_condition}
              and {calltype_condition}
        '''

    sql = tmp_sql.format(start_phonenumber=''' case when elefen_event_type_code = '49' then user_num else  oppos_num end ''',
                         homearea=''' case when elefen_event_type_code = '49' then user_homloc_ctct_tel_arcod else  oppos_homloc_ctct_tel_arcod end ''',
                         end_phonenumber=''' case when elefen_event_type_code = '49' then oppos_num else user_num end ''',
                         relatehomeac=''' case when elefen_event_type_code = '49' then oppos_homloc_ctct_tel_arcod else user_homloc_ctct_tel_arcod end ''',
                         message='trim(sms_cont)',
                         begintime='cast(start_time as bigint)',
                         sendtime='cast(start_time as bigint)',
                         start_lac = ''' case when elefen_event_type_code = '49' then loc_area_code else oppos_loc_area_code end ''',
                         start_lic = ''' case when elefen_event_type_code = '49' then bassta_houest_idecod else oppos_bassta_houest_idecod end ''',
                         start_lon = ''' case when elefen_event_type_code = '49' then bassta_lon else oppos_bassta_lon end ''',
                         start_lat = ''' case when elefen_event_type_code = '49' then bassta_lat else oppos_bassta_lat end ''',
                         end_lac=''' case when elefen_event_type_code = '50' then loc_area_code else oppos_loc_area_code end ''',
                         end_lic=''' case when elefen_event_type_code = '50' then bassta_houest_idecod else oppos_bassta_houest_idecod end ''',
                         end_lon=''' case when elefen_event_type_code = '50' then bassta_lon else oppos_bassta_lon end ''',
                         end_lat=''' case when elefen_event_type_code = '50' then bassta_lat else oppos_bassta_lat end ''',
                         calltype_condition='''(elefen_event_type_code = '49' or elefen_event_type_code = '50') and start_time is not null''',
                         phone_condition=''' verify_phonenumber(user_num) = 1 and verify_phonenumber(oppos_num) = 1
                                            and format_phone(user_num) != format_phone(oppos_num) ''',
                         table_name='ods_pol_pub_dw_sms')

    init_dw_history(spark, 'ods_pol_pub_dw_sms',import_times=import_times, if_write=False)
    msg_schema = ["start_phone", "end_phone", "start_time", "start_time+1 end_time", "send_time", "message",
                  "homearea", "start_lac", "start_lic", "start_lon", "start_lat", "end_lac", "end_lic",
                  "end_lon", "end_lat", "relatehomeac", "tablename"]
    df = spark.sql(sql).selectExpr(*msg_schema)
    res = drop_duplicated_msg(df)
    if res:
        res = res.repartition(100)
        write_orc(res, add_incr_path('edge_groupmsg_detail',cp=cp))


def edge_groupcall_detail():
    '''每天的明细文件夹汇总为每天的通联明细表'''
    detail_files = ['ods_pol_sec_gsm_voic_busi', 'ods_pol_sec_fixlin_voic_busi',
                    'ods_pol_sec_vpmn_busi', 'ods_tb_cdr', 'ods_pol_sec_mobile_roam_busi']

    for cp in cps:
        dfs = []
        for file in detail_files:
            df = read_orc(add_incr_path(file+'_new', cp=cp))
            dfs.append(df)
        if filter(lambda x: x, dfs):
            union_df = reduce(lambda x, y: x.unionAll(y), filter(lambda x: x, dfs))
            tmp_res = filter_service_phone(union_df)
            res = drop_duplicated_call(tmp_res).coalesce(100)
            write_orc(res, add_incr_path("edge_groupcall_detail", cp=cp))
            logger.info('edge_groupcall_detail %s down' % cp)

def oversea_call_detail():
    '''提取每天的境外通联数据明细'''
    for cp in cps:
        call = read_orc(add_incr_path('edge_groupcall_detail',cp=cp))
        if call and call.take(1):
            calling_cols = ['start_phone','end_phone','verify_oversea_phone(end_phone) oversea_country','1 call_type',
                            'start_time','end_time','call_duration']
            called_cols = ['start_phone', 'end_phone', 'verify_oversea_phone(start_phone) oversea_country', '2 call_type',
                            'start_time', 'end_time', 'call_duration']
            calling = call.where('verify_oversea_phone(end_phone) != ""').selectExpr(*calling_cols)
            called= call.where('verify_oversea_phone(start_phone) != ""').selectExpr(*called_cols)
            res = calling.unionAll(called).coalesce(100)
            if res.take(1):
                write_orc(res,add_incr_path('edge_oversea_groupcall',cp=cp))
            else:
                logger.info(u'未发现境外通联')

def dc_edge_groupcall_detail():
    '''整合必要字段为总的通话明细表'''
    cols = ['start_phone','end_phone','start_time','end_time','call_duration','homearea','relatehomeac']
    df = read_orc(add_incr_path('edge_groupcall_detail')).selectExpr(*cols)
    write_orc(df,add_save_path('edge_groupcall_detail'))
    logger.info('edge_groupcall_detail down')


def dc_edge_groupmsg_detail():
    '''整合必要字段为总的短信明细表'''
    cols = ['start_phone','end_phone','start_time','end_time','message','homearea','relatehomeac']
    df = read_orc(add_incr_path('edge_groupmsg_detail')).selectExpr(*cols)
    res = filter_service_phone(df)
    write_orc(res, add_save_path('edge_groupmsg_detail'))
    logger.info('edge_groupmsg_detail down')


def dc_edge_groupcall():
    '''
    合边的电话通联信息
    '''
    create_tmpview_table(spark,'edge_groupcall_detail')

    sql = '''
        select start_phone,end_phone,min(start_time) start_time,
        max(end_time) end_time, sum(call_duration) call_total_duration , 
        count(1) call_total_times from edge_groupcall_detail
        group by start_phone, end_phone
    '''

    df = spark.sql(sql)
    write_orc(df, add_save_path('edge_groupcall'))


def dc_edge_groupmsg():
    '''
    合边的短信通联信息
    '''
    create_tmpview_table(spark,'edge_groupmsg_detail')

    sql = '''
        select format_phone(start_phone) start_phone, format_phone(end_phone) end_phone,min(start_time) start_time,
        max(end_time) end_time,count(*) message_number from edge_groupmsg_detail group by start_phone,end_phone '''

    df = spark.sql(sql)
    write_orc(df, add_save_path('edge_groupmsg'))

def handle_data(tablename):

    if tablename == 'ods_pol_pub_dw_sms':
        dc_edge_groupmsg_day()
    else:
        dc_edge_groupcall_source()

def compact_par():
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
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    if len(sys.argv) == 4:
        import_times = sys.argv[1].split(',')
        cp = str(sys.argv[2])
        tablename = str(sys.argv[3])
        handle_data(tablename)
    elif len(sys.argv) == 2:
        cps = sys.argv[1].split(',')
        edge_groupcall_detail()
        oversea_call_detail()
        dc_edge_groupcall_detail()
        dc_edge_groupmsg_detail()
    else:
        dc_edge_groupmsg_detail()
        dc_edge_groupcall()
        dc_edge_groupmsg()



logger.info('========================end time:%s==========================' % (
    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
