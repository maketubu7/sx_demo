# -*- coding:utf-8 -*-
# Author: zhangxiao, zhaopiyan


import os
import sys
import time, copy, re, math
from pyspark import StorageLevel
from pyspark import SparkContext, HiveContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime, timedelta, date
import logging
import json
from collections import Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
conf = SparkConf()

conf = SparkConf().set('spark.driver.maxResultSize', '10g')
conf.set('spark.yarn.am.cores', 4)
conf.set('spark.executor.memoryOverhead', '15g')
conf.set('spark.executor.memory', '15g')
conf.set('spark.executor.instances', 100)
conf.set('spark.executor.cores', 4)
conf.set('spark.default.parallelism', 1200)
conf.set('spark.sql.shuffle.partitions', 1200)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')

# sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .appName("vertex") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()


# tmp_dir = '/user/bbd/dev/zhangxiao/all_phone_csv'


def write_csv(df, path, header=False, delimiter='\t'):
    df.write.mode('overwrite').csv(os.path.join(tmp_dir, path), header=header, sep=delimiter, quote='"', escape='"')
    return


def write_orc(df, path):
    df.write.mode("overwrite").format("orc").save(path)
    return


# v1.1creat_table
def create_table1(file_path, time_delta, now=date.today()):
    rolling_back_days = timedelta(time_delta)
    week_ago = now - rolling_back_days
    str_week_ago = week_ago.strftime("%Y-%m-%d")
    datelist = []
    for i in range(time_delta):
        datelist.append(now - timedelta(i + 1))
    datelist = map((lambda x: x.strftime("%Y-%m-%d")), datelist)
    datelist = [x for x in datelist if x > '2020-04-23']  # del weak data day
    filepathlist = map((lambda x: file_path + "cp=" + x.replace('-', '') + "00"), datelist)
    # 当遇到由于时间造成的问题时，考虑下面解决方案替代。
    # spark.read.orc(filepathlist).createOrReplaceTempView('table')
    dfs = []
    for a_path in filepathlist:
        try:
            df = spark.read.orc(a_path)
        except:
            df = None
        if df:
            dfs.append(df)

    reduce(lambda a, b: a.union(b), dfs).createOrReplaceTempView('table')

    return str_week_ago


def timestamp_2_str(timestamp):
    d = date.fromtimestamp(int(timestamp))
    return d.strftime("%Y-%m-%d")


def timestamp_2_str_hour(timestamp):
    d = datetime.fromtimestamp(int(timestamp))
    return d.strftime("%H")


def whether_weekday(timestamp):
    d = datetime.fromtimestamp(int(timestamp)).weekday()
    if d < 5:
        return 1
    else:
        return 0


def process_location(lon, lat):
    point_index_lon = lon.index('.')
    if len(lon[point_index_lon + 1:]) >= 4:
        lon_truncate = lon[:point_index_lon + 1 + 4]
    else:
        diff = 4 - len(lon[point_index_lon + 1:])
        lon_truncate = lon + diff * '0'
    point_index_lat = lat.index('.')
    if len(lat[point_index_lat + 1:]) >= 4:
        lat_truncate = lat[:point_index_lat + 1 + 4]
    else:
        diff = 4 - len(lat[point_index_lat + 1:])
        lat_truncate = lat + diff * '0'
    return lon_truncate + lat_truncate


def count_geo(row):
    key, data = row
    counter = {v: 0 for v in data}
    for v in data:
        counter[v] += 1
    ll = sorted(counter.items(), key=lambda x: x[1])
    geo = ll[-1][0]
    return [(key[0], geo)]


def hav(theta):
    s = sin(theta / 2)
    return s * s


def get_distance(lon0, lat0, lon1, lat1):
    earth_radius = 6371.393
    lon1 = toRadians(lon1)
    lat1 = toRadians(lat1)
    dlon = toRadians(lon0 - lon1)
    dlat = toRadians(lat0 - lat1)
    h = hav(dlat) + cos(lat0) * cos(lat1) * hav(dlon)
    distance = 2 * earth_radius * asin(sqrt(h))
    return distance


def geohash_encode(lon, lat, precision=12):
    try:
        lon = eval(lon)
        lat = eval(lat)
    except:
        pass
    base32list = '0123456789bcdefghjkmnpqrstuvwxyz'
    decodemap = {}
    for i in range(len(base32list)):
        decodemap[base32list[i]] = i
    del i
    lat_interval, lon_interval = (-90.0, 90.0), (-180.0, 180.0)
    geohash = []
    bits = [16, 8, 4, 2, 1]
    bit = 0
    ch = 0
    even = True
    while len(geohash) < precision:
        if even:
            mid = (lon_interval[0] + lon_interval[1]) / 2
            if lon > mid:
                ch |= bits[bit]
                lon_interval = (mid, lon_interval[1])
            else:
                lon_interval = (lon_interval[0], mid)
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2
            if lat > mid:
                ch |= bits[bit]
                lat_interval = (mid, lat_interval[1])
            else:
                lat_interval = (lat_interval[0], mid)
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash += base32list[ch]
            bit = 0
            ch = 0
    return ''.join(geohash)


def count_near_geo(geo1, geo2):
    if geo1 == geo2:
        result = 0.0
    else:
        result = 1.0
    return result


def uniphonestr_sql(phone1, phone2):
    if phone1 < phone2:
        result = str(phone1) + 't' + str(phone2)
    else:
        result = str(phone2) + 't' + str(phone1)
    return result


def uniphonestr_rdd(phone1, phone2):
    if phone1 < phone2:
        result = str(phone1) + 't' + str(phone2)
    else:
        result = str(phone2) + 't' + str(phone1)
    return result


spark.udf.register('timestamp_2_str', timestamp_2_str, StringType())
spark.udf.register('timestamp_2_str_hour', timestamp_2_str_hour, StringType())
spark.udf.register('whether_weekday', whether_weekday, StringType())
spark.udf.register("process_location", process_location, StringType())
spark.udf.register('count_geo', count_geo, StringType())
spark.udf.register('hav', hav, FloatType())
spark.udf.register('get_distance', get_distance, FloatType())
spark.udf.register('geohash_encode', geohash_encode, StringType())
spark.udf.register('uniphonestr_sql', uniphonestr_sql, StringType())
count_near_geo = udf(count_near_geo, FloatType())
uniphonestr_rdd = udf(uniphonestr_rdd, StringType())
# spark.udf.register('count_near_geo',count_near_geo, FloatType())


def create_table2(file_path, selected_list, time_delta_past, time_delta_future, now=date.today()):
    # rolling_back_days = timedelta(time_delta_past)
    # rolling_in_days = timedelta(time_delta_future)
    # week_ago = now - rolling_back_days
    # week_in = now + rolling_in_days
    # str_week_ago = week_ago.strftime("%Y-%m-%d")
    # 上面的应该用不到了。
    datelist = [now]
    if time_delta_past != 0:
        for i in range(time_delta_past):
            datelist.append(now - timedelta(i + 1))
    if time_delta_future != 0:
        for i in range(time_delta_future):
            datelist.append(now + timedelta(i + 1))

    datelist = map((lambda x: x.strftime("%Y-%m-%d")), datelist)
    datelist = [x for x in datelist if x > '2020-04-23']  # del weak data day
    filepathlist = map((lambda x: file_path + "cp=" + x.replace('-', '') + "00"), datelist)
    # 当遇到由于时间造成的问题时，考虑下面解决方案替代。
    # spark.read.orc(filepathlist).createOrReplaceTempView('table')
    dfs = []
    for a_path in filepathlist:
        try:
            df = spark.read.orc(a_path)
            df = df.selectExpr(selected_list)
            # df.persist(StorageLevel.MEMORY_AND_DISK)
        except:
            df = None
        if df:
            dfs.append(df)

    res = reduce(lambda a, b: a.union(b), dfs)
    return res


"""
主计算函数
"""


# 大量主叫
def huge_start_call(file_path, index_pasttime=0, index_futuertime=5, cpt_dt=date.today(), select_list=['start_phone', 'start_time']):
    cpt_dt -= timedelta(index_pasttime)
    str_week_ago = cpt_dt.strftime("%Y-%m-%d")
    tmp_df = create_table2(file_path, select_list, index_pasttime, index_futuertime, now=cpt_dt)
    tmp_df.persist(StorageLevel.MEMORY_AND_DISK)
    tmp_df.createOrReplaceTempView('table')
    sql = """select start_phone, count(1) call_cnt from table where timestamp_2_str(start_time)='{cp}' group by start_phone""".format(cp=str_week_ago)
    df = spark.sql(sql)
    result = df.filter("call_cnt>=80")
    cpt_dt_str = cpt_dt.strftime("%Y-%m-%d")
    result = result.withColumn("date", lit(cpt_dt_str))
    tmp_df.unpersist()
    return result


# 大量主叫且主叫比值较高
def huge_start_call_with_ratio(file_path, index_pasttime=0, index_futuertime=5, cpt_dt=date.today(), select_list=['start_phone', 'end_phone', 'start_time']):
    str_week_ago = cpt_dt.strftime("%Y-%m-%d")
    tmp_df = create_table2(file_path, select_list, index_pasttime, index_futuertime, now=cpt_dt)
    tmp_df.persist(StorageLevel.MEMORY_AND_DISK)
    tmp_df.createOrReplaceTempView('table')
    sql1 = """select start_phone phone, count(1) start_call_cnt from table where timestamp_2_str(start_time)='{cp}' group by start_phone""".format(cp=str_week_ago)
    df1 = spark.sql(sql1)
    sql2 = """select end_phone phone, count(1) end_call_cnt from table where timestamp_2_str(start_time)='{cp}' group by end_phone""".format(cp=str_week_ago)
    df2 = spark.sql(sql2)
    result1 = df1.filter("start_call_cnt>=50").join(df2,  ['phone'],  'left').na.fill(0)
    contribs = udf(lambda x, y: x/(y+1e-5),  FloatType())
    result1 = result1.select("phone", "start_call_cnt", contribs(result1['start_call_cnt'], result1['end_call_cnt']).alias('ratio'))
    result = result1.filter("ratio>3")
    cpt_dt_str = cpt_dt.strftime("%Y-%m-%d")
    result = result.withColumn("date", lit(cpt_dt_str))
    tmp_df.unpersist()
    return result


# 大量主叫且也有大量被叫
def huge_start_call_end_call(file_path, index_pasttime=0, index_futuertime=5, cpt_dt=date.today(), select_list=['start_phone', 'end_phone', 'start_time']):
    """
    近1天，主叫次数大于35，且被叫次数大于25。
    :param file_path:
    :param index_pasttime:
    :param cpt_dt:
    :return:
    """
    cpt_dt -= timedelta(index_pasttime)
    str_week_ago = cpt_dt.strftime("%Y-%m-%d")
    tmp_df = create_table2(file_path, select_list, index_pasttime, index_futuertime, now=cpt_dt)
    tmp_df.persist(StorageLevel.MEMORY_AND_DISK)
    tmp_df.createOrReplaceTempView('table')
    sql1 = """select start_phone phone, count(1) start_call_cnt from table where timestamp_2_str(start_time)='{cp}' group by start_phone""".format(cp=str_week_ago)
    df1 = spark.sql(sql1)
    sql2 = """select end_phone phone, count(1) end_call_cnt from table where timestamp_2_str(start_time)='{cp}' group by end_phone""".format(cp=str_week_ago)
    df2 = spark.sql(sql2)
    result = df1.filter("start_call_cnt>=35").join(df2,  ['phone'],  'left').filter("end_call_cnt>=25")
    cpt_dt_str = cpt_dt.strftime("%Y-%m-%d")
    result = result.withColumn("date", lit(cpt_dt_str))
    tmp_df.unpersist()
    return result


# 频繁通联
def frequent_communication(file_path, index_pasttime=4, index_futuertime=5, cpt_dt=date.today(), select_list=['start_phone', 'end_phone', 'start_time']):
    """
    近5天与同一目标通话次数达到至少3天每天至少一次，当天算频繁通联
    :param file_path:
    :param index_pasttime:
    :param cpt_dt:
    :return:
    """
    str_cal_day = cpt_dt.strftime("%Y-%m-%d")
    cpt_dt -= timedelta(index_pasttime)
    str_week_ago = cpt_dt.strftime("%Y-%m-%d")
    tmp_df = create_table2(file_path, select_list, index_pasttime, index_futuertime, now=cpt_dt)
    tmp_df.persist(StorageLevel.MEMORY_AND_DISK)
    tmp_df.createOrReplaceTempView('table')
    sql = """select start_phone, end_phone, timestamp_2_str(start_time) str_start_time from table where timestamp_2_str(start_time)>='{cp1}' and timestamp_2_str(start_time)<='{cp2}' """.format(cp1=str_week_ago, cp2=str_cal_day)
    df = spark.sql(sql)
    df1 = df.select(df.end_phone.alias('start_phone'), df.start_phone.alias('end_phone'), df.str_start_time)
    df2 = df.union(df1)
    result = df2.dropDuplicates().groupBy(["start_phone", "end_phone", "str_start_time"]).agg(count("str_start_time").alias("days")).filter("days>=3")
    cpt_dt_str = cpt_dt.strftime("%Y-%m-%d")
    result = result.withColumn("date", lit(cpt_dt_str))
    tmp_df.unpersist()
    return result


# 0 夜间频繁通联
def nit_frequent_communication(file_path, index_pasttime=0, index_futuertime=5, cpt_dt=date.today(), select_list=['start_phone', 'end_phone', 'start_time']):
    """
    夜间（01:00-05:00）通联次数达到15次，算夜间频繁通联。
    :param file_path:
    :param index_pasttime:
    :param cpt_dt:
    :return:
    """
    cpt_dt -= timedelta(index_pasttime)
    str_week_ago = cpt_dt.strftime("%Y-%m-%d")
    tmp_df = create_table2(file_path, select_list, index_pasttime, index_futuertime, now=cpt_dt)
    tmp_df.persist(StorageLevel.MEMORY_AND_DISK)
    tmp_df.createOrReplaceTempView('table')
    sql = """select start_phone, end_phone, timestamp_2_str_hour(start_time) hours from table where (timestamp_2_str(start_time)='{cp}' and (timestamp_2_str_hour(start_time)>='1' or timestamp_2_str_hour(start_time)<='5'))""".format(cp=str_week_ago)
    df = spark.sql(sql)
    result1 = df.select(df.start_phone.alias('phone'), df.end_phone).groupBy("phone").agg(count("end_phone").alias("times"))
    result2 = df.select(df.end_phone.alias('phone'), df.start_phone).groupBy("phone").agg(count("start_phone").alias("times"))
    result = result1.union(result2).groupBy("phone").agg(sum("times").alias("nit_call")).filter("nit_call>=15")
    cpt_dt_str = cpt_dt.strftime("%Y-%m-%d")
    result = result.withColumn("date", lit(cpt_dt_str))
    tmp_df.unpersist()
    return result


# 通联广泛
def spread_communication(file_path, index_pasttime=2, index_futuertime=5, cpt_dt=date.today(), select_list=['start_phone', 'end_phone', 'start_time']):
    """
    近3天目标一度通联的个数大于45个小于100个，同时不单单是单项拨打，还存在同电话回拨，回拨比例不低于k=0.45，定义为通联广泛。
    :param file_path:
    :param index_pasttime:
    :param cpt_dt:
    :return:
    """
    str_cal_day = cpt_dt.strftime("%Y-%m-%d")
    cpt_dt -= timedelta(index_pasttime)
    str_week_ago = cpt_dt.strftime("%Y-%m-%d")
    tmp_df = create_table2(file_path, select_list, index_pasttime, index_futuertime, now=cpt_dt)
    tmp_df.persist(StorageLevel.MEMORY_AND_DISK)
    tmp_df.createOrReplaceTempView('table')
    sql1 = """select distinct start_phone, end_phone from table where timestamp_2_str(start_time)>='{cp1}' and timestamp_2_str(start_time)<='{cp2}'""".format(cp1=str_week_ago, cp2=str_cal_day)
    df1 = spark.sql(sql1)
    df2 = df1.select(df1.end_phone.alias('start_phone'), df1.start_phone.alias('end_phone'))
    df = df2.union(df1)
    df = df.groupBy('start_phone', 'end_phone').agg(count('end_phone').alias("cnt"))
    ff = lambda cond: count(when(cond, 1).otherwise(None))
    cond1 = (df['cnt'] == 1)
    dfc1 = df.groupby('start_phone').agg(ff(cond1).alias("cnt1"))
    cond2 = (df['cnt'] >= 2)
    dfc2 = df.groupby('start_phone').agg(ff(cond2).alias("cnt2"))
    dfc = dfc1.join(dfc2, ['start_phone'], 'left')
    contribs = udf(lambda x, y: x / (y + 1e-5), FloatType())
    phone_perc = dfc.select('start_phone', contribs(dfc.cnt2, dfc.cnt1).alias('perc'))
    df = df.join(phone_perc, ['start_phone'], 'left')
    df = df.filter("perc>0.45")  # 有相互通联的比例要占到单呼出呼入比例的30%
    result = df.groupBy("start_phone").agg(count("end_phone").alias("different_phone")).filter(
        "different_phone>=45").filter("different_phone<100")  # 多余40小于100个不同电话
    cpt_dt_str = cpt_dt.strftime("%Y-%m-%d")
    result = result.withColumn("date", lit(cpt_dt_str))
    tmp_df.unpersist()
    return result


# 短时间通联
def short_time_call(file_path, index_pasttime=0, index_futuertime=5, cpt_dt=date.today(), select_list=['start_phone', 'end_phone', 'start_time', 'call_duration']):
    """
    近1天通联时间8秒以内，次数大于12次。
    :param file_path:
    :param index_pasttime:
    :param cpt_dt:
    :return:
    """
    cpt_dt -= timedelta(index_pasttime)
    str_week_ago = cpt_dt.strftime("%Y-%m-%d")
    tmp_df = create_table2(file_path, select_list, index_pasttime, index_futuertime, now=cpt_dt)
    tmp_df.persist(StorageLevel.MEMORY_AND_DISK)
    tmp_df.createOrReplaceTempView('table')
    sql = """select start_phone, end_phone from table where (timestamp_2_str(start_time)='{cp}' and call_duration<=8)""".format(cp=str_week_ago)
    df = spark.sql(sql)
    result1 = df.select(df.start_phone.alias('phone'), df.end_phone).groupBy("phone").agg(count("end_phone").alias("times"))
    result2 = df.select(df.end_phone.alias('phone'), df.start_phone).groupBy("phone").agg(count("start_phone").alias("times"))
    result = result1.union(result2).groupBy("phone").agg(sum("times").alias("short_call")).filter("short_call>=12")
    cpt_dt_str = cpt_dt.strftime("%Y-%m-%d")
    result = result.withColumn("date", lit(cpt_dt_str))
    tmp_df.unpersist()
    return result

"""
批量计算函数
"""


# 跑批函数
def batch_def_run(function_name, file_path, resultpath, index_pasttime, index_futuretime, now):
    df = globals()[function_name](file_path, index_pasttime, index_futuretime, now).repartition(20)
    df.write.mode("overwrite").format("orc").save(resultpath)
    del df


if __name__ == '__main__':
    logger.info('=====start time:%s=====' % (time.strftime('%Y-%m-%d %H:%M:%s', time.localtime())))
    # 路径
    # input
    # lon, lat, 电围事件
    file_path_lonlat = "/phoebus/_fileservice/users/slmp/shulianmingpin/incrdir/bbd_dw_detail/"
    # phone2phone，话单数据
    file_path_call2call = "/phoebus/_fileservice/users/slmp/shulianmingpin/incrdir/edge_groupcall_detail/"
    # post_delivery，寄递数据
    file_path_postdeli = "/phoebus/_fileservice/users/slmp/shulianmingpin/incrdir/edge_phone_sendpackage_phone_detail/"

    # output
    resultpath = "/phoebus/_fileservice/users/slmp/shulianmingpin/investlabel/"

    # fuctionlist
    # 第一个为计算方法
    # 第二个为指标要计算的过去时间范围，0为当天。
    # 第三个为计算指标数据可能存在在未来的几天数据中
    # 第四个为计算所需文件路径
    runlist = [
        # ('day_in_nit_out', 1, 1, file_path_lonlat), # x
        # ('frequent_move', 1, 1, file_path_lonlat),  # x
        # ('homeless', 5, 1, file_path_lonlat),  #  x
        # ('cross_regional', 3, 1, file_path_lonlat),  # x
        # ('with_muti_cards', 3, 1, file_path_lonlat),  # 1
        # ('with_muti_device', 3, 1, file_path_lonlat),  # 2
        # ('have_muti_deviceandcards', 3, 1, file_path_lonlat),  # 3
        # ('frequent_change_cards', 1, 1, file_path_lonlat),  # 4
        # ('huge_start_call', 0, 5, file_path_call2call),  # 5 完成
        # ('huge_start_call_with_ratio', 0, 5, file_path_call2call),  # 6 完成，但是还是会报错
        ('huge_start_call_end_call', 0, 5, file_path_call2call),  # 7  完成10天不报错
        # ('frequent_communication', 4, 5, file_path_call2call),  # 8 只跑通2天，第3天报错
        ('nit_frequent_communication', 0, 5, file_path_call2call),  # 9 完成5天不报错
        ('short_time_call', 0, 5, file_path_call2call),  # 10 完成5天不报错  ps：9和10出现了同一个问题，7月10号和7月9号的数据量大小一致，需要排查。
        # ('post_delivery_same_phone', 1, 1, file_path_postdeli),  # x
        # ('post_delivery_abnormal', 1, 1, file_path_postdeli),  # x
        # ('post_abnormal', 1, 1, file_path_postdeli),  # x
        # ('delivery_abnormal', 1, 1, file_path_postdeli),  # x
        # ('spread_communication', 2, 5, file_path_call2call)  # 11
    ]

    # 由于目前寄递数据只有5月26日的，因此只计算5月27日，后续now取today()
    day = int(sys.argv[2])
    month = int(sys.argv[1])
    now = date(2020, month, day)
    #   now = date.today()

    # run
    i = 0
    while i <= 5:  # 数字代表天数。
        cal_date = now - timedelta(i)
        i += 1
        cpt_dt = cal_date.strftime("%Y%m%d")
        for function_name_cuple in runlist:
            function_name = function_name_cuple[0]
            resultpath_tmp = resultpath + function_name_cuple[0] + "/" + cpt_dt
            index_pasttime = function_name_cuple[1]
            index_futuretime = function_name_cuple[2]
            file_path = function_name_cuple[3]
            try:
                batch_def_run(function_name, file_path, resultpath_tmp, index_pasttime, index_futuretime, cal_date)
            except:
                print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!%s data problem on date %s' % (file_path, cpt_dt))

    logger.info('=====end time:%s=====' % (time.strftime('%Y-%m-%d %H:%M:%s', time.localtime())))



