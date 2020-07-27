# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : company.py
# @Software: PyCharm
# @content : 工商相关信息

import sys, os
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
conf.set('spark.yarn.memoryOverhead', '10g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '10g')
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

##todo:all
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'


def im_relate_im():
    # md_id 唯一标识
    # qq QQ号
    # rel_qq 关系人 - QQ号
    # rel_type 关系类型
    # rel_group 好友分组
    # rel_remark 好友备注
    # last_dis_place 最后发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    # totle_count 累计发现次数
    # totle_days 累计发现天数
    # trac_inf 溯源信息
    # recent_dic_mark 发现标记
    # predict 数据可信度

    ## qq 好友
    init(spark,'nb_app_dws_per_per_his_fri_qq',if_write=False,is_relate=True)

    sql = '''
        select qq qq1, rel_qq qq2,rel_group grup_name,rel_remark remark, first_time start_time, 
        last_time end_time from nb_app_dws_per_per_his_fri_qq
        where verify_im_account(qq) = 1 and verify_im_account(rel_qq) = 1
    '''
    cols = ['qq1','qq2','start_time','end_time']
    df1 = spark.sql(sql).selectExpr(*cols)
    df2 = read_orc(spark,add_save_path('edge_qq_link_qq_zp',root=save_root)).selectExpr(*cols)
    df3 = get_upload_edge(spark,'qqfqq').selectExpr(*cols)
    df = df1.unionAll(df2).unionAll(df3).dropDuplicates(['qq1','qq2'])
    write_orc(df,add_save_path('edge_qq_link_qq',root=save_root))

    # rel_wxid 关系人 - WXID
    # rel_wx 关系人 - WX账号
    # rel_type 关系类型
    # rel_group 好友分组
    # rel_remark 好友备注
    # dis_place 发现地
    # first_time 首次发现时间
    # last_time 最后发现时间

    init(spark, 'nb_app_dws_per_per_his_fri_wx', if_write=False, is_relate=True)

    sql = '''
                select format_wechat(wx) wechat1, format_wechat(rel_wx) wechat2,wxid wechatid1,rel_wxid wechatid2, rel_type type,rel_group group_name, 
                rel_remark remark, first_time start_time,last_time end_time from nb_app_dws_per_per_his_fri_wx
                where verify_wechat(wx) = 1 and verify_wechat(rel_wx) = 1
            '''
    df1 = spark.sql(sql).dropDuplicates(['wechat1','wechat2'])
    cols = ['wechat1','wechat2','"" wechatid1','"" wechatid2','"" type','"" group_name','"" remark','start_time','end_time']
    df2 = get_upload_edge(spark,'wechatfwechat').selectExpr(cols)
    res = df1.unionAll(df2).dropDuplicates(['wechat1','wechat2'])
    write_orc(res,add_save_path('edge_wechat_link_wechat',root=save_root))

    ##用户与用户帐号的关系
    # md_id　唯一标识
    # domain　域名
    # vt_type　虚拟身份类型
    # userid　用户ID
    # rel_user_acc　系人 - 用户账号
    # last_dis_place　最后发现地
    # first_time　首次发现时间
    # last_time　最后发现时间
    # totle_count　累计发现次数
    # totle_days　累计发现天数
    ##  暂时不需要此关系
    init(spark, 'nb_app_dws_per_per_his_tr_uidacc', if_write=False, is_relate=True)

    sql = '''
        select  concat_ws('-',userid,vt_type) user_id, concat_ws('-',rel_user_acc,vt_type) user_account, vt_type type, first_time start_time,last_time end_time 
        from nb_app_dws_per_per_his_tr_uidacc
    '''
    df = spark.sql(sql).dropDuplicates(['user_id','user_account'])
    # write_orc(df,add_save_path('edge_userid_link_account',root=save_root))

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    im_relate_im()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))