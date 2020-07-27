# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : vertex_key_person.py
# @Software: PyCharm
# @content : 重点前科吸毒涉毒人员汇总

import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from person_schema_info import *


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')
conf=SparkConf().set('spark.driver.maxResultSize', '2g')
conf.set('spark.executor.memoryOverhead', '15g')
conf.set('spark.yarn.am.cores', 4)
conf.set('spark.executor.memory', '15g')
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/person'
save_root = 'relation_theme_extenddir'

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
    married = [u'已婚',u'复婚',u'初婚',u'再婚',u'丧偶',u'已婚']
    unmarried = [u'离婚',u'未婚']

    def match_word(hyzk,types):
        res = ""
        if not hyzk:
            return u'其他'
        for words in types:
            for key in words:
                if key in hyzk:
                    res = "%s(%s)"%(words[0],hyzk)
                    return res
        return res if res else u'其他(%s)'%hyzk
    types = [married,unmarried]
    try:
        return match_word(hyzk,types)
    except:
        return u'其他'

spark.udf.register('format_hyzk',format_hyzk,StringType())

def vertex_person():
    # person_no	人员编号
    # name	姓名
    # fur_name	曾用名
    # sex_desig	性别
    # comm_cert_code	证件类别代码
    # comm_cert_name	证件类别
    # cred_num	证件号码
    # cert_no	身份证号
    # ctct_tel	联系电话
    # nation_desig	民族
    # birth_date	出生日期
    # eduhis_name	学历
    # mar_sta_name	婚姻状况
    # polsta_name 政治面貌
    # self_iden_code	个人身份代码
    # self_iden_name	个人身份名称
    # cty_area_name	国家和地区名称
    # nation_name	国籍
    # ind_tname	行业类别
    # prof	职业
    # prof_tname	职业类别
    # photo	照片
    # death_date	死亡日期
    # death_judge_flag	是否死亡
    # native_addi_name	籍贯省市县名称
    # acc_no	户号
    # hou_tcode	户类型代码
    # hou_tname	户类型名称
    # pred_addr_name	现住址
    # pred_door_no	现住址门（楼）牌号
    # pred_door_deta_addr	现住址详细地址
    # pred_admin_name	现住址行政区划
    # pred_towstr_name	现住址乡镇街道
    # domic_admin_name	户籍地行政区划
    # domic_addr_name	户籍地址
    table_info = [
        {'tablename':'ods_pol_per_perm_resid_info','zjhm':'cred_num','xm':"name",'zym':'fur_name','mz':'nation_desig',
     'whcd':'eduhis_name','hyzk':'mar_sta_name','gj':'nation_name','zzmm':'polsta_name','hkszdxz':'domic_addr_name',
     'sjjzxz':'pred_door_deta_addr','jg':'native_addi_name','ywxm':'name_spell','rank_time':'last_accep_time','table_sort':1
     },
    ]

    tmp_sql = ''' select format_zjhm({zjhm}) zjhm,'身份证' zjlx,{gj} gj,{xm} xm,
        {ywxm} ywxm,{zym} zym,format_xb({zjhm}) xb,
        format_csrq_from_sfzh({zjhm}) csrq, {mz} mz,{jg} jg,format_whcd({whcd}) whcd,format_hyzk({hyzk}) hyzk,
        {zzmm} zzmm,{hkszdxz} hkszdxz,{sjjzxz} sjjzxz, {table_sort} table_sort
        from {tablename} where verify_sfz({zjhm})=1
        '''
    dfs = []
    for info in table_info:
        sql = tmp_sql.format(**info)
        init(spark,info.get('tablename'),if_write=False)
        df = spark.sql(sql)
        dfs.append(df)
    res1 = reduce(lambda a,b:a.unionAll(b),dfs)
    res1 = res1.dropDuplicates(['zjhm'])
    res2 = read_orc(spark,add_save_path('vertex_person_bbd',root=save_root))
    res3 = read_orc(spark,add_save_path('vertex_person_zp',root=save_root)).withColumn('table_sort',lit(5))
    res = res1.unionAll(res2).unionAll(res3)
    res = res.selectExpr('*', 'row_number() over (partition by zjhm order by table_sort asc) num') \
        .where('num=1').drop('num').drop('table_sort')
    cols = ['zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'valid_datetime(csrq) csrq', 'mz', 'jg',
                                      'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz']
    write_orc(res.selectExpr(*cols),add_save_path('vertex_person',root=save_root))

    ## 照片信息 ods_a_ry_zpxx
    # init(spark,'ods_a_ry_zpxx',if_write=False)
    # sql = ''' select format_zjhm(gmsfhm) zjhm, zp from ods_a_ry_zpxx where verify_sfz(gmsfhm) = 1 and zp != "" '''
    # zp_df = spark.sql(sql).dropDuplicates(['zjhm'])
    # zp_dfs = zp_df.rdd.randomSplit([0.1 for _ in range(10)])
    # i = 0
    # for rdd in zp_dfs:
    #     cp = 2020 + i
    #     df = spark.createDataFrame(rdd,['zjhm','zp'])
    #     write_orc(df,add_save_path('vertex_person_photo',root=save_root,cp=cp))
    #     i += 1

def vertex_person_from_source():
    '''人节点清洗'''
    inboard_sql = ''' select format_zjhm({zjhm}) zjhm,'身份证' zjlx,{gj} gj,{xm} xm,
        {ywxm} ywxm,{zym} zym,format_xb({zjhm}) xb,
        format_csrq_from_sfzh({zjhm}) csrq, {mz} mz,{jg} jg,{whcd} whcd,{hyzk} hyzk,
        {zzmm} zzmm,{hkszdxz} hkszdxz,{sjjzxz} sjjzxz,
        '{tablename}' as tablename, {table_sort} table_sort
        from {tablename} where verify_sfz({zjhm})=1
        '''
    for item in inbord_person_schema:
        sql = inboard_sql.format(**item)
        logger.info(sql)
        tablename = item['tablename']
        if tablename.startswith('edge'):
            create_tmpview_table(spark,tablename)
        else:
            init(spark, item['tablename'], if_write=False)
            df = spark.sql(sql)
            write_parquet(df, path_prefix,item['tablename'] + '-' + item['zjhm'])
            logger.info('%s init down' % item['tablename'])

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
        write_parquet(df, path_prefix,item['tablename'] + '-' + item['zjhm'])
        logger.info('%s down' % item['tablename'])

    paths = [info['tablename'] + '-' + info['zjhm'] for info in inbord_person_schema + oversea_person_schema]
    union_df = None
    for path in paths:
        df = read_parquet(spark,path_prefix,path)
        if not union_df:
            union_df = df
        else:
            union_df = union_df.unionAll(df)

    res_tmp = union_df.selectExpr('*', 'row_number() over (partition by zjhm order by table_sort asc) num') \
        .where('num=1').drop('num').drop('table_sort')

    ## todo 标准化文化程度，婚姻状况
    tranfrom_schema = ['zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                                      'format_whcd(whcd) whcd', 'format_whcd(hyzk) hyzk', 'zzmm', 'hkszdxz', 'sjjzxz','2 table_sort']
    res = res_tmp.selectExpr(*tranfrom_schema)
    write_orc(res, add_save_path('vertex_person_bbd',root=save_root))
    logger.info('vertex_person down')


def vertex_person_increase(tablename,import_times,cp):
    ''' 每天需要增量的人节点 '''
    person_tables = [
    # ods_soc_civ_avia_leaport_data	民航离港数据
    {'tablename':'ods_soc_civ_avia_leaport_data','zjhm':'cred_num','xm':"pass_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'pass_for_name',
     'rank_time':'coll_time','table_sort':35,'cur_time':'sail_time'
     },
    # ods_soc_civ_avia_rese	民航订票
    {'tablename':'ods_soc_civ_avia_rese','zjhm':'cred_num','xm':"pass_name",'zym':'""','mz':'""','whcd':'""','hyzk':'""',
     'gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'domic_admin_name',
     'ywxm':'concat(pass_for_first_name," ",pass_for_last_name)','rank_time':'coll_time','table_sort':36,'cur_time':'staoff_time'
     },
    # ods_soc_traf_raista_entper	火车站进站人员信息
    {'tablename':'ods_soc_traf_raista_entper','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time',
     'table_sort':37,'cur_time':'date2timestampstr(riding_date)'
     },
    # ods_soc_traf_raiway_saltic_data	铁路售票数据
    {'tablename':'ods_soc_traf_raiway_saltic_data','zjhm':'cred_num','xm':"name",'zym':'""','mz':'nation_desig',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'domic_addr_name','jg':'""','ywxm':'""',
     'rank_time':'coll_time','table_sort':38,'cur_time':'date2timestampstr(riding_date)'
     },
    # 网吧上网信息 ods_pol_sec_netbar_intper_info
    {'tablename':'ods_pol_sec_netbar_intper_info','zjhm':'cred_num','xm':"inte_per_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""',
     'rank_time':'coll_time','table_sort':39,'cur_time':'format_timestamp(intenet_start_time)'
     },
    # ods_soc_civ_avia_arrport_data	民航进港数据
    {'tablename':'edge_person_arrived_airline_detail','zjhm':'sfzh','xm':'""','zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""',
    'ywxm':'""','rank_time':'hbrq','table_sort':88
     },
    ]

    tmp_sql = ''' 
        select format_zjhm({zjhm}) zjhm,'身份证' zjlx,{gj} gj,{xm} xm,
        {ywxm} ywxm,{zym} zym,format_xb({zjhm}) xb,
        format_csrq_from_sfzh({zjhm}) csrq, {mz} mz,{jg} jg,{whcd} whcd,{hyzk} hyzk,
        {zzmm} zzmm,{hkszdxz} hkszdxz,{sjjzxz} sjjzxz,
        '{tablename}' as tablename, {table_sort} table_sort
        from {tablename} where verify_sfz({zjhm})=1
        '''

    for info in person_tables:
        if info['tablename'] == tablename:
            init_history(spark,tablename,import_times,if_write=False)
            sql = tmp_sql.format(**info)
            logger.info(sql)
            df = spark.sql(sql)
            write_orc(df,add_save_path('vertex_person_%s'%tablename,cp=cp,root='relation_incrdir'))
            logger.info('vertex_person_%s down'%tablename)



if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    if len(sys.argv) == 4:
        import_times = sys.argv[1].split(',')
        cp = str(sys.argv[2])
        tablename = str(sys.argv[3])
        vertex_person_increase(tablename,import_times,cp)
    else:
        vertex_person_from_source()
        vertex_person()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))