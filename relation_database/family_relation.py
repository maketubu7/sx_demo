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
conf.set('spark.yarn.executor.memoryOverhead', '30g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '2g')
conf.set('spark.executor.instances', 10)
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'

type_group = {
    'child':['120', '121', '122', '123', '124', '125', '126', '127', '128', '129',
             '130', '131', '132', '133', '134', '135', '136', '137', '138', '139', ],
    'father':['151','157'],
    'grandpar':['160', '161', '162', '163', '164', '165', ],
    'grandson':['140', '141', '142', '143', '144', '145', '146', '147', '148', '149', ],
    'guardian':['159'],
    'mother':['152','158'],
    'other':['100', '153', '154', '155', '156', '172', '174', '176', '178', '179', '181','182',
             '183', '184', '185', '186', '187', '188', '189', '190', '193', '194', '195', '196'],
    'parent':['150'],
    'samegrandpar':['191','192'],
    'samepar':['170', '171', '173', '175', '177', ],
    'spouse':['110'],
    'wife':['112'],
    'bushand':['111'],
    'supgrandpar':['166', '167', '168', '169', ]
}

def format_kinsfolk_type(col):
    res = 'other'
    try:
        for k,v in type_group.items():
            if col in v:
                res = k
                return res
    except:
        return res
    return res

spark.udf.register('format_kinsfolk_type',format_kinsfolk_type,StringType())

def deal_common_relate(res):
    ret = []
    relate_sfzh,sfzhs = res
    if len(sfzhs)>1:
        try:
            sfzhs = sorted(sfzhs)
            for index,value in enumerate(sfzhs):
                for i in range(index+1,len(sfzhs)):
                    ret.append((value,sfzhs[i],relate_sfzh))
        except:
            pass
    return ret


def domic_relate():
    ## 户籍信息
    # cert_type 证件类型
    # cert_num 证件号码
    # domic_num 户号
    # hou_head_rel 与户主关系
    # domic_type 户籍类型
    # domic_nature 户籍性质
    # domic_addr 户籍地址
    # domic_adm_div 所属行政区划
    # move_in_date_fs 迁入时间
    # move_out_date_fs 迁出时间
    # rel_type 最新关系类型
    # last_dis_place 最新发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    # totle_count 累计发现次数
    # totle_days 累计发现天数

    #vertex_hukou
    init(spark, 'nb_app_dws_per_res_his_dchousehold', if_write=False, is_relate=True)
    sql = '''
        select domic_num, domic_type, domic_nature, domic_addr, domic_adm_div from nb_app_dws_per_res_his_dchousehold
    '''

    df = spark.sql(sql).dropDuplicates(['domic_num'])
    write_orc(df,add_save_path('vertex_domic',root=save_root))

    # ## 人关联户口
    sql = '''
        select cert_num sfzh,domic_num,valid_datetime(move_in_date_fs) start_time,
        valid_datetime(move_out_date_fs) end_time,domic_type from nb_app_dws_per_res_his_dchousehold
        where verify_sfz(cert_num) = 1 and domic_num != '' and domic_num is not null
    '''

    df = spark.sql(sql).dropDuplicates(['sfzh','domic_num']).drop('domic_type')
    write_orc(df,add_save_path('edge_person_link_domic',root=save_root))

    ## 同户口
    tmp_df = read_orc(spark,add_save_path('edge_person_link_domic',root=save_root))
    # 排除集体户
    df = tmp_df.groupby('domic_num').agg(count('sfzh').alias('num'))
    df1 = df.where('num <= 10')

    df3 = tmp_df.join(df1,'domic_num','inner').select(tmp_df.domic_num,tmp_df.sfzh)

    df3.createOrReplaceTempView('tmp')
    domics = spark.sql('''select domic_num,collect_set(sfzh) as sfzhs from tmp group by  domic_num''')

    df = spark.createDataFrame(domics.rdd.flatMap(deal_common_relate), ['sfzh1', 'sfzh2', 'domic_num'])
    common_df = df.selectExpr('sfzh1','sfzh2','format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time','domic_num')
    write_orc(common_df,add_save_path('edge_find_common_domic',root=save_root))


def find_common_parents():
    '''
    推理同一个父亲，同一个母亲
    :return:
    '''
    mother = read_orc(spark,add_save_path('edge_person_motheris_person',root=save_root)).select('sfzh','mqsfzh')
    father = read_orc(spark,add_save_path('edge_person_fatheris_person',root=save_root)).select('sfzh','fqsfzh')

    find_mohter = read_orc(spark,add_save_path('edge_find_mother',root=save_root)).select('sfzh','mqsfzh')
    find_father = read_orc(spark,add_save_path('edge_find_father',root=save_root)).select('sfzh','fqsfzh')

    father_all = father.unionAll(find_father)
    father_all.createOrReplaceTempView('tmp')
    # 同父 需要加上 找到的父亲母亲
    suns = spark.sql('''select fqsfzh,collect_set(sfzh) as  sfzhs from tmp group by  fqsfzh''')
    df = spark.createDataFrame(suns.rdd.flatMap(deal_common_relate), ['sfzh1', 'sfzh2', 'fqsfzh'])
    common_father = df.select(when(df.sfzh1 > df.sfzh2, df.sfzh1).otherwise(df.sfzh2).alias('sfzh1'), \
                              when(df.sfzh1 > df.sfzh2, df.sfzh2).otherwise(df.sfzh1).alias('sfzh2')).dropDuplicates()
    common_father = common_father \
            .selectExpr('sfzh1','sfzh2','"推理" type_name','format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time')
    write_orc(common_father, add_save_path('edge_find_common_father',root=save_root))
    logger.info('edge_find_common_father down')

    # 同母 需要加上 找到的父亲母亲
    mother_all = mother.unionAll(find_mohter)
    mother_all.createOrReplaceTempView('tmp')
    suns = spark.sql('''select mqsfzh,collect_set(sfzh) as sfzhs from tmp group by  mqsfzh''')

    df = spark.createDataFrame(suns.rdd.flatMap(deal_common_relate), ['sfzh1', 'sfzh2', 'mqsfzh'])
    common_mother = df.select(when(df.sfzh1 > df.sfzh2, df.sfzh1).otherwise(df.sfzh2).alias('sfzh1'), \
                              when(df.sfzh1 > df.sfzh2, df.sfzh2).otherwise(df.sfzh1).alias('sfzh2')).dropDuplicates()
    common_mother = common_mother \
                    .selectExpr('sfzh1','sfzh2','"推理" type_name','format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time')
    write_orc(common_mother, add_save_path('edge_find_common_mother',root=save_root))
    logger.info('common_mother down')


def find_common_grandian():
    df = read_orc(spark,add_save_path('edge_person_guardianis_person',root=save_root))
    df.createOrReplaceTempView('tmp')
    sql = '''
        select jhrsfzh, collect_set(sfzh) sfzhs from tmp group by jhrsfzh
    '''
    res_tmp = spark.createDataFrame(spark.sql(sql).rdd.flatMap(deal_common_relate),['sfzh1','sfzh2','jhrsfzh']).selectExpr('sfzh1','sfzh2')
    ## 排除同父同母同父母
    common_father = read_orc(spark,add_save_path('edge_find_common_father',root=save_root)).selectExpr(['sfzh1','sfzh2'])
    common_mother = read_orc(spark,add_save_path('edge_find_common_mother',root=save_root)).selectExpr(['sfzh1','sfzh2'])
    common_parents = read_orc(spark,add_save_path('edge_find_common_parent',root=save_root)).selectExpr(['sfzh1','sfzh2'])
    exists_df = common_father.unionAll(common_mother).unionAll(common_parents).dropDuplicates(['sfzh1','sfzh2'])
    res = res_tmp.subtract(exists_df) \
            .selectExpr('sfzh1','sfzh2','"推理" type_name','format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time') \
            .dropDuplicates(['sfzh1','sfzh2'])
    write_orc(res,add_save_path('edge_find_common_grandian',root=save_root))


def find_parent():
    '''
    通过配偶和父母关系推理
    :return:
    '''
    mother = read_orc(spark,add_save_path('edge_person_motheris_person',root=save_root)).select('sfzh','mqsfzh')
    father = read_orc(spark,add_save_path('edge_person_fatheris_person',root=save_root)).select('sfzh','fqsfzh')

    create_tmpview_table(spark,'edge_person_spouse_person',root=save_root)
    sql3 = '''select sfzhnv,max(sfzhnan) sfzhnan,count(sfzhnan) as num 
                from edge_person_spouse_person group by  sfzhnv '''
    # 母亲只有一个配偶的数据
    motherHasOneFather = spark.sql(sql3).where('num=1')
    # 缺失父亲的数据
    tmp = mother.join(father, 'sfzh', 'left').drop(father.sfzh)
    lose_father = tmp.where('fqsfzh is null')
    # 获取到父亲
    find_father = lose_father.join(motherHasOneFather, lose_father.mqsfzh == motherHasOneFather.sfzhnv, 'inner') \
        .select(lose_father.sfzh, motherHasOneFather.sfzhnan.alias('fqsfzh')).dropDuplicates()
    find_father = find_father.selectExpr('sfzh','fqsfzh','"推理" type_name','format_starttime_from_sfzh(sfzh) start_time','0 end_time') \
                .where('verify_man(fqsfzh)=1').dropDuplicates(['sfzh','fqsfzh'])
    write_orc(find_father, add_save_path('edge_find_father',root=save_root))
    logger.info('edge_find_father down')

    # 找母亲 A以父亲关联B，B仅以一条配偶关系关联C
    # 父亲只有一个配偶的数据
    create_tmpview_table(spark,'edge_person_spouse_person',root=save_root)
    sql4 = '''select  sfzhnan,max(sfzhnv) sfzhnv,count(sfzhnv) as num  
                from edge_person_spouse_person group by  sfzhnan '''
    faherHasOneMother = spark.sql(sql4).where('num=1')

    tmp = father.join(mother, 'sfzh', 'left').drop(mother.sfzh)
    lose_mother = tmp.where('mqsfzh is null')
    # 获取到母亲
    find_mother = lose_mother.join(faherHasOneMother, lose_mother.fqsfzh == faherHasOneMother.sfzhnan, 'inner') \
        .select(lose_mother.sfzh, faherHasOneMother.sfzhnv.alias('mqsfzh')).dropDuplicates()

    find_mother = find_mother.selectExpr('sfzh','mqsfzh','"推理" type_name','format_starttime_from_sfzh(sfzh) start_time','0 end_time') \
                .where('verify_man(mqsfzh)=0').dropDuplicates(['sfzh','mqsfzh'])

    write_orc(find_mother,add_save_path('edge_find_mother',root=save_root))
    logger.info('edge_find_mother down')

def find_spouse():
    ''' 推理配偶 '''
    # 补全配偶 A以一条父亲关系关联B且同时以一条母亲关联C
    mother = read_orc(spark,add_save_path('edge_real_motheris',root=save_root)).select('sfzh', 'mqsfzh')
    father = read_orc(spark,add_save_path('edge_real_fatheris',root=save_root)).select('sfzh', 'fqsfzh')

    # 一个人关联的父亲母亲
    df = mother.join(father, 'sfzh', 'inner').select(mother.sfzh, mother.mqsfzh, father.fqsfzh)
    create_tmpview_table(spark,'edge_person_spouse_person',root=save_root)
    spouse = spark.sql('''select  sfzhnan, sfzhnv from edge_person_spouse_person ''')
    # 关联父亲的原有配偶
    df1 = df.join(spouse, df.fqsfzh == spouse.sfzhnan, 'left') \
            .select(df.fqsfzh,df.mqsfzh,spouse.sfzhnv)

    # 关联母亲的的原有配偶
    create_tmpview_table(spark,'edge_person_spouse_person',root=save_root)
    spouse2 = spark.sql('''select  sfzhnan, sfzhnv from edge_person_spouse_person ''')
    df2 = df1.join(spouse2,df1.mqsfzh==spouse2.sfzhnv,'left') \
        .select(df1.fqsfzh.alias('sfzhnan'),df1.mqsfzh.alias('sfzhnv'),df1.sfzhnv.alias('sfzh1'),spouse2.sfzhnan.alias('sfzh2'))
    # 2个关联 的原有配偶 都为空时 判断为配偶关系
    find_spouse = df2.where('sfzh1 is null and sfzh2 is null').drop('sfzh1').drop('sfzh2').withColumn('type_name',lit("推理"))
    cols = ['sfzhnan','sfzhnv','type_name','0 start_time','0 end_time']
    find_spouse = find_spouse.selectExpr(*cols)
    write_orc(find_spouse,add_save_path('edge_find_spouse',root=save_root))
    logger.info('edge_find_spouse down')


def some_family_relation():
    ## 人物关系主题
    # cert_type 证件类型
    # cert_num 证件号码
    # rel_cert_type 关系人 - 证件类型
    # rel_cert_num 关系人 - 证件号码
    # rel_type 关系类型
    # last_dis_place 最近一次发现地
    # first_time 首次发现时间
    # last_time 最后发现时间
    # totle_count 累计发现次数
    # totle_days 累计发现天数

    init(spark, 'nb_app_dws_per_per_his_dcfr', if_write=False, is_relate=True)
    sql = '''
        select cert_num sfzh1,rel_cert_num sfzh2,rel_type type,cast(first_time as bigint) start_time,cast(last_time as bigint) end_time
        from nb_app_dws_per_per_his_dcfr
        where verify_sfz(cert_num) = 1 and verify_sfz(rel_cert_num) = 1
    '''

    cols = ['sfzh1','sfzh2','type','if(start_time>0,start_time,0) start_time','if(end_time>0,end_time,0) end_time']
    df = spark.sql(sql).dropDuplicates(['sfzh1','sfzh2','type']).selectExpr(*cols).where('sfzh1 != sfzh2')
    write_orc(df,add_save_path('family_source',root=save_root))

    source = read_orc(spark,add_save_path('family_source',root=save_root)).where('sfzh1 != sfzh2')
    type_df = get_all_dict(spark).where('lb="person_link_person_type"').select('dm','mc')

    source = source.join(type_df,source['type']==type_df['dm'],'left') \
            .selectExpr('sfzh1','sfzh2','format_kinsfolk_type(type) type','mc type_name','start_time','end_time') \
            .na.fill({'type_name':u'其他亲属关系'})
    source.persist()

    ## 父子 母子
    father1_cols = ['sfzh1 sfzh','sfzh2 fqsfzh','type_name','format_starttime_from_sfzh(sfzh1) start_time','0 end_time']
    father2_cols = ['sfzh2 sfzh','sfzh1 fqsfzh','type_name','format_starttime_from_sfzh(sfzh2) start_time','0 end_time']
    father_df1 = source.where('type="child" and verify_man(sfzh2) = 1').selectExpr(*father1_cols)
    father_df2 = source.where('type="father" and verify_man(sfzh1) = 1').selectExpr(*father2_cols)
    real_father = father_df1.unionAll(father_df2).dropDuplicates(['sfzh','fqsfzh'])
    write_orc(real_father,add_save_path('edge_real_fatheris',root=save_root))


    mother1_cols = ['sfzh1 sfzh','sfzh2 mqsfzh','type_name','format_starttime_from_sfzh(sfzh1) start_time','0 end_time']
    mother2_cols = ['sfzh2 sfzh','sfzh1 mqsfzh','type_name','format_starttime_from_sfzh(sfzh2) start_time','0 end_time']
    mother_df1 = source.where('type="child" and verify_man(sfzh2) = 0').selectExpr(*mother1_cols)
    mother_df2 = source.where('type="mother" and verify_man(sfzh1) = 0').selectExpr(*mother2_cols)
    real_mother = mother_df1.unionAll(mother_df2).dropDuplicates(['sfzh','mqsfzh'])
    write_orc(real_mother,add_save_path('edge_real_motheris',root=save_root))


    ## 祖父母 曾祖父母
    grandpar1_cols = ['sfzh1 sfzh','sfzh2 grandsfzh','type_name','format_starttime_from_sfzh(sfzh1) start_time','0 end_time']
    grandpar2_cols = ['sfzh2 sfzh','sfzh1 grandsfzh','type_name','format_starttime_from_sfzh(sfzh2) start_time','0 end_time']
    grandpar_df1 = source.where('type="grandson"').selectExpr(*grandpar1_cols)
    grandpar_df2 = source.where('type="grandpar"').selectExpr(*grandpar2_cols)
    grandpar_df = grandpar_df1.unionAll(grandpar_df2).dropDuplicates(['sfzh','grandsfzh'])
    write_orc(grandpar_df,add_save_path('edge_find_grandparents',root=save_root))

    supgrandpar_cols = ['sfzh2 sfzh','sfzh1 greatgrandsfzh','type_name','format_starttime_from_sfzh(sfzh2) start_time','0 end_time']
    supgrandpar_df = source.where('type="supgrandpar"').selectExpr(*supgrandpar_cols).dropDuplicates(['sfzh','greatgrandsfzh'])
    write_orc(supgrandpar_df,add_save_path('edge_find_greatgrands',root=save_root))


    ## 配偶
    bush_cols = ['sfzh1 sfzhnan','sfzh2 sfzhnv','type_name','start_time','end_time']
    wife_cols = ['sfzh2 sfzhnan','sfzh1 sfzhnv','type_name','start_time','end_time']
    spouse_cols =['if(verify_man(sfzh1)=1,sfzh1,sfzh2) sfzhnan','if(verify_man(sfzh1)=0,sfzh1,sfzh2) sfzhnv','type_name','start_time','end_time']

    spouse_df1 = source.where('type="bushand" and verify_man(sfzh1)=1 and verify_man(sfzh2)=0') \
                .selectExpr(*bush_cols)
    spouse_df2 = source.where('type="wife" and verify_man(sfzh2)=1 and verify_man(sfzh1)=0') \
                .selectExpr(*wife_cols)
    spouse_df3 = source.where('type="spouse"').selectExpr(*spouse_cols)
    ## 根据父母推理配偶
    find_spouse()
    tuili_spouse = read_orc(spark,add_save_path('edge_find_spouse',root=save_root))
    if tuili_spouse:
        spouse_df = spouse_df1.unionAll(spouse_df2).unionAll(spouse_df3).unionAll(tuili_spouse).dropDuplicates(['sfzhnan','sfzhnv'])
        res = spouse_df.where('verify_man(sfzhnan)=1 and verify_man(sfzhnv)=0')
        write_orc(res,add_save_path('edge_person_spouse_person',root=save_root))

    ## 根据配偶推理父母
    find_parent()
    real_mother = read_orc(spark,add_save_path('edge_real_motheris',root=save_root))
    find_mother = read_orc(spark,add_save_path('edge_find_father',root=save_root))
    if find_mother:
        mother_df = real_mother.unionAll(find_mother).dropDuplicates(['sfzh','mqsfzh']) \
                    .where('verify_man(mqsfzh)=0')
        write_orc(mother_df,add_save_path('edge_person_motheris_person',root=save_root))

    find_father = read_orc(spark,add_save_path('edge_find_father',root=save_root))
    real_father = read_orc(spark,add_save_path('edge_real_fatheris',root=save_root))
    if find_father:
        father_df = real_father.unionAll(find_father).dropDuplicates(['sfzh','fqsfzh']) \
                .where('verify_man(fqsfzh)=1')
        write_orc(father_df,add_save_path('edge_person_fatheris_person',root=save_root))

    ## 监护人
    init(spark,'nb_app_dws_per_per_his_dcguardian',if_write=False,is_relate=True)
    sql = '''
        select cert_num sfzh, rel_cert_num jhrsfzh, "监护人" type_name, format_starttime_from_sfzh(cert_num) start_time,0 end_time
        from nb_app_dws_per_per_his_dcguardian where substr(rel_cert_num,7,4) < substr(cert_num,7,4)
        and verify_sfz(cert_num) = 1 and verify_sfz(rel_cert_num) = 1
    '''
    guardian_df1 = spark.sql(sql).dropDuplicates(["sfzh","jhrsfzh"])
    guardian_cols = ['sfzh2 sfzh','sfzh1 jhrsfzh','type_name','format_starttime_from_sfzh(sfzh2) start_time','0 end_time']
    guardian_df2 = source.where('type="guardian"').selectExpr(*guardian_cols)

    guar_cols = ['sfzh sfzh1','jhrsfzh sfzh2','type_name','start_time','end_time']
    guardian_df = guardian_df1.unionAll(guardian_df2).dropDuplicates(['sfzh','jhrsfzh']).selectExpr(*guar_cols)

    ## 去掉父母存在的关系
    f_cols = ['sfzh sfzh1','fqsfzh sfzh2']
    father = read_orc(spark,add_save_path('edge_person_fatheris_person',root=save_root)).selectExpr(*f_cols)
    m_cols = ['sfzh sfzh1','mqsfzh sfzh2']
    mother = read_orc(spark,add_save_path('edge_person_motheris_person',root=save_root)).selectExpr(*m_cols)
    parents = father.unionAll(mother).dropDuplicates()
    guardian_tmp = guardian_df.selectExpr('sfzh1','sfzh2').subtract(parents)
    guardian_res = guardian_df.join(guardian_tmp,['sfzh1','sfzh2'],'inner') \
            .select(guardian_df.sfzh1.alias('sfzh'),guardian_df.sfzh2.alias('jhrsfzh'),guardian_df.type_name,
                    guardian_df.start_time,guardian_df.end_time)

    write_orc(guardian_res,add_save_path('edge_person_guardianis_person',root=save_root))

    # # 同父母
    samepar_cols = ['if(sfzh1<sfzh2,sfzh1,sfzh2) sfzh1','if(sfzh1>sfzh2,sfzh1,sfzh2) sfzh2']
    samepar_df_tmp = source.where('type="samepar"').selectExpr(*samepar_cols)
    samepar_df_source = source.where('type="samepar"').selectExpr(*(samepar_cols+['type_name'])).dropDuplicates(['sfzh1','sfzh2'])
    # ## 排除同父同母的
    same_father = read_orc(spark,add_save_path('edge_find_common_father',root=save_root)).select('sfzh1','sfzh2')
    same_mother = read_orc(spark,add_save_path('edge_find_common_mother',root=save_root)).select('sfzh1','sfzh2')
    samepar_df = samepar_df_tmp.subtract(same_father.unionAll(same_mother)) \
            .selectExpr('sfzh1','sfzh2','format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time') \
            .dropDuplicates(['sfzh1','sfzh2'])
    samepar_df = samepar_df.join(samepar_df_source,['sfzh1','sfzh2'],'inner') \
            .select(samepar_df.sfzh1,samepar_df.sfzh2,samepar_df_source.type_name,samepar_df.start_time,samepar_df.end_time)
    write_orc(samepar_df.dropDuplicates(['sfzh1','sfzh2']),add_save_path('edge_find_common_parent',root=save_root))
    ## 同曾祖父
    samegrandpar_cols = ['if(sfzh1<sfzh2,sfzh1,sfzh2) sfzh1','type_name','if(sfzh1>sfzh2,sfzh1,sfzh2) sfzh2','format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time']
    samegrandpar_df = source.where('type="samegrandpar"').selectExpr(*samegrandpar_cols).dropDuplicates(['sfzh1','sfzh2'])
    write_orc(samegrandpar_df,add_save_path('edge_find_common_greatgrand',root=save_root))

    # ## 同祖父 todo:推理同祖父
    samesupergrand_cols = ['if(sfzh1<sfzh2,sfzh1,sfzh2) sfzh1','if(sfzh1>sfzh2,sfzh1,sfzh2) sfzh2','"推理" type_name','format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time']
    grandpar_df.selectExpr('sfzh','grandsfzh').createOrReplaceTempView('tmp')
    sql = ''' select grandsfzh, collect_set(sfzh) sfzhs from tmp group by grandsfzh'''
    samesupergrand = spark.createDataFrame(spark.sql(sql).rdd.flatMap(deal_common_relate),['sfzh1','sfzh2','grandsfzh'])
    samesupergrand = samesupergrand.selectExpr(*samesupergrand_cols[:2])
    samepar = read_orc(spark,add_save_path('edge_find_common_mother',root=save_root)).select('sfzh1','sfzh2') \
                .unionAll(read_orc(spark,add_save_path('edge_find_common_father',root=save_root)).select('sfzh1','sfzh2'))
    samesupergrand = samesupergrand.subtract(samepar).selectExpr(*samesupergrand_cols)
    write_orc(samesupergrand,add_save_path('edge_find_common_grand',root=save_root))

    ## 其他亲属关系
    other_cols = ['if(sfzh1<sfzh1,sfzh1,sfzh1) sfzh1','if(sfzh1>sfzh1,sfzh1,sfzh2) sfzh2','type_name','format_start_sfzhs(sfzh1,sfzh2) start_time','0 end_time']
    samegrandpar_df = source.where('type="other"').selectExpr(*other_cols).dropDuplicates(['sfzh1','sfzh2'])
    write_orc(samegrandpar_df,add_save_path('edge_person_kinsfolk_person',root=save_root))

    source.unpersist()

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    ## 推理同父母，夫妻
    domic_relate()
    some_family_relation()
    find_common_grandian()


    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))