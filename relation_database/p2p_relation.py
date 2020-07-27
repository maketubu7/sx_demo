# encoding=utf-8
import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time, copy, re, math
from datetime import date,datetime, timedelta
import datetime
from collections import OrderedDict
import json
import logging
import calendar


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

conf = SparkConf().set('spark.driver.maxResultSize', '30g')
# conf.set('spark.driver.memoryOverhead','10g')
conf.set('spark.executor.memoryOverhead', '15g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '15g')
conf.set('spark.executor.instances', 100)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')
conf.set("spark.sql.shuffle.partitions",1000)
# conf.set("spark.default.paralleism",1000)


spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
commonUtil = CommonUdf(spark)

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/relation'
root_home = 'person_relation_detail'
save_root = 'relation_theme_extenddir'

def read_parquet(path):
    '''读 parquet'''
    df = spark.read.parquet(os.path.join(path_prefix, 'parquet/', path))
    return df

def write_parquet(df, path):
    '''写 parquet'''
    df.write.mode("overwrite").parquet(path=os.path.join(path_prefix, 'parquet/', path))

def write_orc(df, path,mode='overwrite'):
    df.write.format('orc').mode(mode).save(path)
    logger.info('write success')

def read_orc(path):
    try:
        df = spark.read.orc(path)
    except:
        logger.error('该路径下文件不存在：%s'%path)
        return None
    return df if df.take(1) else None

def save_mid_score(df, cp,table_name = 'relation_mid_score'):
    '''每个月的每个类型的基础分历史信息'''
    write_orc(df,add_save_path(table_name,cp=cp,root=root_home))
    logger.info('write table %s partition = %s down' % (table_name, cp,))

def read_mid_score(old_cp, label, table_name='relation_mid_score'):
    cols = ['sfzh1 sfzha','sfzh2 sfzhb','score','attenuation_coeff','label']
    df = read_orc(add_save_path(table_name,cp=old_cp,root=root_home)).where('label="%s"'%label)
    df = df.withColumn('attenuation_coeff',lit(float(attenuation[label])))
    return df.selectExpr(*cols) if df.take(1) else None

def get_attenuation_score(old,cur):
    if not old and not cur:
        return None
    if not old:
        return cur
    if not cur:
        res = old.selectExpr('sfzha sfzh1','sfzhb sfzh2','attenuation_coeff*score score','label')
        return res
    else:
        all_sfzh = old.selectExpr('sfzha sfzh1', 'sfzhb sfzh2').unionAll(cur.select('sfzh1', 'sfzh2')).drop_duplicates()
        cond1 = [all_sfzh.sfzh1==old.sfzha,all_sfzh.sfzh2==old.sfzhb]

        tmp_df1 = all_sfzh.join(old, cond1, 'left') \
            .selectExpr('sfzh1', 'sfzh2', 'score', 'attenuation_coeff','label')

        cond2 = [tmp_df1.sfzh1 == cur.sfzh1, tmp_df1.sfzh2 == cur.sfzh2]
        tmp_df2 = tmp_df1.join(cur,cond2,'left') \
                .select(tmp_df1.sfzh1,tmp_df1.sfzh2,tmp_df1.score,tmp_df1.attenuation_coeff,tmp_df1.label.alias('old_label'),cur.label,cur.score.alias('curr_score'))

        tmp_df2 = tmp_df2.na.fill({'score':0.00,'curr_score':0.00,'attenuation_coeff':0.00,'label':''})
        tmp = tmp_df2.selectExpr('sfzh1 ','sfzh2 ','(score*attenuation_coeff+curr_score) as score ','coalesce_str(old_label,label) label')
        return tmp

## 每种关系的定义分数
score = {
    'Allcall': 10,
    'Allmsg': 7,
    'Sendpackage':3,
    'Inf_with_airline_travel': 8,
    'Inf_with_trainline_travel': 7,
    'Inf_with_internetbar_surfing': 5,
    'Inf_same_hotel_house': 10
}

## 每种关系每月衰减系数
attenuation = {
    'Allcall': 0.5,
    'Allmsg': 0.5,
    'Sendpackage':0.5,
    'Inf_with_airline_travel': 0.8,
    'Inf_with_trainline_travel': 0.7,
    'Inf_with_internetbar_surfing': 0.6,
    'Inf_same_hotel_house': 0.9438743126816935
}

## label与表的映射关系
table_info = OrderedDict()
table_info['relation_telcall_month'] = 'Allcall'
table_info['relation_telmsg_month'] = 'Allmsg'
table_info['relation_post_month'] = 'Sendpackage'
table_info['relation_withair_month'] = 'Inf_with_airline_travel'
table_info['relation_withtrain_month'] = 'Inf_with_trainline_travel'
table_info['relation_withinter_month'] = 'Inf_with_internetbar_surfing'
table_info['relation_samehotel_month'] = 'Inf_same_hotel_house'

def get_score(num, avg_std, label):
    #计算每一种关系类型的结果分
    result = score[label] if num >= avg_std else score[label] * num / avg_std
    return float(result)

spark.udf.register('get_score', get_score, DoubleType())

def get_dscore(score, degree, meanD):
    '''得到平均归一化分数'''
    result = score
    if degree / meanD < 1:
        result = score * degree / meanD
    return float(result)

spark.udf.register('get_dscore', get_dscore, DoubleType())

def get_score_by_label(df):
    # 获取均值和标准差
    if not df:
        return None
    avg_std = df.selectExpr('avg(num) avg', 'stddev(num) as std').collect()
    # 得到归一化基准分数S = μ+3σ；
    v = float(avg_std[0]['avg'] + 3 * avg_std[0]['std'])
    avg_std_df = df.withColumn('avg_std', lit(v))
    # 根据类型 计算对应分数
    col1 = ['sfzh1', 'sfzh2', 'get_score(num,avg_std,label) as score', 'label']
    col2 = ['start_person sfzh1', 'end_person sfzh2', 'get_score(num,avg_std,label) as score', 'label']
    cond = 'start_person' not in [col[0] for col in df.dtypes]
    tmp =  avg_std_df.selectExpr(*col1) if cond else avg_std_df.selectExpr(*col2)
    res = tmp.na.fill({'score':0.0})
    return res


def deal_familymate():
    '''
    处理家庭推理关系,取优先级最高的分
    '''
    # 同父
    table_list = [
        {'tablename': 'edge_find_common_father', 'sfzh1': 'sfzh1', 'sfzh2': 'sfzh2', 'label': 'Inf_same_father',
                   'score': 9.0}, # 同母
        {'tablename': 'edge_find_common_mother', 'sfzh1': 'sfzh1', 'sfzh2': 'sfzh2', 'label': 'Inf_same_mother',
         'score': 9.0}, # 同监护人
        # {'table': 'edge_find_common_guardian', 'sfzh1': 'sfzh1', 'sfzh2': 'sfzh2', 'label': 'Inf_same_guardianis',
        #  'score': 9.0}, # 同祖父母
        {'tablename': 'edge_find_common_grand', 'sfzh1': 'sfzh1', 'sfzh2': 'sfzh2', 'label': 'Inf_same_grandparents',
         'score': 8.0}, # 同曾祖父母
        {'tablename': 'edge_find_common_greatgrand', 'sfzh1': 'sfzh1', 'sfzh2': 'sfzh2',
         'label': 'Inf_same_greatgrandparents', 'score': 8.0}
        ]

    tmp_sql = 'select {sfzh1} sfzh1, {sfzh2} sfzh2, "{label}" label, {score} score from {tablename}'

    union_df = create_extend_uniondf(spark,table_list,tmp_sql,root=save_root)

    union_df.createOrReplaceTempView('tmp')
    sql = ''' select sfzh1, sfzh2, max(score) score, concat_ws(',', collect_set(label)) label 
                    from tmp group by sfzh1, sfzh2 '''
    df = spark.sql(sql)
    write_orc(df,add_save_path('edge_familymate',root=save_root))

# 直接关系
table_list = [
    # 配偶
    {'table': 'edge_person_spouse_person', 'sfzh1': 'sfzhnan', 'sfzh2': 'sfzhnv', 'label': 'Spouse', 'score': 10.0},
    # 父亲
    {'table': 'edge_person_fatheris_person', 'sfzh1': 'sfzh', 'sfzh2': 'fqsfzh', 'label': 'Fatheris', 'score': 10.0},
    # 母亲
    {'table': 'edge_person_motheris_person', 'sfzh1': 'sfzh', 'sfzh2': 'mqsfzh', 'label': 'Motheris', 'score': 10.0},
    # 监护人
    {'table': 'edge_person_guardianis_person', 'sfzh1': 'sfzh', 'sfzh2': 'jhrsfzh', 'label': 'Guardianis','score': 10.0},
    # 同事    源数据未更新
    # {'table': 'edge_company_workmate', 'sfzh1': 'start_person', 'sfzh2': 'end_person', 'label': 'Inf_same_company',
    #  'score': 6.0},
    # 同学    源数据未更新
    # {'table': 'edge_schoolmate', 'sfzh1': 'start_person', 'sfzh2': 'end_person', 'label': 'Inf_schoolmate',
    #  'score': 6.0},
    # 家庭推理关系 取最高分 权重依次降低同父，同母，同监护人，同祖父母，同曾祖父母
    {'table': 'edge_familymate', 'sfzh1': 'sfzh1', 'sfzh2': 'sfzh2', 'label': 'label','score': 'score'}
    ]


def base_score(cp):
    dfs = []
    for table, label in table_info.items():
        logger.info('table is: %s and label is: %s ' % (table, label,))
        df = read_orc(add_save_path(table,cp=cp,root=root_home))
        if df:
            df = df.withColumn('label', lit(label))
        # 获取间接关系基础分
        df2 = get_score_by_label(df)
        dfs.append(df2)

    for info in table_list:
        logger.info('dealing %s'% info['table'])
        tmp_sql = 'select {sfzh1} sfzh1,{sfzh2} sfzh2,{score} score,"{label}" label from {table}'
        sql = tmp_sql.replace('"','').format(**info)  if info['table']=='edge_familymate' else tmp_sql.format(**info)
        create_tmpview_table(spark,info['table'],root=save_root)
        df1 = spark.sql(sql)
        dfs.append(df1)
        # if info['label'] not in ['Fatheris','Motheris','Guardianis']:
            ## 除了上述关系，其他的都需要做成双向边
        tmp_sql = 'select {sfzh2} sfzh1,{sfzh1} sfzh2,{score} score,"{label}" label from {table}'
        sql = tmp_sql.replace('"', '').format(**info) if info['table'] == 'edge_familymate' else tmp_sql.format(**info)
        df2 = spark.sql(sql)
        dfs.append(df2)
    ## 保存每月的每种关系类型的得分 中间表
    union_df = reduce(lambda x,y:x.unionAll(y),filter(lambda df:df is not None,dfs))
    mid_df = union_df.withColumn('cp',lit(cp))
    save_mid_score(mid_df, cp)
    # 合并所有类型节点 和值作为基础分
    score_df = union_df.groupby('sfzh1', 'sfzh2').agg(sum("score").alias("score"),concat_ws(',', collect_set("label")).alias("label"))

    write_parquet(score_df, 'score_df')
    s_df = read_parquet('score_df')
    logger.info('score_df')

    tmp_df = s_df.select(s_df.sfzh1.alias('sfzh'), s_df.score) \
        .unionAll(s_df.select(s_df.sfzh2.alias('sfzh'), s_df.score))

    # 取最大值 极值  degree
    ms_df = tmp_df.groupby("sfzh").agg(sum("score").alias("s_score"),count('score').alias('degree'))

    df1 = s_df.join(ms_df, ms_df['sfzh']==s_df['sfzh1'], 'left') \
        .selectExpr('sfzh1','sfzh2','score','s_score s_score_zb','degree degree_zb','label')
    df2 = df1.join(ms_df, ms_df['sfzh']==df1['sfzh2'], 'left') \
        .selectExpr('sfzh1','sfzh2','score','s_score_zb','degree_zb','s_score s_score_tb','degree degree_tb','label')

    # 自比： 100*原始分/自比总分   他比：100*原始分/他比总分
    df = df2.selectExpr('sfzh1', 'sfzh2', 'label', 'degree_zb','degree_tb',
                        '100*score/s_score_zb as zb_score', '100*score/s_score_tb as tb_score','score weight_score')

    #节点度数归一化
    res = df.selectExpr('sfzh1', 'sfzh2', 'label', 'round(get_dscore(zb_score,degree_zb,40),2) as zb_score',
                         'round(get_dscore(tb_score,degree_tb,40),2) as tb_score','weight_score')
    res = res.withColumn('cp',lit(cp))
    write_orc(res,add_save_path('relation_history_score',cp=cp,root=root_home))
    logger.info('base_score down')

def next_score(old_cp, cp):
    exists_files = []
    for table, label in table_info.items():
        logger.info('table is: %s and label is: %s ' % (table, label,))
        df = read_orc(add_save_path(table, cp=cp, root=root_home))
        if df:
            df = df.withColumn('label', lit(label))
        ##获取上月历史数据计算新的衰减分数,对结果进行衰减相加
        old_tmp = read_mid_score(old_cp, label)
        curr_tmp = get_score_by_label(df)
        tmp = get_attenuation_score(old_tmp,curr_tmp)
        if tmp:
            exists_files.append(table)
            write_parquet(tmp,table)

    # ## 处理推理家庭关系，得到结果分与label合集
    for info in table_list:
        logger.info('dealing %s'% info['table'])
        tmp_sql = 'select {sfzh1} sfzh1,{sfzh2} sfzh2,{score} score,"{label}" label from {table}'
        sql = tmp_sql.replace('"', '').format(**info) if info['table'] == 'edge_familymate' else tmp_sql.format(**info)
        create_tmpview_table(spark,info['table'],root=save_root)
        write_parquet(spark.sql(sql),info['table'])
        exists_files.append(info['table'])
        # if info['label'] not in ['Fatheris','Motheris','Guardianis']:
            ## 除了上述关系，其他的都需要做成双向边
        tmp_sql = 'select {sfzh2} sfzh1,{sfzh1} sfzh2,{score} score,"{label}" label from {table}'
        sql = tmp_sql.replace('"', '').format(**info) if info['table'] == 'edge_familymate' else tmp_sql.format(**info)
        write_parquet(spark.sql(sql), info['table']+'_1')
        exists_files.append(info['table']+'_1')
    dfs = []
    for table in exists_files:
        dfs.append(read_parquet(table))
    union_df = reduce(lambda x, y: x.unionAll(y), filter(lambda df:df is not None,dfs))
    mid_df = union_df.withColumn('cp', lit(cp))
    save_mid_score(mid_df, cp)
    # 合并所有类型节点 和值作为基础分
    union_df = read_orc(add_save_path('relation_mid_score',cp=cp,root=root_home))
    score_df = union_df.groupby('sfzh1', 'sfzh2').agg(sum("score").alias("score"),concat_ws(',', collect_set("label")).alias("label"))
    write_parquet(score_df, 'score_df')
    s_df = read_parquet('score_df')

    tmp_df = s_df.select(s_df.sfzh1.alias('sfzh'), s_df.score) \
        .unionAll(s_df.select(s_df.sfzh2.alias('sfzh'), s_df.score))

    ms_df = tmp_df.groupby("sfzh").agg(sum("score").alias("s_score"), count('score').alias('degree'))

    df1 = s_df.join(ms_df, ms_df['sfzh'] == s_df['sfzh1'], 'left') \
        .selectExpr('sfzh1', 'sfzh2', 'score', 's_score s_score_zb', 'degree degree_zb', 'label')
    df2 = df1.join(ms_df, ms_df['sfzh'] == df1['sfzh2'], 'left') \
        .selectExpr('sfzh1', 'sfzh2', 'score', 's_score_zb', 'degree_zb', 's_score s_score_tb', 'degree degree_tb',
                    'label')

    # 自比： 100*原始分/自比总分   他比：100*原始分/他比总分 权重分直接为基础分，两者得分一致
    df = df2.selectExpr('sfzh1', 'sfzh2', 'label', 'degree_zb', 'degree_tb',
                        '100*score/s_score_zb as zb_score', '100*score/s_score_tb as tb_score','score weight_score')

    # 节点度数归一化
    res = df.selectExpr('sfzh1', 'sfzh2', 'label', 'round(get_dscore(zb_score,degree_zb,40),2) as zb_score',
                        'round(get_dscore(tb_score,degree_tb,40),2) as tb_score','weight_score')
    res = res.withColumn('cp', lit(cp))
    write_orc(res, add_save_path('relation_history_score', cp=cp, root=root_home))
    logger.info('next score {}-{} down'.format(old_cp,cp))


def merge_data():
    ''' 合并并格式化按月的关系分 只取最近6个月的关系分 '''
    create_tmpview_table(spark,'relation_history_score',root=root_home)
    six_month_ago = add_month(date.today(),-6).strftime('%Y%m0000')

    sql = '''select sfzh1,sfzh2,concat_ws(';',data) data from(
                    select sfzh1,sfzh2,collect_set(data) data from 
                   (select sfzh1,sfzh2,concat_ws('-',label,zb_score,tb_score,
                   weight_score,substr(cp,0,8)) as data from relation_history_score where cp >= %s) tmp
                   group by sfzh1,sfzh2) tmp2'''%six_month_ago

    write_parquet(spark.sql(sql), 'merge_data')
    logger.info('merge_data down ')

    df = read_parquet('merge_data')
    df.persist()
    last_cp = spark.sql('select max(cp) from relation_history_score').collect()[0][0]

    sql = ''' select sfzh1, sfzh2, tb_score,zb_score,weight_score from relation_history_score where cp=%s '''%last_cp
    df2 = spark.sql(sql)
    logger.info('df2 down')
    res = df.join(df2,['sfzh1','sfzh2'],'left') \
        .select(df.sfzh1,df.sfzh2,df.data,df2.tb_score.alias('last_tb_score'),df2.zb_score.alias('last_zb_score'),df2.weight_score)
    df.unpersist()
    write_orc(res, add_save_path("relation_score_res",root=save_root))
    logger.info('relation_score_res down ')

def add_month(dt, months):
    month = dt.month - 1 + months
    year = dt.year + month / 12
    month = month % 12 + 1
    day = 1
    return dt.replace(year=year, month=month, day=day)

def auto_exec():
    '''
    只需要执行本月和前一个月就可以了，改动如下
    '''
    now_time = datetime.datetime.today()
    first_day = datetime.date(now_time.year, now_time.month, 1)

    start_date = first_day - datetime.timedelta(days=31)
    end_date = first_day - datetime.timedelta(days=1)

    start_month = datetime.date(start_date.year, start_date.month,1).strftime('%Y%m0000')
    end_month = datetime.date(end_date.year, end_date.month,1).strftime('%Y%m0000')
    next_score(start_month, end_month)
    logger.info('next_score(%s,%s) down'%(end_month, start_month))

def get_history():
    start_date = date(2018,8,1)
    end_date = date(2019,4,1)
    deal_list = []
    while start_date <= end_date:
        date_str = start_date.strftime('%Y%m0000')
        deal_list.append(int(date_str))
        start_date = add_month(start_date,1)
    for i in range(0,len(deal_list)-1):
        next_score(deal_list[i], deal_list[i+1])

if __name__ == "__main__":
    logger.info('========================start time:%s=========================='
                % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # deal_familymate()
    # base_score(2019100000)
    # next_score(2019100000,2019110000)
    # next_score(2019110000,2019120000)
    # next_score(2019120000,2020010000)
    # next_score(2020010000,2020020000)
    # next_score(2020020000,2020030000)
    # next_score(2020030000,2020040000)
    # next_score(2020040000,2020050000)
    # next_score(2020050000,2020060000)
    # auto_exec()
    merge_data()


    logger.info('========================end time:%s=========================='
                % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

