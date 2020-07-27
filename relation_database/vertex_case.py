# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : vertex_case.py
# @Software: PyCharm
# @content : 案件节点信息

import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession
# from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')
#warehouse_location = '/user/hive/warehouse/'
conf=SparkConf().set('spark.driver.maxResultSize', '2g')
conf.set('spark.executor.memoryOverhead', '3g')
conf.set('spark.yarn.am.cores', 4)
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/company'
save_root = 'relation_theme_extenddir'

def vertex_case():
    # ods_pol_crim_enfo_case_info	案件基本信息
    # case_no	案件编号
    # pol_case_no	警情编号
    # case_name	案件名称
    # cascla_name	案件类别
    # case_iden	案件标识
    # accep_time	受理时间
    # undo_case_date	撤案日期
    # disc_time	发现时间
    # disc_loca_addi_code	发现地点行政区划代码
    # disc_loca_admin_name	发现地点行政区划
    # disc_loca_addr_name	发现地点详址
    # case_addr_addi_code	发案地行政区划代码
    # case_addr_admin_name	发案地行政区划
    # case_addr_addr_name	发案地详址
    # case_loss_prop_brief_cond	案事件损失财物_简要情况
    # brief_case	简要案情
    # case_date	立案日期
    # repo_case_time	报案时间
    # case_attr_code	案件属性

    table_list = [

        {'asjbh': 'case_no', 'ajmc': 'case_name', 'asjfskssj': 'accep_time', 'asjfsjssj': 'undo_case_date',
         'asjly': 'cascla_name','ajlb': 'cascla_name', 'fxasjsj': 'disc_time', 'fxasjdd_dzmc': 'case_addr_addr_name',
         'jyaq': 'brief_case', 'ajbs': 'case_iden', 'larq': 'case_date','city':'case_addr_admin_name','link_app':'',
         'tablename': 'ods_pol_crim_enfo_case_info'}
    ]

    tmp_sql = '''
            select format_data({asjbh}) asjbh, {ajmc} ajmc, cast(format_timestamp({asjfskssj}) as bigint) asjfskssj,
            cast(valid_datetime({asjfsjssj}) as bigint) asjfsjssj, {asjly} asjly, {ajlb} ajlb,
            cast(format_timestamp({fxasjsj}) as bigint) fxasjsj, {fxasjdd_dzmc} fxasjdd_dzmc, {jyaq} jyaq,
            {ajbs} ajbs, cast(valid_datetime({larq}) as bigint) larq, {city} city, '{link_app}' link_app, '{tablename}' tablename
            from {tablename} where format_data({asjbh}) != ''
        '''

    sql = tmp_sql.format(**table_list[0])
    init(spark,table_list[0].get('tablename'),if_write=False)
    cols = ['asjbh','ajmc','asjfskssj','asjfsjssj','asjly','ajlb','fxasjsj','fxasjdd_dzmc','jyaq','ajbs','larq','city','link_app']
    df1 = spark.sql(sql).selectExpr(*cols)

    ## 真实案件数据合并
    df2 = read_orc(spark,add_save_path('vertex_case_zp',root=save_root)).selectExpr(*cols)
    df3 = read_orc(spark,add_save_path('vertex_case_steal',root=save_root)).selectExpr(*cols)

    df = df1.unionAll(df2).unionAll(df3).dropDuplicates(['asjbh'])

    write_orc(df,add_save_path('vertex_case',root=save_root))

def vertex_alert():
    table_list = [
        {'jqbh': 'pol_case_no', 'jqxz': 'pol_natu', 'jqfxdz': 'pol_case_occu_plac_addr_name',
         'jyqk': 'brief_cond', 'jjsj': 'cast(format_timestamp(acce_alarm_time) as bigint)',
         'tablename': 'ods_pol_crim_enfcas_pol_info','table_sort':1},
        {'jqbh': 'pol_case_no', 'jqxz': '""', 'jqfxdz': 'disc_loca_addr_name', 'jyqk': 'brief_case',
         'jjsj': 'cast(format_timestamp(repo_case_time) as bigint)', 'tablename': 'ods_pol_crim_enfo_case_info','table_sort':2},
    ]

    tmp_sql = '''
                select format_data({jqbh}) jqbh, {jqxz} jqxz, {jqfxdz} jqfxdz,
                {jyqk} jyqk,  {jjsj} jjsj, '{tablename}' tablename, {table_sort} table_sort
                from {tablename}
                where format_data({jqbh}) != ''
            '''

    union_df = create_uniondf(spark,table_list,tmp_sql)

    res = union_df.selectExpr('*','row_number() over (partition by jqbh order by table_sort asc) num') \
                        .where('num=1').drop('num').drop('table_sort')

    write_orc(res,add_save_path('vertex_alert',root=save_root))
    logger.info('vertex_alert down')


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    vertex_case()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))