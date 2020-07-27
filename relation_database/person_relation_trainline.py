# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 14:54
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : train.py
# @Software: PyCharm
# @content : 火车相关信息

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging,sys
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')

conf=SparkConf().set('spark.driver.maxResultSize', '20g')
conf.set('spark.yarn.executor.memoryOverhead', '20g')
conf.set('soark.shuffle.partitions',800)
conf.set('spark.executor.memory', '10g')
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

##todo:all

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/train'
save_root = 'relation_theme_extenddir'

table_list = [
        {'tablename': 'ods_soc_traf_raista_entper', 'cc': 'train_no', 'fcrq': 'riding_date',
         'fz': 'dep_stat_rail_site_name', 'dz': 'terminus_rail_site_name', 'zjhm': 'cert_no',
         'sfd':'dep_stat_rail_site_name','mdd':'terminus_rail_site_name',
         'cxh': 'coach_no', 'zwh': 'seat_no','path':'edge_person_enter_trainline_detail',
         'cond':'riding_date>"20190101" and length(trim(train_no))>=2'},
        {'tablename': 'ods_soc_traf_raiway_saltic_data', 'cc': 'train_no', 'fcrq': 'riding_date',
         'sfd':'""','mdd':'""',
         'fz': 'dep_stat_desig', 'dz': 'arr_stat_desig', 'zjhm': 'cred_num', 'cxh': 'coach_no',
         'zwh': 'seat_no','cond':'riding_date>"20190101" and length(trim(train_no))>=2',
             'path':'edge_person_reserve_trainline_detail'},
    ]

def vertex_trainline():
    '''
    火车节点信息
    1、ODS_SOC_TRAF_RAISTA_ENTPER	火车站进站人员信息
        TRAIN_NO	车次
        DEP_TIME	发车时间
        RIDING_DATE	乘车日期
    2、ODS_SOC_TRAF_RAIWAY_SALTIC_DATA	铁路售票数据
        RIDING_DATE	乘车日期
        DEP_TIME	发车时间
        TRAIN_NO	车次
        TRAIN_NUM	列车车号
    :return:
    '''

    tmp_sql = '''select format_lineID({cc},{fcrq}) trianid,
                format_data({cc}) cc,cast(date2timestampstr({fcrq}) as bigint) fcrq,
                trim({sfd}) sfd, trim({mdd}) mdd,'{tablename}' tablename
                from {tablename} 
                where date2timestampstr({fcrq}) !='0' '''

    df = create_uniondf(spark,table_list,tmp_sql)

    df2 = df.drop_duplicates(['trianid'])
    write_orc(df2,add_save_path('vertex_trainline',root=save_root))
    logger.info('vertex_trainline down')

def edge_person_reserve_trainline_detail():
    '''
    人购买火车票信息
    1、ODS_SOC_TRAF_RAIWAY_SALTIC_DATA	铁路售票数据
        CRED_NUM	证件号码
        RIDING_DATE	乘车日期
        DEP_TIME	发车时间
        TRAIN_NO	车次
        TRAIN_NUM	列车车号
        DEP_STAT_DESIG	始发站_名称
        ARR_STAT_DESIG	到达站_名称
        TRAIN_TIC_NO	车票号码
        COACH_NO	车厢号
        SEAT_NO	座位号
        BOOK_PERS_CRED_NUM	订票人_证件号码
    2、ODS_SOC_TRAF_RAISTA_ENTPER	火车站进站人员信息
        CERT_NO	公民身份号码
        RIDING_DATE	乘车日期
        TRAIN_TIC_NO	车票号码
        TRAIN_NO	车次
        COACH_NO	车厢号
        SEAT_NO	座位号
        DEP_TIME	发车时间
        DEP_STAT_RAIL_SITE_NAME	始发站_铁路站点名称
        STAOFF_STAT_RAIL_SITE_NAME	出发站_铁路站点名称
        ARR_STAT_RAIL_SITE_NAME	到达站_铁路站点名称
        TERMINUS_RAIL_SITE_NAME	终点站_铁路站点名称
    :return:
    '''
    tmp_sql = '''
            select  format_zjhm({zjhm}) sfzh, format_lineID({cc},{fcrq}) trianid,
            format_data({cc}) cc, cast(date2timestampstr({fcrq}) as bigint) fcrq, format_data({cxh}) cxh,
            format_data({zwh}) zwh,format_data({fz}) fz,format_data({dz}) dz,'{tablename}' tablename
            from {tablename} WHERE
            verify_sfz({zjhm})=1 and date2timestampstr({fcrq})!='0' 
            '''

    for info in table_list:
        if info['tablename'] == tablename:
            init_history(spark, info['tablename'], import_times=import_times,if_write=False)
            sql = tmp_sql.format(**info)
            tmp_df = spark.sql(sql).repartition(20)
            write_orc(tmp_df, add_incr_path(info['path'], cp))
            logger.info('%s %s down' %(info['path'],cp))

def edge_person_reserve_trainline():
    '''
    人购买火车票信息
    1、ODS_SOC_TRAF_RAIWAY_SALTIC_DATA	铁路售票数据
        CRED_NUM	证件号码
        RIDING_DATE	乘车日期
        DEP_TIME	发车时间
        TRAIN_NO	车次
        TRAIN_NUM	列车车号
        DEP_STAT_DESIG	始发站_名称
        ARR_STAT_DESIG	到达站_名称
        TRAIN_TIC_NO	车票号码
        COACH_NO	车厢号
        SEAT_NO	座位号
        BOOK_PERS_CRED_NUM	订票人_证件号码
    2、ODS_SOC_TRAF_RAISTA_ENTPER	火车站进站人员信息
        CERT_NO	公民身份号码
        RIDING_DATE	乘车日期
        TRAIN_TIC_NO	车票号码
        TRAIN_NO	车次
        COACH_NO	车厢号
        SEAT_NO	座位号
        DEP_TIME	发车时间
        DEP_STAT_RAIL_SITE_NAME	始发站_铁路站点名称
        STAOFF_STAT_RAIL_SITE_NAME	出发站_铁路站点名称
        ARR_STAT_RAIL_SITE_NAME	到达站_铁路站点名称
        TERMINUS_RAIL_SITE_NAME	终点站_铁路站点名称
    :return:
    '''
    cols = ['sfzh', 'trianid', 'cc', 'fcrq', 'cxh', 'zwh', 'fz', 'dz','fcrq start_time','fcrq+18000 end_time']
    df1 = read_orc(spark,add_incr_path('edge_person_reserve_trainline_detail'))
    df2 = read_orc(spark,add_incr_path('edge_person_enter_trainline_detail'))
    df = df1.unionAll(df2)
    df2 = df.drop_duplicates(['sfzh','trianid']).selectExpr(*cols)
    write_orc(df2,add_save_path('edge_person_reserve_trainline',root=save_root))
    logger.info('edge_person_reserve_trainline down')

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    if len(sys.argv) == 4:
        import_times = sys.argv[1].split(',')
        cp = str(sys.argv[2])
        tablename = str(sys.argv[3])
        edge_person_reserve_trainline_detail()
    else:
        vertex_trainline()
        edge_person_reserve_trainline()


    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))