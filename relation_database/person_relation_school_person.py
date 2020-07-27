# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : company.py
# @Software: PyCharm
# @content : 工商相关信息

from pyspark import SparkConf,StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging,sys
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')
#warehouse_location = '/user/hive/warehouse/'
conf=SparkConf().set('spark.driver.maxResultSize', '5g')
conf.set('spark.executor.memoryOverhead', '15g')
conf.set('spark.yarn.am.cores', 4)
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

path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/schoolmate'
save_root = 'relation_theme_extenddir'

def _min(a, b):
    return a if a < b else b

def _max(a, b):
    return a if a > b else b

spark.udf.register('get_min_long',_min, IntegerType())
spark.udf.register('get_max_long',_max, IntegerType())

def groupByExtend(x,y):
    x.extend(y)
    return x

def timestamp2str2(timestamp,default=''):
    '''时间戳转字符串时间格式'''
    try:
        timestamp = int(timestamp)
        return time.strftime('%Y', time.localtime(timestamp))
    except:
        pass
    return default
spark.udf.register('timestamp2str2', timestamp2str2, StringType())

def edge_schoolmate():
    '''
    同校的同学， 增加关系开始的最小和最大时间
    tips: 修改同学前置表的表结构 增加start_time,end_time 字段
    '''
    def same_school(data):
        ret = []
        end_school_id,rows = data
        school_id = end_school_id.split('_')[0]
        if len(rows)>1:
                for index,row in enumerate(rows):
                    sfzh1 = row

                    for i in range(index+1,len(rows)):
                        sfzh2 = rows[i]
                        if sfzh1!=sfzh2:
                            person1 = _min(sfzh1,sfzh2)
                            person2 = _max(sfzh1,sfzh2)
                            ret.append([school_id,person1,person2])
        if not ret:
            ret.append(['','',''])
        return ret
    #上大学 根据同学校 同专业 同年入学
    cols = ['start_person',"concat_ws('_',end_school_id,timestamp2str2(start_time)) as end_school_id", "start_time", "end_time"]
    person_attend_school = read_orc(spark,add_save_path('edge_person_attend_school',root=save_root)) \
                .where("start_time != 0").selectExpr(*cols)
    person_attend_school.persist(StorageLevel.MEMORY_AND_DISK)

    #推理同学
    rdd=person_attend_school.rdd.map(lambda r:(r.end_school_id,[r.start_person])).reduceByKey(groupByExtend).flatMap(same_school)
    df_tmp = spark.createDataFrame(rdd, ["end_school_id","sfzh1", "sfzh2"]).drop_duplicates()
    df_tmp.persist(StorageLevel.MEMORY_AND_DISK)

    #关联上学时间
    person_attend_school.createOrReplaceTempView('attend_school')
    df_tmp.createOrReplaceTempView('school_mate')

    sql = '''
        select /*+ BROADCAST (attend_school)*/ a.sfzh1, a.sfzh2, get_max_long(b.start_time,c.start_time) start_time,
        get_min_long(b.end_time,c.end_time) end_time, a.end_school_id
        from school_mate a 
        inner join attend_school b on a.sfzh1=b.start_person and a.end_school_id=b.end_school_id
        inner join attend_school c on a.sfzh2=c.start_person and a.end_school_id=c.end_school_id
    '''

    tmp = spark.sql(sql).where('sfzh1 != sfzh2 and end_time >= start_time')

    # 关联学校名
    school_df = read_orc(spark,add_save_path('vertex_school',root=save_root)).where('format_data(name) != ""')
    res_tmp = tmp.join(school_df,tmp.end_school_id==school_df.school_id,'inner').select(tmp.sfzh1,tmp.sfzh2,tmp.start_time,tmp.end_time,school_df.name)
    res_tmp.createOrReplaceTempView("tmp")
    sql = '''
        select if(sfzh1<sfzh2,sfzh1,sfzh2) as start_person,if(sfzh1>sfzh2,sfzh1,sfzh2) as end_person, 
        min(start_time) as start_time, max(end_time) as end_time, 
        concat_ws(',',collect_set(name)) as name from tmp group by sfzh1,sfzh2
    '''
    res = spark.sql(sql)
    write_orc(res,add_save_path('edge_schoolmate',root=save_root))
    person_attend_school.unpersist()
    df_tmp.unpersist()

if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    edge_schoolmate()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))