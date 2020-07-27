# -*- coding: utf-8 -*-
# @content : 涉毒数据清洗入图

import logging
import sys
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().set('spark.driver.maxResultSize', '30g')
conf.set('spark.yarn.am.cores', 5)
conf.set('spark.executor.memory', '15g')
conf.set('spark.executor.instances', 30)
conf.set('spark.executor.cores', 8)
conf.set('spark.dynamicAllocationenabled', 'false')
conf.set('spark.executor.extraJavaOptions', '-XX:+UseG1GC')

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()


class SdParser:

    def __init__(self, file_path, groupcall_path, vertex_phone_save_path, groupcall_save_path, vertex_person_path,
                 vertex_person_save_path, edge_smz_save_path, vertex_phone_path, edge_smz_path):
        self.file_path = file_path
        self.groupcall_path = groupcall_path
        self.groupcall_save_path = groupcall_save_path
        self.vertex_phone_save_path = vertex_phone_save_path
        self.vertex_person_save_path = vertex_person_save_path
        self.vertex_person_path = vertex_person_path
        self.edge_smz_save_path = edge_smz_save_path
        self.vertex_phone_path = vertex_phone_path
        self.edge_smz_path = edge_smz_path

    def rdd(self):
        lineRdd = spark.sparkContext.textFile(self.file_path)
        reload(sys)
        sys.setdefaultencoding('utf-8')
        return lineRdd.map(lambda line: line.split(",")).map(lambda line:
                                                             (line[0].encode('utf-8').strip(),
                                                              line[2].encode('utf-8').strip(),
                                                              line[1].encode('utf-8').strip()))

    def df(self):
        fields = [StructField("sfzh", StringType(), True), StructField("phone", StringType(), True),
                  StructField("xm", StringType(), True)]
        schema = StructType(fields)
        return spark.createDataFrame(self.rdd(), schema)

    def person_df(self):
        return spark.read.orc(self.vertex_person_path)

    def smz_df(self):
        return spark.read.orc(self.edge_smz_path)

    def phone_df(self):
        return spark.read.orc(self.vertex_phone_path)

    def groupcall_df(self):
        return spark.read.orc(self.groupcall_path)

    def write_vertex_person(self):
        personDf = self.person_df()
        df1 = self.df()
        df1.createOrReplaceTempView("view_file")
        personDf.createOrReplaceTempView("view_person")
        sql = '''
               select  person.* from view_file vfile left join view_person person on vfile.sfzh=person.zjhm
            '''
        res = spark.sql(sql)
        res.write.format('orc').mode("overwrite").save(self.vertex_person_save_path)

    def write_edge_smz(self):
        smzDf = self.smz_df()
        df1 = self.df()
        df1.createOrReplaceTempView("view_file")
        smzDf.createOrReplaceTempView("view_smz")
        sql = '''
             select smz.* from view_file vfile left join view_smz smz on vfile.sfzh=smz.start_perosn
           '''
        res = spark.sql(sql)
        res.write.format('orc').mode("overwrite").save(self.edge_smz_save_path)

    def write_vertex_phone_1(self):
        df1 = self.df()
        df2 = self.groupcall_df()
        df1.createOrReplaceTempView("view_phone")
        df2.createOrReplaceTempView("view_groupcall")
        logger.info("===========sql==========")

        phoneDf1 = spark.sql(
            "select distinct groupcall.start_phone from view_phone as vphone , view_groupcall as groupcall where vphone.phone=groupcall.start_phone")

        phoneDf1.show(10)
        logger.info("===========show==========")
        phoneDf1.write.format('orc').mode("overwrite").save(self.vertex_phone_save_path)

    def write_vertex_phone(self):
        df1 = self.df()
        df2 = self.groupcall_df()
        df1.createOrReplaceTempView("view_phone")
        df2.createOrReplaceTempView("view_groupcall")
        sql1 = "select distinct groupcall.start_phone as phone from view_phone as vphone , view_groupcall as groupcall where vphone.phone=groupcall.start_phone"
        phoneDf1 = spark.sql(sql1)
        sql2 = "select distinct groupcall.end_phone as phone from view_phone as vphone , view_groupcall as groupcall where vphone.phone=groupcall.end_phone"

        sql3 = "select distinct vphone.phone as phone from view_phone as vphone , view_groupcall as groupcall where (vphone.phone=groupcall.start_phone)";
        sql4 = "select distinct vphone.phone as phone from view_phone as vphone , view_groupcall as groupcall where (vphone.phone=groupcall.end_phone)";
        phoneDf2 = spark.sql(sql2)
        phoneDf3 = phoneDf1.union(phoneDf2)
        phoneDf4 = spark.sql(sql3)
        phoneDf5 = spark.sql(sql4)
        phoneDf6 = phoneDf4.union(phoneDf5)
        phoneDf7 = phoneDf6.union(phoneDf3)
        phone_df = self.phone_df()
        phoneDf7.createOrReplaceTempView("phone")
        phone_df.createOrReplaceTempView("data_phone")
        query_sql = '''
                  select p1.phone, p2.province,p2.city from phone as p1 left join data_phone as p2 on p1.phone=p2.phone
                '''
        phoneDf8 = spark.sql(query_sql)
        logger.info("===========showCnt1:%d==========" % (phoneDf6.count()))
        logger.info("===========showCnt2:%d==========" % (phoneDf8.count()))
        phoneDf8.distinct().write.format('orc').mode("overwrite").save(self.vertex_phone_save_path)

    def write_edge_groupcall_file(self):
        df1 = self.df()
        df2 = self.groupcall_df()
        df1.createOrReplaceTempView("view_phone")
        df2.createOrReplaceTempView("view_groupcall")
        sql1 = "select groupcall.*  from view_phone as vphone , view_groupcall as groupcall where vphone.phone=groupcall.start_phone"
        groupcallDf1 = spark.sql(sql1)
        sql2 = "select groupcall.*  from view_phone as vphone , view_groupcall as groupcall where vphone.phone=groupcall.end_phone"
        groupcallDf2 = spark.sql(sql2)
        groupcallDf3 = groupcallDf1.union(groupcallDf2)
        groupcallDf3.write.format('orc').mode("overwrite").save(self.groupcall_save_path)


if __name__ == "__main__":
    file_path = "hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/sd_data.txt"
    groupcall_path = "hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_groupcall"
    vertex_person_path = "hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/vertex_person"
    vertex_person_save_path = "hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/vertex_person_sd"
    vertex_phone_save_path = "hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/vertex_phonenumber_sd"
    groupcall_save_path = "hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_groupcall_sd"
    edge_smz_save_path = "hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_person_smz_phone_sd"
    vertex_phone_path = "hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/vertex_phonenumber"
    edge_smz_path = "hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/extenddir/edge_person_smz_phone"
    p = SdParser(file_path, groupcall_path, vertex_phone_save_path, groupcall_save_path,
                 vertex_person_path, vertex_person_save_path, edge_smz_save_path, vertex_phone_path,edge_smz_path)


    #  logger.info("====开始生成vertex表=====at:%s" % (
    #   time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    # p.write_vertex_phone()
    #  logger.info("====结束生成vertex表=====at:%s" % (
        #     time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # logger.info("====开始生成groupcall表=====at:%s" % (
        #    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    #p.write_edge_groupcall_file()

    # p.write_vertex_person()
    p.write_edge_smz()
#  logger.info("====结束生成groupcall表=====at:%s" % (
#     time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
