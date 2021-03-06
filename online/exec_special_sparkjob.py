# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 11:22
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : exec_his_sparkjob.py
# @Software: PyCharm
# @content : 处理历史数据 或每天的增量数据

import os,sys,subprocess
import time
from datetime import datetime,timedelta,date
import logging
from collections import OrderedDict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


EXEC_HOME = r'/opt/workspace/sx_graph/online'
os.getenv('PATH')
os.chdir(EXEC_HOME) # 设置命令执行目录


def timestamp2cp(timestamp,format="%Y%m%d00"):
    try:
        stamp = int(timestamp)
        d = datetime.fromtimestamp(stamp)
        return d.strftime(format)
    except:
        return ''

def add_path(tablename):
    '''格式化路径'''
    jz_file = ['dw_evt','voic_busi','phone_owner','roam_busi','ods_tb_cdr','vpmn_busi','dw_sms','elefen_calrec']
    ##数据治理 源文件地址/phoebus/_fileservice/users/zhjw/sjzl/daml/data/ods/
    ##技侦数据 源文件地址/phoebus/_fileservice/users/jz/jizhen/daml/data/ods/
    sjzl_path = '/phoebus/_fileservice/users/zhjw/sjzl/daml/data/ods/{}/import/'
    jz_path = '/phoebus/_fileservice/users/jz/jizhen/daml/data/ods/{}/import/'
    for file in jz_file:
        if file in tablename:
            return jz_path.format(tablename.upper())
    return sjzl_path.format(tablename.upper())

def get_cp_imports(tablename,cp):
    '''t-1当天的历史文件'''
    path = add_path(tablename)
    command = ''' /opt/app/hadoop-2.6.0/bin/hdfs dfs -ls %s | awk '{print $8}' > /opt/workspace/tmp_file/%s.tmp '''
    command = command % (path,tablename)
    os.system(command)
    yesterday = cp
    thistimes = []
    with open('/opt/workspace/tmp_file/%s.tmp'%tablename) as f:
        lines = f.readlines()
        for line in lines:
            thistime = line.split('/')[-1].replace('\n','')
            if timestamp2cp(thistime) == yesterday:
                thistimes.append(thistime)

    return thistimes

def get_all_importtime(tablename):
    '''所有的历史文件'''
    path = add_path(tablename)
    command = ''' hdfs dfs -ls %s | awk '{print $8}' > /opt/workspace/tmp_file/%s.tmp '''
    command = command % (path,tablename)
    os.system(command)
    thistimes = []
    with open('/opt/workspace/tmp_file/%s.tmp'%tablename) as f:
        lines = f.readlines()
        for line in lines:
            thistime = line.split('/')[-1].replace('\n','')
            if thistime and thistime != 'tmp':
                thistimes.append(thistime)

    return thistimes

def exec_command(command):
    subprocess.call('source /etc/profile', shell=True)
    retcode = subprocess.call(command,shell=True)
    if retcode == 0:
        logger.info('%s exec success' %command.replace('                ',''))
    else:
        logger.info('%s exec failed' %command.replace('                ',''))

filenames = OrderedDict()

# filenames['ods_pol_pub_dw_sms']='call_msg_new'
# filenames['ods_pol_sec_gsm_voic_busi']='call_msg_new'
# filenames['ods_pol_sec_vpmn_busi']='call_msg_new'
# filenames['ods_tb_cdr']='call_msg_new'
# filenames['ods_pol_sec_mobile_roam_busi']='call_msg_new'
# filenames['ods_pol_sec_fixlin_voic_busi']='call_msg_new'
filenames['ods_pol_pub_dw_evt']='dw_evt_detail'
# filenames['ods_soc_lgs_expr_integ_info']='package'

increase_command = '''
            spark-submit --master yarn-cluster \
            --archives hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/mini.zip#mini,hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/jdk-8u172-linux-x64.tar.gz#jdk \
            --conf "spark.executorEnv.JAVA_HOME=jdk/jdk1.8.0_172" \
            --conf "spark.yarn.appMasterEnv.JAVA_HOME=jdk/jdk1.8.0_172" \
            --conf "spark.executorEnv.PYSPARK_PYTHON=mini/mini_pyspark/bin/python" \
            --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=mini/mini_pyspark/bin/python" \
            --conf "spark.executorEnv.PYSPARK_DRIVER=mini/mini_pyspark/bin/python" \
            --conf "spark.yarn.appMasterEnv.PYSPARK_DRIVER=mini/mini_pyspark/bin/python" \
            --conf "hive.metasotre.uris=thrift://24.1.11.2:10005" \
            --conf "hive.server2.thrift.port=10000" \
            --conf "spark.sql.warehouse.dir=/phoebus/_fileservice/users/slmp/shulianmingpin/" \
            --queue root.slmp.shulianmingpin \
            --py-files common.py,parseUtil.py,person_schema_info.py \
            --files table_schema_indices.csv \
            %s.py %s'''

source_command = '''
                spark-submit --master yarn-cluster \
                --archives hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/mini.zip#mini,hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/jdk-8u172-linux-x64.tar.gz#jdk \
                --conf "spark.executorEnv.JAVA_HOME=jdk/jdk1.8.0_172" \
                --conf "spark.yarn.appMasterEnv.JAVA_HOME=jdk/jdk1.8.0_172" \
                --conf "spark.executorEnv.PYSPARK_PYTHON=mini/mini_pyspark/bin/python" \
                --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=mini/mini_pyspark/bin/python" \
                --conf "spark.executorEnv.PYSPARK_DRIVER=mini/mini_pyspark/bin/python" \
                --conf "spark.yarn.appMasterEnv.PYSPARK_DRIVER=mini/mini_pyspark/bin/python" \
                --conf "hive.metasotre.uris=thrift://24.1.11.2:10005" \
                --conf "hive.server2.thrift.port=10000" \
                --conf "spark.sql.warehouse.dir=/phoebus/_fileservice/users/slmp/shulianmingpin/" \
                --queue root.slmp.shulianmingpin \
                --py-files common.py,parseUtil.py,person_schema_info.py \
                --files table_schema_indices.csv \
                %s.py %s %s %s
                '''

# increase_files = ['call_msg','hotel','package']
increase_files = ['call_msg_new']

def deal_history(cps):
    '''
    处理每天的增量数据
    :return:
    '''
    for cp in cps:
        ## 原始数据
        # for tablename, filename in filenames.items():
        #     times = get_cp_imports(tablename,cp)
        #     if times:
        #         expr = ','.join(times)
        #         command = source_command%(filename, expr, cp, tablename)
        #         exec_command(command)
        #     else:
        #         logger.info('%s 分区%s 没有导入数据,在当前运行时间%s '%(tablename,cp,time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())))
        ##每天关系明细表
        for filename in increase_files:
            command = increase_command %(filename,cp)
            exec_command(command)
            logger.info('%s %s down'%(filename,cp))

if __name__ == '__main__':
    ## start_cp = 2020052700 end_cp = 2020060300
    start_cp = sys.argv[1]
    end_cp = sys.argv[2]

    start_date = datetime.strptime(start_cp,'%Y%m%d00')
    end_date = datetime.strptime(end_cp,'%Y%m%d00')
    num = (end_date - start_date).days

    cps = [(start_date + timedelta(days=_)).strftime('%Y%m%d00') for _ in range(num)][::-1]
    # print cps
    deal_history(cps)








