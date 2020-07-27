# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 15:35
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : jg_info.py
# @Software: PyCharm
# @content : 全图数据调度

import sys, os
import time, copy, re, math
from datetime import datetime, timedelta, date
import threading
import json
import logging
import subprocess
import signal

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
reload(sys)
sys.setdefaultencoding('utf-8')

EXEC_HOME = r'/opt/workspace/sx_graph/online'
os.getenv('PATH')
os.chdir(EXEC_HOME) # 设置命令执行目录

## 顺序执行，不能调换顺序
relate_files = [
               'airline',
               'call_msg',
               'car_etl',
               'case',  # todo 没有权限
               'company',
               'family',
               'hotel',
               'imsi_imei', # todo 文件数太多
               'internet',
               'jail',
               'package',
               'person',
               'person_open_phone', ## todo 没有权限
               'school',
               'train',
               'phonenumber',
            ]
            
tuili_files=[
            'with_travel',
            'with_interbar',
            'zhusu',
            ]


statics_files = ['jg','data_count']

PROP = {"user": "root",
        "password": "BBDerscom!@#",
        "driver": "com.mysql.jdbc.Driver",
        "db_name": "pangu"}

tmp_command = '''
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
                --py-files common.py,parseUtil.py,person_schema_info.py,jg_info.py,person_link_phone_info.py \
                --files table_schema_indices.csv \
                {}.py
                '''

def exec_command(command):
    subprocess.call('source /etc/profile', shell=True)
    retcode = subprocess.call(command,shell=True)
    if retcode == 0:
        logger.info('%s exec success' %command.replace('                ',''))
    else:
        logger.info('%s exec failed' %command.replace('                ',''))
    return retcode


def exec_files(files):
    for file in files:
        command = tmp_command.format(file)
        retcode = exec_command(command)
        if retcode != 0:
            logger.error('exec file %s failed exit with stutas 1'%file)
            exit(1)


if __name__ == '__main__':
    '''
    定期的全量实名制数据更新,图数据更新，
    '''
    logger.info('========================start deal all data time:%s==========================' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
    #初始化数据库信息
    
    # ★★此脚本执行顺序  不可更改★★
    # exec_files(relate_files)
    # exec_files(tuili_files)
    exec_files(statics_files)



    

    
    
    
    logger.info('========================end deal all data time:%s==========================' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
# nohup python /home/bbd/workspace/online/schedu_all.py > /home/bbd/workspace/online/logs/all.log 2>&1 &