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


EXEC_HOME = r'/opt/workspace/sx_graph/relation_database'
os.getenv('PATH')
os.chdir(EXEC_HOME) # 设置命令执行目录


def exec_command(command):
    subprocess.call('source /etc/profile', shell=True)
    retcode = subprocess.call(command,shell=True)
    if retcode == 0:
        logger.info('%s exec success' %command.replace('                ',''))
    else:
        logger.info('%s exec failed' %command.replace('                ',''))




tmp_comnand = """
    spark-submit --master yarn-cluster --archives hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/mini.zip#mini,hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/jdk-8u172-linux-x64.tar.gz#jdk --conf "spark.executorEnv.JAVA_HOME=jdk/jdk1.8.0_172" --conf "spark.yarn.appMasterEnv.JAVA_HOME=jdk/jdk1.8.0_172" --conf "spark.executorEnv.PYSPARK_PYTHON=mini/mini_pyspark/bin/python" --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=mini/mini_pyspark/bin/python" --conf "spark.executorEnv.PYSPARK_DRIVER=mini/mini_pyspark/bin/python" --conf "spark.yarn.appMasterEnv.PYSPARK_DRIVER=mini/mini_pyspark/bin/python" --queue root.slmp.shulianmingpin 1_3_long_run_update.py %s %s > compute_%s%s.log
"""

compute_args = [
    [7,20],
    [7,15],
    [7,10],
    [7,5],
]


def deal_history():
    for args in compute_args:
        month = args[0]
        day = args[1]
        command =tmp_comnand%(month,day,month,day)
        exec_command(command)

if __name__ == '__main__':
    deal_history()


