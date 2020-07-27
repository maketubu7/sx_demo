# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : commonlib.py
# @Software: PyCharm
# @content : 前置筛选配置

import os,sys,subprocess
import logging
from collections import OrderedDict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


EXEC_HOME = r'/opt/workspace/sx_graph/data_inteface'
os.getenv('PATH')
os.chdir(EXEC_HOME) # 设置命令执行目录

def exec_command(command):
    subprocess.call('source /etc/profile', shell=True)
    retcode = subprocess.call(command,shell=True)
    logger.info('retcode %s'%retcode)
    if retcode == 0:
        logger.info('%s exec success' %command.replace('                ',''))
    else:
        logger.info('%s exec failed' %command.replace('                ',''))
    return retcode

def format_args(info):
    pass_key = ['user','params','applicationId','is_call_basic','is_person_basic']
    conds = []
    for key, v in info.items():
        if key not in pass_key and v:
            conds.append('--%s %s'%(key,v))
    return ' '.join(conds)

def build_command(**kwargs):
    #queue root.slmp.shulianmingpin
    type = kwargs.pop('target')
    task_id = kwargs.get('task_id')
    cond_str = format_args(kwargs)
    if type == 'person':
        person_command = '''
                    spark-submit --master yarn-cluster \
                    --archives hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/mini.zip#mini,hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/jdk-8u172-linux-x64.tar.gz#jdk \
                    --conf "spark.executorEnv.JAVA_HOME=jdk/jdk1.8.0_172" \
                    --conf "spark.yarn.appMasterEnv.JAVA_HOME=jdk/jdk1.8.0_172" \
                    --conf "spark.executorEnv.PYSPARK_PYTHON=mini/mini_pyspark/bin/python" \
                    --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=mini/mini_pyspark/bin/python" \
                    --conf "spark.executorEnv.PYSPARK_DRIVER=mini/mini_pyspark/bin/python" \
                    --conf "spark.yarn.appMasterEnv.PYSPARK_DRIVER=mini/mini_pyspark/bin/python" \
                    --name %s \
                    --queue root.slmp.shulianmingpin \
                    --py-files commonlib.py,common.py \
                    --jars mysql-connector-java-5.1.39.jar \
                    advanced_person_filter.py %s > ./logs/task_%s.log
                    '''%(task_id,cond_str,task_id)
        return person_command
    # --queue root.slmp.shulianmingpin \
    elif type == 'phone':
        phone_command = '''
                        spark-submit --master yarn-cluster \
                        --archives hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/mini.zip#mini,hdfs://ngpcluster/phoebus/_fileservice/users/slmp/shulianmingpin/jdk-8u172-linux-x64.tar.gz#jdk \
                        --conf "spark.executorEnv.JAVA_HOME=jdk/jdk1.8.0_172" \
                        --conf "spark.yarn.appMasterEnv.JAVA_HOME=jdk/jdk1.8.0_172" \
                        --conf "spark.executorEnv.PYSPARK_PYTHON=mini/mini_pyspark/bin/python" \
                        --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=mini/mini_pyspark/bin/python" \
                        --conf "spark.executorEnv.PYSPARK_DRIVER=mini/mini_pyspark/bin/python" \
                        --conf "spark.yarn.appMasterEnv.PYSPARK_DRIVER=mini/mini_pyspark/bin/python" \
                        --name %s \
                        --queue root.slmp.shulianmingpin \
                        --py-files commonlib.py,common.py \
                        --jars mysql-connector-java-5.1.39.jar \
                        advanced_phone_filter.py %s
                        '''%(task_id,cond_str)
        return phone_command

def build_task_params(**kwargs):
    prop = OrderedDict()
    prop['task_id'] = kwargs.get('task_id')
    prop['target_type'] = kwargs.get('target')
    prop['search_range'] = 'all' if str(kwargs.get('is_all')) == '1' else 'upload'
    prop['user'] = kwargs.get('user')
    prop['params'] = kwargs.get('params')
    prop['applicationId'] = kwargs.get('applicationId')
    return prop


def build_kill_command(applicationId):
    if applicationId:
        # command = ''' yarn application -list | grep %s | awk '{print $1}' | grep application | xargs yarn application -kill ''' %task_id
        command = ''' yarn application -kill %s''' %applicationId
        return command
    return ''

def get_real_applicationId(task_id):
    '''得到任务的spark真实任务id'''
    if task_id:
        command = ''' yarn application -list | grep %s | awk '{print $1}' | grep application ''' % task_id
        stdout = subprocess.Popen(command, shell=True,stdout=subprocess.PIPE)
        res = stdout.stdout.read().split('\n')[0].strip()
        return res
    return ''

def get_jg_data():
    '''检查distcp 转发文件是否有丢失，并生成重执行文件
    /opt/workspace/sx_graph/data_inteface/retry_load_jg_data.sh'''
    def get_filesize_arr(filestr):
        keys = ['K','M','G','T']
        for key in keys:
            if key in filestr:
                index = filestr.index(key)
                return [filestr[index+1:],filestr[:index+1]]

    def get_diff_file(source,target):
        source_dict = OrderedDict()
        target_dict = OrderedDict()
        res = []
        for item in source:
            source_dict[item[0]] = item[1]
        for item in target:
            target_dict[item[0]] = item[1]

        source_files = source_dict.keys()
        target_files = target_dict.keys()

        for file in source_files:
            print file
            if file in target_files and source_dict[file] == target_dict[file]:
                print 'yes'
                pass
            else:
                print 'no'
                res.append(file)
        return res

    def create_execfile(res):
        with open('/opt/workspace/sx_graph/data_inteface/retry_load_jg_data.sh','w') as f:
            f.seek(0)
            f.truncate()
            if res:
                for file in res:
                    f.writelines('hadoop fs -rm -r -f %s%s \n'%(target_root,file))
                    f.writelines('hadoop distcp -m 30 -i %s%s %s \n'%(souce_root,file,target_root))

    def exec_retry():
        command = 'bash /opt/workspace/sx_graph/data_inteface/retry_load_jg_data.sh'
        with open('/opt/workspace/sx_graph/data_inteface/retry_load_jg_data.sh','r') as f:
            lines = len(f.readlines())
        if lines > 0:
            retcode = exec_command(command)
            return 1
        else:
            return 0

    souce_root = '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_data/'
    target_root = 'hdfs://24.1.17.4:8020/user/shulian/graph_data/jg_data/'
    exec_command('hadoop fs -rm -r -f %s.distcp*'%target_root)

    command = ''' hadoop fs -du -h %s '''%souce_root
    stdout = subprocess.Popen(command, shell=True,stdout=subprocess.PIPE)
    res = map(lambda a:a.replace(souce_root,'').replace(' ',''),stdout.stdout.read().split('\n')[:-1])
    res_source = map(lambda x:get_filesize_arr(x),res)
    command = ''' hadoop fs -du -h %s '''%target_root
    stdout = subprocess.Popen(command, shell=True,stdout=subprocess.PIPE)
    res = map(lambda a:a.replace(target_root,'').replace(' ',''),stdout.stdout.read().split('\n')[:-1])
    res_target= map(lambda x:get_filesize_arr(x),res)
    diff_files = get_diff_file(res_source,res_target)
    print diff_files
    create_execfile(diff_files)
    return exec_retry()



if __name__ == '__main__':
    a = 1
    exec_command('bash /opt/workspace/sx_graph/distcp_schedu/load_all_jg_data.sh')
    while a:
        a = get_jg_data()