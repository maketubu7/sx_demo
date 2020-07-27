# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : commonlib.py
# @Software: PyCharm
# @content : 两边的目录都为jg_data_standby

import os,sys,subprocess
import logging
from collections import OrderedDict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


EXEC_HOME = r'/opt/workspace/sx_graph/distcp_schedu'
os.getenv('PATH')
os.chdir(EXEC_HOME) # 设置命令执行目录
souce_root = '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_data_standby/'
target_root = 'hdfs://24.1.17.4:8020/user/shulian/graph_data/jg_data_standby/'

def exec_command(command):
    subprocess.call('source /etc/profile', shell=True)
    retcode = subprocess.call(command,shell=True)
    logger.info('retcode %s'%retcode)
    if retcode == 0:
        logger.info('%s exec success' %command.replace('                ',''))
    else:
        logger.info('%s exec failed' %command.replace('                ',''))
    return retcode

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
        with open('/opt/workspace/sx_graph/distcp_schedu/retry_load_jg_data.sh','w') as f:
            f.seek(0)
            f.truncate()
            if res:
                for file in res:
                    f.writelines('hadoop fs -rm -r -f %s%s \n'%(target_root,file))
                    f.writelines('hadoop distcp -m 30 -i %s%s %s \n'%(souce_root,file,target_root))

    def exec_retry():
        command = 'bash /opt/workspace/sx_graph/distcp_schedu/retry_load_jg_data.sh'
        with open('/opt/workspace/sx_graph/distcp_schedu/retry_load_jg_data.sh','r') as f:
            lines = len(f.readlines())
        if lines > 0:
            retcode = exec_command(command)
            return 1
        else:
            return 0

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
    exec_command('hdfs dfs -rm -r -f %s*'%target_root)
    exec_command('bash /opt/workspace/sx_graph/distcp_schedu/load_all_jg_data_standby.sh')
    while a:
        a = get_jg_data()