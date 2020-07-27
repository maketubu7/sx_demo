# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : commonlib.py
# @Software: PyCharm
# @content : mysql工具类

import os,sys
import time
from datetime import datetime
import pymysql,json

reload(sys)
sys.setdefaultencoding('utf-8')
root_home = os.getcwd()

class propertiesTool:

    def __init__(self,filename):
        self._filename = os.path.join(root_home,filename)
        self._prop = {}

    def getProperties(self):
        with open(self._filename,mode='r') as f:
            lines = f.readlines()
            for line in lines:
                if line.startswith('#'):
                    continue
                args = line.split("=")
                self._prop[args[0]] = args[1].replace('\n','').replace('\r','')
        return self._prop

    def getPropertiesValue(self,name):

        try:
            return self._prop[name]
        except:
            return ''

class jdbcService:

    def __init__(self,prop):
        port = int(prop.pop('port'))
        self.conn = pymysql.connect(port=port,autocommit=True,use_unicode=True,**prop)
        self.cursor = self.conn.cursor()
        self.cursor.execute('set names utf8')
        self.cursor.execute('set character set utf8')
        self.cursor.execute('set character_set_connection=utf8')

    def stop(self):
        try:
            if self.conn and self.cursor:
                self.conn.close()
                self.cursor.close()
        except :
            pass

    def getAllZjhm(self,task_id):
        sql = ''' select node_num from tb_advanced_task_state where task_id='%s' '''%task_id
        self.cursor.execute(sql)
        zjhms = [_[0] for _ in self.cursor.fetchall()]
        return zjhms

    def getAllPhoneNumber(self,task_id):

        sql = ''' select node_num from tb_advanced_task_state where task_id=%s '''%task_id
        self.cursor.execute(sql)
        phones = [_[0] for _ in self.cursor.fetchall()]
        return phones

    def getMxTaskid(self):
        ''' 得到最大任务id '''
        sql = ''' select max(task_id) task_id from tb_advanced_task_state '''
        self.cursor.execute(sql)
        task_res = self.cursor.fetchone()
        today = time.strftime("%Y%m%d", time.localtime())
        try:
            task_id = task_res[0]
            if today in task_id:
                return str(int(task_id) + 1)
        except:
            return today + '0001'
        return today + '0001'

    def updateTaskRunning(self,**kwargs):
        ''' 插入任务状态 0 running, 及其相关参数 '''
        # try:
        task_id = kwargs.get('task_id')
        kwargs['search_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        exists_sql = ''' select count(1) num from tb_advanced_task_state where task_id = '%s' '''%task_id
        self.cursor.execute(exists_sql)
        exists = self.cursor.fetchone()[0]
        if exists:
            sql = ''' update tb_advanced_task_state set state = 0 where task_id = '%s' '''%task_id
        else:
            sql = u''' insert into tb_advanced_task_state (task_id, target_type,search_range,user,params,search_time,state) 
                values ('{task_id}','{target_type}','{search_range}','{user}','{params}','{search_time}',0)'''.format(**kwargs)
        self.cursor.execute(sql)
        # except:
        #     self.conn.rollback()

    def updateTaskState(self,task_id,state):
        '''更新任务状态 1 success -1 failed '''
        try:
            sql = ''' update tb_advanced_task_state set state=%d where task_id="%s" '''%(state,task_id)
            self.cursor.execute(sql)
        except:
            self.conn.rollback()

    def backfillTaskID(self,task_id,upload_id):
        ''' 回填任务id '''
        try:
            exists_sql = ''' select count(1) num from tb_task_link_info where task_id='%s' and upload_id=%s '''%(task_id,upload_id)
            self.cursor.execute(exists_sql)
            exists =self.cursor.fetchone()[0]
            if not exists:
                sql = ''' insert into tb_task_link_info (task_id, upload_id) values ('%s',%s) '''%(task_id,upload_id)
                self.cursor.execute(sql)
        except:
            self.conn.rollback()

    def getOrDelTaskState(self,task_id):
        ## 删除任务记录，并返回当前状态
        try:
            sql = ''' select state from tb_advanced_task_state where task_id = '%s' '''%task_id
            self.cursor.execute(sql)
            state  = self.cursor.fetchone()[0]
            delete_sql = ''' delete from tb_advanced_task_state where task_id = '%s' '''%task_id
            self.cursor.execute(delete_sql)
            return state
        except:
            self.conn.rollback()
            return 1

    def deleteAllTask(self,user):
        '''删除当前用户的所有任务'''
        all_ids = []
        try:
            all_sql = ''' select task_id, state from tb_advanced_task_state where user='%s' '''%user
            self.cursor.execute(all_sql)
            all_ids = list(self.cursor.fetchall())
            del_sql = ''' delete from tb_advanced_task_state where user = '%s' '''%user
            self.cursor.execute(del_sql)
            return all_ids
        except:
            self.conn.rollback()
        finally:
            return all_ids

    def checkResult(self,task_id):
        self.cursor.execute('show tables')
        tables = self.cursor.fetchall()
        res_table = 'bbd_task%s'%task_id
        if res_table in [row[0] for row in list(tables)]:
            return 1
        else:
            return 2


    def loadResultData(self,task_id,label):
        ## 加载数据集到正式表,并删除临时表,解决重试任务的数据重复问题
        try:
            exists_sql = ''' select count(1) num from tb_advanced_filter_result where task_id = '%s' '''%task_id
            self.cursor.execute(exists_sql)
            exists_num = self.cursor.fetchone()[0]
            if exists_num > 0:
                delete_sql = ''' delete from tb_advanced_filter_result where task_id = '%s' '''%task_id
                self.cursor.execute(delete_sql)
            sql = ''' insert into tb_advanced_filter_result (node_num,task_id,label)
                    select node_num, task_id, '%s' label from bbd_task%s  '''%(label,task_id)
            self.cursor.execute(sql)
            self.cursor.execute(''' drop table bbd_task%s '''%task_id)
        except:
            self.conn.rollback()

    def getUploadID(self,task_id):
        try:
            sql = ''' select upload_id from tb_task_link_info where task_id = '%s' '''%task_id
            self.cursor.execute(sql)
            return self.cursor.fetchone()[0]
        except:
            self.conn.rollback()
            return 1

    def isRetryTask(self,task_id):
        '''是否是重试任务'''
        try:
            sql = ''' select count(1) from tb_advanced_task_state where task_id = '%s' '''%task_id
            self.cursor.execute(sql)
            return self.cursor.fetchone()[0] > 0
        except:
            self.conn.rollback()
            return False
    def getRealApplicationId(self,task_id):
        '''得到任务的spark真实任务id'''
        try:
            sql = ''' select applicationId from tb_advanced_task_state where task_id = '%s' ''' % task_id
            self.cursor.execute(sql)
            id = self.cursor.fetchone()[0]
            return id if id else ''
        except:
            return ''
    def backfillApplicationId(self,applicationId,task_id):
        try:
            sql = ''' update tb_advanced_task_state set applicationId='%s' where task_id = '%s' ''' %(applicationId,task_id)
            self.cursor.execute(sql)
        except:
            self.conn.rollback()

if __name__ == '__main__':
    prop = propertiesTool('jdbc.properties').getProperties()
    mysqlService = jdbcService(prop)
    kwargs = {
        'task_id':'application_15899695',
        'target_type':'person',
        'search_range':'all',
        'user':'test',
        'params':''' {"a":"b"} ''',
    }
    print mysqlService.checkResult('aa')



