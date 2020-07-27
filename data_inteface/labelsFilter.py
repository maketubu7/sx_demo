# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : LabelsFilter.py
# @Software: PyCharm
# @content : 前置筛选接口

from flask import Flask,request,jsonify
from wsgiref.simple_server import make_server
import time,json,logging,sys
from logging.handlers import RotatingFileHandler
from commonlib import *
from mysqlService import jdbcService,propertiesTool
from concurrent.futures import ThreadPoolExecutor

reload(sys)
sys.setdefaultencoding('utf-8')
executor1 = ThreadPoolExecutor(4)
executor2 = ThreadPoolExecutor(4)
app = Flask(__name__)
log_file = 'bbd_app.log'

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
# logger = logging.getLogger(__name__)

@app.route('/advancedFilter',methods=['post','get'])
def labelFilter():
    ''' 高级筛选接口 '''
    args = json.loads(request.get_data().decode('utf-8'))
    prop = propertiesTool('jdbc.properties').getProperties()
    mysqlService = jdbcService(prop)
    if args.get('task_id'):
        args['task_id'] = args.get('task_id')
    else:
        args['task_id'] = str(mysqlService.getMxTaskid())
    task_id = args.get('task_id')
    resp = {'code': 200, 'msg': 'success', 'payload': {'task_id': task_id}}
    if str(args.get('is_all')) == '0':
        ## 若搜索范围为导入 则回填任务id
        upload_id = args.get('upload_id')
        mysqlService.backfillTaskID(task_id,upload_id)
    command = build_command(**args)
    type = args.get('target')
    executor1.submit(exec_task,command,mysqlService,task_id,type)
    ## 回填任务参数信息
    params = json.dumps(args, ensure_ascii=False)
    args['params'] = params
    task_info = build_task_params(**args)
    print('canshu: ')
    print(task_info)
    mysqlService.updateTaskRunning(**task_info)  ## 更新任务状态及相关参数
    return jsonify(resp)

def get_appid(task_id,mysqlService):
    '''异步线程回填spark任务id'''
    time.sleep(40)
    applicationId = get_real_applicationId(task_id)
    if applicationId:
        app.logger.info('回填spark任务id成功:{%s:%s}'%(task_id,applicationId))
        mysqlService.backfillApplicationId(applicationId,task_id)
    else:
        app.logger.info('回填spark任务id失败:{%s:%s}'%(task_id,applicationId))

def exec_task(command,mysqlService,task_id,type):
    '''异步执行线程,提交spark任务'''
    app.logger.info(u'task_id:%s 开始执行'%task_id)
    executor1.submit(get_appid,task_id,mysqlService)
    retcode = exec_command(command)
    if retcode == 0:
        task_state = mysqlService.checkResult(task_id)
        if task_state == 1:
            mysqlService.loadResultData(task_id, type)
        mysqlService.updateTaskState(task_id, task_state)  ## 更新任务结果状态
        app.logger.info(u'task_id:%s 任务执行成功' % task_id)
    else:
        mysqlService.updateTaskState(task_id, -1)
        app.logger.info(u'task_id:%s 任务执行失败请查看具体任务日志' % task_id)
    mysqlService.stop()

@app.route('/killTask',methods=['post','get'])
def killTask():
    '''删除任务接口'''
    args = json.loads(request.get_data().decode('utf-8'))
    task_id = args.get('task_id')
    resp = {'code': 200, 'msg': 'success', 'payload': {'task_id': task_id}}
    prop = propertiesTool('jdbc.properties').getProperties()
    mysqlService = jdbcService(prop)
    ## 删除任务明细
    applicationId = mysqlService.getRealApplicationId(task_id)
    app.logger.info(u'任务:%s 执行删除操作' % task_id)
    state = mysqlService.getOrDelTaskState(task_id)
    if not state:
        ## 任务为运行状态时，杀掉该任务
        app.logger.info('appid:%s'%applicationId)
        command = build_kill_command(applicationId)
        executor2.submit(exec_command,command)
    app.logger.info(u'任务:%s 删除所有信息'%task_id)
    mysqlService.stop()
    return jsonify(resp)

@app.route('/killAllTask',methods=['post','get'])
def killAllTask():
    '''任务列表 全部删除'''
    args = json.loads(request.get_data().decode('utf-8'))
    user = args.get('user')
    resp = {'code': 200, 'msg': 'success', 'payload': {}}
    prop = propertiesTool('jdbc.properties').getProperties()
    mysqlService = jdbcService(prop)
    ## 删除任务明细
    id_states = mysqlService.deleteAllTask(user)
    all_ids = []
    if id_states:
        all_ids = [row[0] for row in id_states]
        if filter(lambda row:row[1]==0,id_states):
            task_id = filter(lambda row:row[1]==0,mysqlService.deleteAllTask('user'))[0][0]
            applicationId = mysqlService.getRealApplicationId(task_id)
            exec_command(build_kill_command(applicationId))
    resp['payload']['task_id'] = all_ids
    app.logger.info('删除所有任务,task_id:[%s]'%','.join(all_ids))
    mysqlService.stop()
    return jsonify(resp)

if __name__ == '__main__':
    app.config['JSON_AS_ASCII'] = False

    handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024,backupCount=5)
    fmt = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - func: [%(funcName)s] - %(message)s'
    app.run(host='24.2.21.6',port=8099,debug=True)
    # app.run(host='24.2.21.6',port=8099)
    # formatter = logging.Formatter(fmt)
    # handler.setFormatter(formatter)
    # app.logger.addHandler(handler)
    # app.logger.setLevel(logging.INFO)
    #
    # server = make_server(host='24.2.21.6',port=8099,app=app)
    # server.serve_forever()
    # app.run()














