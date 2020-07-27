# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : preprocess_util.py
# @Software: PyCharm
# @content : 合并数据

from copy import deepcopy
import sys, os, json
import pandas as pd
import time, copy, re, math
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')

# save_path = 'D:\qq\wechat_data\\'
root_path = u'D:\\big_case_jsh\\network\\'
save_path = u'D:\\big_case_jsh\\result\\'
call_root_path = u'D:\\big_case_jsh\\call_msg\\'

qq_names = ['qq','nickname','sex','city','mobile','email','realname','csrq',
           'college','area','province','regis_time','regis_ip','study','sign_name']
#QQ号(APPID)	昵称(NICKNAME)	性别(SEX)	出生日期(BIRTHDAY)	注册IP(IP)	居住地/城市(LIVEPLACE)
# 个性签名(SIGN_NAME)	手机号码(MOBILE)	邮箱地址(EMAIL)	注册时间(REGIS_TIME)	国家(AREA)	所在省份(PROVINCE)	所在地市(CITY)	学校(COLLEGE)	学历(STUDY)
# 2920918274	₍ᐢ..ᐢ₎	女性	2001-10-12	183.185.129.224(中国山西太原万柏林区|联通)	合肥市	“梦想不多 兜里有糖 肚里有墨 手里有活 卡里有钱 未来有你”
# ['qq','nickname','sex','csrq','regis_ip','sign_name','mobel','email','regis_time','area','province','city','college','study']

# 微信号(APPID)	微信ID(USERID)	QQ号(BINDQQ)	手机号码(MOBILE)	昵称(NICKNAME)
# 性别(SEX)	所在省份(PROVINCE)	所在地市(CITY)	境外帐号标识(OVERSEA)	年龄 (AGE)	邮箱(EMAIL)
# lrjooo000	3498701448	2920918274	13015442862	Mi Manchi	女性	山西	太原	境内

def handle_files(domain):
    all_files = []
    for root, dir, files in os.walk(domain):
        for file in files:
            all_files.append(os.path.join(root,file))
    return all_files

def write_csv(df,filename,root=save_path):
    save_path = root + filename + '.csv'
    df = df.astype(dtype='str')
    df.to_csv(save_path,index=False,encoding='utf8')

def get_qqh(path):
    df = pd.read_excel(path, sheet_name=1, usecols=[0],names=['qq'])
    df['qq'] = df['qq'].astype(str)
    return df['qq'][0]

def deal_qq_friend(file,type='qq'):
    schema_we_f = ['wechat1', 'wechat2','wechat2_id']
    schema_qq_f = ['qq1','qq2','last_time','first_time']
    schema_qq = ['qq','nickname','sex','csrq','regis_ip','sign_name','mobile','email',
                 'regis_time','area','province','city','college','study']
    schema_we = ['wechat','wechatid','qq','mobile','nickname','sex','province','city','oversea','age','email']
    qq_cols = ['qq','nickname','sex','city','mobile','email','csrq',
           'college','area','province','regis_time','regis_ip','study','sign_name']
    self_code = get_qqh(file)
    if type == 'qq_f':
        df = pd.read_excel(file,sheet_name=0,usecols=[0,2,3],names=['qq2','last_time','first_time'])
        df.fillna('',inplace=True)
        df['qq1'] = self_code
        df = df.astype(dtype='str')
        return df[schema_qq_f]

    elif type == 'we_f':
        df = pd.read_excel(file,sheet_name=0,usecols=[0,1],names=['wechat2','wechat2_id'])
        df['wechat1'] = self_code
        df = df.astype(dtype='str')
        df.fillna('', inplace=True)
        return df[schema_we_f]

    elif type == 'qq':
        df = pd.read_excel(file,sheet_name=1,usecols=[0,1,2,3,4,6,7,8,9,10,11,12,13,14],names=schema_qq)
        df = df.astype(dtype='str')
        df.fillna('', inplace=True)
        return df[schema_qq]

    elif type == 'we':
        df = pd.read_excel(file,sheet_name=1,usecols=[0,1,2,3,4,5,6,7,8,9,10],names=schema_we)
        df = df.astype(dtype='str')
        df.fillna('',inplace=True)
        return df[schema_we]

def deal_call(file):
    call_scheam = ['call_duration','start_time','call_type','start_phone','end_phone']
    df = pd.read_csv(file,usecols=[4,5,8,9,10],names=call_scheam,header=0)
    df = df.astype(dtype='str')
    return df

def call_msg():
    all_files = handle_files(call_root_path)
    df = pd.concat([deal_call(_) for _ in all_files],axis=0)
    write_csv(df,'call_detail')





def deal_network():

    all_files = handle_files(root_path)

    we_file = filter(lambda x: u'微信' in x,all_files)
    qq_file = filter(lambda x: u'QQ' in x,all_files)

    we_f = []
    qq_f = []
    qq = []
    we = []

    for file in we_file:
        we_f.append(deal_qq_friend(file,type='we_f'))
        we.append(deal_qq_friend(file,type='we'))

    for file in qq_file:
        qq_f.append(deal_qq_friend(file,type='qq_f'))
        qq.append(deal_qq_friend(file,type='qq'))

    wefs = pd.concat(we_f,axis=0)
    qqfs = pd.concat(qq_f,axis=0)
    qqs = pd.concat(qq,axis=0)
    wes = pd.concat(we,axis=0)

    write_csv(wefs,'edge_wechat_friend_wechat')
    write_csv(qqfs,'edge_qq_friend_qq')
    write_csv(qqs,'vertex_qq')
    write_csv(wes,'vertex_wechat')


if __name__ == '__main__':

    # deal_qq_group_talk()
    deal_network()
    call_msg()










