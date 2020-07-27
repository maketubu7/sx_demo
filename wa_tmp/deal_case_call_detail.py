# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : imsi_imei.py
# @Software: PyCharm
# @content : 设备码相关信息

import sys, os
import pandas as pd
import time, copy, re, math
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')

QQ_INFO_ROOT = u'D:\qq\qq_info'
QQ_INFO_ROOT2 = u'D:\qq\jsd\\31_jsd'
QQ_INFO_ROOT3 = u'D:\qq\jsd\\43_jsd'
QQ_GROUP_ROOT = u'D:\qq\qq_group'
QQ_SMZ = u'D:\qq\\netcode_smz.xls'

qq_names = ['qq','nickname','sex','city','mobile','email','realname','csrq',
           'college','area','province','regis_time','regis_ip','study','sign_name']
group_names = ['groupid','groupname','count','masterid','groupflag','groupclass',
               'introduction','annment','create_time','last_time']
friend_names = ['start_qq', 'end_qq','start_time','end_time']
qq_attend_names = ['qq','groupid','start_time','end_time']
person_own_qq = ['sfzh','qq','start_time','end_time']

def handle_files(domain):
    all_files = []
    for root, dir, files in os.walk(domain):
        for file in files:
            all_files.append(os.path.join(root,file))
    return all_files

def get_qqh(path):
    df = pd.read_excel(path, sheet_name=0, usecols=[0],names=['qq'])
    df['qq'] = df['qq'].astype(str)
    return df['qq'][0]

def as_str(df,cols):
    for name in cols:
        df[name] = df[name].astype(str)
    return df

def read_fliter(path,sheet_name=0,type='qq'):
    '''
    标准格式读取
    :param path:
    :param sheet_name:
    :param type:
    :return:
    '''
    ## qq 0 friend 1 group 2
    try:
        if type == 'qq':
            df = pd.read_excel(path,sheet_name=sheet_name,names=qq_names,na_values='')
            return as_str(df,qq_names)
        elif type == 'qq_attend':
            df = pd.read_excel(path, sheet_name=sheet_name,usecols=[0],names=['groupid'],na_values='')
            qq = get_qqh(path)
            df['qq'] = qq
            df['start_time'] = '0'
            df['end_time'] = '0'
            return as_str(df,qq_attend_names)[qq_attend_names]
        elif type == 'find_group':
            tmp_names = ['groupid','groupname','count','create_time','groupflag','groupclass','introduction','annment','masterid']
            df = pd.read_excel(path, sheet_name=sheet_name, usecols=[0,1,2,3,4,5,6,7,8], names=tmp_names,na_values='')
            df['last_time'] = '0'
            return as_str(df, group_names)[group_names]
        elif type == 'friend':
            qq = get_qqh(path)
            df = pd.read_excel(path, sheet_name=sheet_name, usecols=[0], names=['end_qq'],na_values='')
            df['start_qq'] = qq
            df['start_time'] = '0'
            df['end_time'] = '0'
            return as_str(df, friend_names)[friend_names]
        elif type == 'group_info':
            df = pd.read_excel(path, sheet_name=sheet_name,
                    names=['groupid','groupname','count','masterid','create_time','last_time'],na_values='')
            df['groupflag'] = u'普通群'
            df['groupclass'] = ''
            df['introduction'] = ''
            df['annment'] = ''
            return as_str(df, group_names)[group_names]
        elif type == 'group_attend':
            df = pd.read_excel(path, sheet_name=sheet_name, usecols=[0,3],names=qq_attend_names[:2],na_values='')
            df.to_csv()
            df['start_time'] = '0'
            df['end_time'] = '0'
            return as_str(df,qq_attend_names)[qq_attend_names]
        elif type == 'find_qq':
            df = pd.read_excel(path, sheet_name=sheet_name, usecols=[0,1], names=qq_names[:2],na_values='')
            for col in qq_names[2:]:
                df[col] = ''
            return as_str(df,qq_names)[qq_names]
    except:
        print(path + 'read failed')


def read_simple_fliter(path,sheet_name=0,type='qq'):
    '''
    标准格式读取
    :param path:
    :param sheet_name:
    :param type:
    :return:
    '''
    ## qq 0 friend 1 group 2
    try:
        if type == 'qq':
            df = pd.read_excel(path,sheet_name=sheet_name,usecols=[0],names=['qq'])
            for col in qq_names[1:]:
                df[col] = ''
            return as_str(df,qq_names)[qq_names]
        elif type == 'qq_attend':
            df = pd.read_excel(path, sheet_name=sheet_name,usecols=[0],names=['groupid'])
            qq = get_qqh(path)
            df['qq'] = qq
            df['start_time'] = '0'
            df['end_time'] = '0'
            return as_str(df,qq_attend_names)[qq_attend_names]
        elif type == 'find_group':
            tmp_names = ['groupid','groupname','count','create_time','groupflag','groupclass','introduction','annment','masterid']
            df = pd.read_excel(path, sheet_name=sheet_name, usecols=[0], names=['groupid'])
            for col in group_names[1:]:
                df[col] = ''
            return as_str(df, group_names)[group_names]
        elif type == 'friend':
            qq = get_qqh(path)
            df = pd.read_excel(path, sheet_name=sheet_name, usecols=[0], names=['end_qq'],na_values='')
            df['start_qq'] = qq
            df['start_time'] = '0'
            df['end_time'] = '0'
            return as_str(df, friend_names)[friend_names]
    except:
        print(path + 'read failed')

def get_all_info():
    all_qq_file = handle_files(QQ_INFO_ROOT3)
    # all_group_info = handle_files(QQ_GROUP_ROOT)
    qq_info = []
    group_info = []
    qq_attend_group = []
    qq_friend = []
    for file in all_qq_file:
        if 'format' in file:
            print('reading ' + file)
            qq = read_fliter(file,sheet_name=0,type='qq')
            friend = read_fliter(file,sheet_name=1,type='friend')
            attend = read_fliter(file,sheet_name=2,type='qq_attend')
            group = read_fliter(file,sheet_name=2,type='find_group')
            qq_info.append(qq)
            qq_friend.append(friend)
            qq_attend_group.append(attend)
            group_info.append(group)
        if 'pass2' in file:
            print('reading ' + file)
            qq = read_simple_fliter(file, sheet_name=0, type='qq')
            friend = read_simple_fliter(file, sheet_name=1, type='friend')
            attend = read_simple_fliter(file, sheet_name=2, type='qq_attend')
            group = read_simple_fliter(file, sheet_name=2, type='find_group')
            qq_info.append(qq)
            qq_friend.append(friend)
            qq_attend_group.append(attend)
            group_info.append(group)

    # for file in all_group_info:
    #     qq = read_fliter(file, sheet_name=1, type='find_qq')
    #     group = read_fliter(file, sheet_name=0, type='group_info')
    #     attend = read_fliter(file, sheet_name=1, type='group_attend')
    #     qq_info.append(qq)
    #     group_info.append(group)
    #     qq_attend_group.append(attend)

    all_qq = pd.concat(qq_info,axis=0).fillna('')
    all_group = pd.concat(group_info,axis=0).fillna('')
    all_attend = pd.concat(qq_attend_group,axis=0).fillna('')
    all_qq_friend = pd.concat(qq_friend,axis=0).fillna('')
    return all_qq,all_group,all_attend,all_qq_friend


def write_csv(df,filename):
    save_path = 'D:\qq\\result\\' + filename
    df = df.astype(dtype='str')
    df.to_csv(save_path,header=False,index=False,encoding='utf8')

def deal_smz():
    path = 'D:\qq\\netcode_smz.xls'
    df = pd.read_excel(path,usecols=[1,2],names=['sfzh','qq'])
    df = df[df['sfzh'].notna()]
    df['start_time'] = '0'
    df['end_time'] = '0'
    return as_str(df,person_own_qq)[person_own_qq]

def get_call_info():
    '''提取通话明细'''
    all_files = handle_files('D:\qq\phone_call')
    all_txt_files = handle_files('D:\qq\gz_call\call_txt')
    cols = ['start_phone','end_phone','start_time','call_duration','call_type','type']
    dfs = []
    for file in all_files:
        print(file)
        df = pd.read_excel(file,usecols=[1,4,5,8,9,10],names=['type','call_duration','start_time','call_type','start_phone','end_phone'])
        df = df.astype(dtype='str')
        dfs.append(df[cols])

    for file in all_txt_files:
        print(file)
        df= pd.read_csv(file,usecols=[1,4,5,8,9,10],names=['type','call_duration','start_time','call_type','start_phone','end_phone'])
        df = df.astype(dtype='str')
        dfs.append(df[cols])
    union_df = pd.concat(dfs,axis=0)
    write_csv(union_df,'vip_call.csv')

def union_csv():
    filename = ['all_attend','all_qq','all_group','all_qq_friend']
    all_files = handle_files('D:\qq\\result')
    for name in filename:
        dfs = []
        print(name)
        for files in all_files:
            if name in files:
                df = pd.read_csv(files,dtype='str',header=None)
                # print(df.head())
                dfs.append(df)
        res = pd.concat(dfs,axis=0)
        print(res.head())
        write_csv(res,name+'_res.csv')


def get_call_detail():
    files = handle_files(u'D:\\Users\\User\\Desktop\\DB\\孤立电话\\孤立号码')
    dfs = []
    for file in files:
        # ,names=['start_phone','end_phone','type','date','duration']
        df = pd.read_excel(file,usecols=[0,4,6,7,9])
        dfs.append(df)
    res = pd.concat(dfs,axis=0).fillna('')
    write_csv(res,'call_detail.csv')



if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    # all_qq, all_group, all_attend, all_qq_friend = get_all_info()
    # write_csv(all_qq,'all_qq_3.csv')
    # write_csv(all_group,'all_group_3.csv')
    # write_csv(all_attend,'all_attend_3.csv')
    # write_csv(all_qq_friend,'all_qq_friend_3.csv')

    # smz = deal_smz()
    # write_csv(smz,'person_own_qq.csv')
    # get_call_info()
    # union_csv()
    get_call_detail()


    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))