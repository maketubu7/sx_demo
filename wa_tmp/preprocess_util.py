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
import numpy as np
import time, copy, re, math
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')


save_path = 'D:\\Users\\User\\Desktop\\case\\case_name\\'

def handle_files(domain):
    '''提取递归目录下的所有的目录文件'''
    all_files = []
    for root, dir, files in os.walk(domain):
        for file in files:
            all_files.append(os.path.join(root,file))
    return all_files

def add_save_path(file,suffix='.xlsx'):
    return save_path + file + suffix

def save_multi_excel(ac,df1,df2,sheet_name1,sheet_name2):
    '''写入两个df到excel的两个sheet页'''
    filename = save_path + ac+'.xlsx'
    writer = pd.ExcelWriter(filename)
    df1.to_excel(writer,sheet_name=sheet_name1,index=False)
    df2.to_excel(writer,sheet_name=sheet_name2,index=False)
    writer.save()

def save_excel(df,path):
    '''写入两个df到excel的两个sheet页'''
    writer = pd.ExcelWriter(path)
    df.to_excel(writer,index=False)
    writer.save()


def read_excel(path,usecols=None,names=None):
    df = pd.read_excel(path,usecols=usecols,names=names,dtype='str')
    df = df.astype(dtype='str')
    return df


def write_csv(df,path):
    df = df.astype(dtype='str')
    df.to_csv(path,index=False,encoding='utf8')

def extract_num(data):
    '''去除中文备注，保留节点信息 全数字 比如qq'''
    matchs = re.findall('[\d]{6,20}', data)
    if matchs:
        return '|'.join(matchs)
    else:
        return ''

def extract_id_num(data):
    '''去除中文备注，保留节点信息, id为下划线字母数字组合 比如微信帐号'''
    matchs = re.findall('[0-9a-zA-Z]+_*[0-9a-zA-Z]+', data)
    if matchs:
        return '|'.join(matchs)
    else:
        return ''

def format_data(data):
    '''格式化标准数据， 处理单一列数据'''
    try:
        return data.replace('\n','').replace(' ','').replace('“','').replace('”','') \
            .replace("'",'').replace('，','').replace('\r','').replace(',','').replace('、','')
    except:
        return ''

def format_data_contain_sep(data,method='id_num'):
    '''格式化数据但保存分隔符 "|",处理一对多数据  '''
    if data.strip():
        if method == 'num':
            return extract_num(data)
        elif method =="id_num":
            return extract_id_num(data)
    return ''

def format_col_from_df(df,col,method='special'):
    '''格式化dataframe 里的某个字段,处理一对多的情况'''
    df[col] = df[col].astype('str')
    if method == 'special':
        df[col] = df[col].apply(lambda x:format_data_contain_sep(x,method='num'))
    else:
        df[col] = df[col].apply(lambda x:format_data(x))
    res = df.loc[df[col] != ""]
    return res


def format_df(df):
    df.replace(' ','',inplace=True)
    df.replace('\n','',inplace=True)
    df.replace('"','',inplace=True)
    df.replace(',','',inplace=True)
    df.replace("'",'',inplace=True)
    df.replace('\r', '', inplace=True)
    return df

def split_row_form_df(df,*cols):
    '''
    对多对多的数据进行处理，处理为一对一,并整体去重
    :param df: dataframe
    :param cols: 需要处理的列名
    :return: dataframe
    '''
    for col in cols:
        format_col_from_df(df,col)
    for col in cols:
        df = df.loc[df[col] != ""]
        df = df.drop(col,axis=1).join(df[col].str.split('|',expand=True).stack().reset_index(level=1,drop=True).rename(col))
    df.drop_duplicates(inplace=True)
    for col in cols:
        df = df.loc[df[col] != 'nan']
    return df

def nliv_df_stand(df,col,bykey=None):
    assert bykey is not None, " bykey is must array of key ['a','b'] "
    dfs = []
    keyed = len(bykey)
    for key,sub_df in df.groupby(bykey):
        key_tmp = []
        if keyed == 1:
            key_tmp.append(key)
        if keyed > 1:
            for i in range(keyed):
                key_tmp.append(list(key)[i])
        if list(sub_df[col])[0]:
            tmps = (list(sub_df[col])[0]).split('|')
            for acc in tmps:
                acc = format_data(acc)
                dfs.append(key_tmp+[acc])
        else:
            continue
    df = pd.DataFrame(dfs,columns=bykey+[col])
    return df

def get_col_count(df,by=None,cols=None):
    '''
    :param df:
    :param by:
    :param cols:
    :return:
    '''
    assert by is not None, 'by must be subset'
    assert cols is not None, 'col must be subset'
    dfs = []
    if isinstance(by,list) and isinstance(cols,list):
        columns = deepcopy(by)
        columns = columns + [col+'count' for col in cols]
        for k,v in df.groupby(by):
            tmp = [k]
            if len(by) > 1:
                tmp = list(k)
            for col in cols:
                tmp.append(len(v[col]))
            dfs.append(tmp)
        return pd.DataFrame(dfs,columns=columns)

def get_col_detail(df,by=None,col=None,dup=False):
    assert by is not None, 'by must be subset'
    assert isinstance(col,str), 'col must be subset'
    dfs = []
    if isinstance(by,list) and isinstance(col,str):
        columns = deepcopy(by)
        columns.append(col)
        subset = by
        if len(by) == 1:
            subset = by[0]
        for key, sub_df in df.groupby(subset):
            banks = list(sub_df[col])
            if banks:
                accs = ','.join(banks).split(',')
                for acc in accs:
                    tmp = [key]
                    if len(by) > 1:
                        tmp = list(key)
                    acc = format_data(acc)
                    tmp.append(acc)
                    dfs.append(tmp)
            else:
                continue
        res = pd.DataFrame(dfs, columns=columns)
        res.drop_duplicates(inplace=dup)
        return res

if __name__ == '__main__':
    ## runfile('D:\shangxi_online\sx_demo\wa_tmp\preprocess_util.py',wdir='D:\shangxi_online\sx_demo\wa_tmp')
    # df = read_excel(add_save_path('case_tmp'),usecols=[2,3,5],names=['zjhm','zjhm1','wechat'])
    df = read_excel(add_save_path('case_tmp'),usecols=[2,5],names=['zjhm','wechat'])
    res = split_row_form_df(df,'zjhm','wechat')
    write_csv(res,add_save_path('edge_person_link_wechat',suffix='.csv'))
    save_excel(res,add_save_path('edge_person_link_wechat',suffix='.xlsx'))












