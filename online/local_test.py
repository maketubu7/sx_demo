#encoding=utf-8
import sys
from datetime import datetime
from math import ceil,floor
import numpy as np
import pandas as pd
import argparse

reload(sys)
sys.setdefaultencoding('utf-8')


def udf_format_zjhm_string(data):
    '''格式化证件号码，如身份证，驾驶证等其他证件号码'''
    if not data or not data.strip():
        return ''
    data = data.replace('\r','').replace('\n','').replace('\t', '').replace(' ','').replace('　','').upper().strip()
    return data

def udf_format_lineID_str(*args):
    try:
        if args:
            tmp = ''
            for arg in args:
                tmp += arg
            return tmp
        return ''
    except:
        return ''

def udf_timestamp2cp_int(timestamp,format="%Y%m%d00"):
    stamp = int(timestamp)
    d = datetime.fromtimestamp(stamp)
    return int(d.strftime(format))

def get_history_data(import_times):
    import_cp = sorted(set([str(udf_timestamp2cp_int(import_time,format='%Y%m%d00')) for import_time in import_times]))
    cps = {}
    for cp in import_cp:
        cps[cp] = [_ for _ in import_times if str(udf_timestamp2cp_int(_,format='%Y%m%d00')) == cp]

    print cps


def zws(arr):
    '''
    4分位数求解 对应np.percentile(interpolation='nearest')
    :param arr:
    :return:
    '''
    assert isinstance(arr,list), 'The type must be list'
    arr = sorted(arr)
    length = len(arr)
    zws = []
    if length % 2 == 1:
        indices = [int(length* _) for _ in [0.25,0.5,0.75]]
        zws = [arr[_] for _ in indices]
    else:
        q1 = float(length+1) / 4 - 1
        q2 = length / 2 - 0.5
        q3 = float(3*(length+1)) / 4 -1
        print q1,q2,q3
        zws.append(float(arr[int(floor(q1))]*0.25 + arr[int(ceil(q1))]*0.75))
        zws.append(float(arr[int(floor(q2))] + arr[int(ceil(q2))]) / 2)
        zws.append(float(arr[int(floor(q3))]*0.75 + arr[int(ceil(q3))]*0.25))
    return zws

def get_multiphone(sfz,data):
    tmps = []
    tmp = data.split(',')
    if len(tmp) > 1:
        for phone in tmp:
            if phone:
                tmps.append([sfz,phone])
    return tmps

def deal_smz():
    path = 'D:\Users\User\Desktop\\bbd\person_phone.csv'
    df = pd.read_csv(path, sep='\t')
    df1 = df[~df.phone.str.contains(',')]
    df2 = df[df.phone.str.contains(',')]
    res = []
    for sfz, sub_df in df2.groupby('sfz'):
        phones = list(sub_df['phone'])[0]
        tmp = get_multiphone(sfz,phones)
        res.append(tmp)
    dfs = []
    for kvs in res:
        for kv in kvs:
            dfs.append(kv)
    df3 = pd.DataFrame(dfs,columns=['sfz','phone'])
    res_df = pd.concat([df1,df3],axis=0)
    res_df['sfz'] = res_df['sfz'].astype(str)
    res_df.to_csv('D:\Users\User\Desktop\\bbd\person_phone_res.csv',index=None)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='args manual')
    parser.add_argument('--labels',type=str,default='')
    parser.add_argument('--hjdz',type=str,default='')
    parser.add_argument('--xm',type=str,default='')
    parser.add_argument('--xm1',type=str,default='aaa')
    args = parser.parse_args()
    print args.xm1