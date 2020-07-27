# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : sxwa_msg.py
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


users = ['2295547261',
 '95582762',
 '3001923717',
 '1403197859',
 '1852941086',
 '1444003186',
 '2281541347',
 '1135900304',
 '717469373',
 '413876241',
 '490240839',
 '3007815969',
 '1403061109',
 '2393562017',
 '226890781',
 '996895510',
 '755871161',
 '2618950563',
 '2392398998',
 '1852134618',
 '291615432',
 '189555029',
 '3041787593',
 '578512567',
 '1402890406',
 '815474183',
 '1048989777',
 '2057835857',
 '285675412',
 '446934214',
 '3042964674',
 '3035176278',
 '317825899',
 '3154449319',
 '997788368',
 '277282222',
 '1127928888',
 '357123200',
 '1853890125',
 '505220712',
 '800587',
 '2781467311',
 '1846116831',
 '474987237',
 '169075542',
 '3007570226',
 '599911697',
 '2474960505',
 '1115573988',
 '1115573178',
 '511666735',
 '913166583',
 '1459046533',
 '255851111',
 '2549940442',
 '1405850502',
 '1770596062',
 '1474447641']

# save_path = 'D:\qq\wechat_data\\'
save_path = 'D:\zp\\bank_res\\'

def handle_files(domain):
    all_files = []
    for root, dir, files in os.walk(domain):
        for file in files:
            all_files.append(os.path.join(root,file))
    return all_files

def save_excel(ac,df1,df2,sheet_name1,sheet_name2):
    filename = save_path + ac+'.xlsx'
    writer = pd.ExcelWriter(filename)
    df1.to_excel(writer,sheet_name=sheet_name1,index=False)
    df2.to_excel(writer,sheet_name=sheet_name2,index=False)
    writer.save()

def handle_msg(df,ac):
    df1 = df[df['ac']==ac]
    df2 = df1[df1['bc']==ac]
    res = pd.concat([df1,df2],axis=0)
    res.columns = ['发送方','接收方','内容','发送时间']
    return res

def get_ops_num():
    with open('D:\qq\wechat_data\ops_num.txt','r') as f:
        text = f.read()
        ops = json.loads(text,encoding='utf8')
    ops_info = {}

    for data in ops:
        ac = list(data.keys())[0]
        bcs = data[ac]
        ops_info[ac] = bcs

    with open('D:\qq\wechat_data\qq_msg_bk.txt',mode='r') as f:
        text = f.read().decode('utf8')
        message = json.loads(text, encoding='utf8')

    mess = pd.DataFrame(message,dtype='str')
    for ac in users:
        values = [[_] for _ in ops_info[ac]]
        df1 = pd.DataFrame(values, columns=[u'通联帐号'])
        df2 = handle_msg(mess,ac)
        save_excel(ac,df1,df2,sheet_name1='通联帐号',sheet_name2='私聊信息')

def get_all_wechat(data,type=''):
    str = ','.join(filter_list(list(data)))
    matchs = re.findall('[\d]{6,20}', str)
    if type == 'wx':
        matchs = re.findall('[\w,-]{5,20}', str)
    return '|'.join(set(matchs))

def filter_list(data):
    res = filter(lambda x:not isinstance(x,float),data)
    # res = [str(x) for x in tag]
    return res if res else []

def format_data(data):
    try:
        return data.replace('\n','').replace(' ','').replace('“','').replace('”','') \
            .replace("'",'').replace(',','')
    except:
        return ''


def extract_link_wechat(path,sheet_name): #'D:\zp\zp_case.xlsx'
    df = pd.read_excel(path,sheet_name=sheet_name,header=0,dtype='str')
    df = df.fillna({'ajbh':'A'})
    dfs = []
    columns=['ajbh','ajmc','wechatid','qq','alipay','bank_num']
    for key, sub_df in df.groupby(['ajbh','ajmc']):
        ajbh = format_data(key[0])
        ajmc = format_data(key[1])
        wechat = get_all_wechat(sub_df['wechatid'],type='wx')
        qq = get_all_wechat(sub_df['qq'])
        alipay = get_all_wechat(sub_df['alipay'])
        bank_num = get_all_wechat(sub_df['bank_num'])
        dfs.append([ajbh,ajmc,wechat,qq,alipay,bank_num])
    res = pd.DataFrame(dfs,columns=columns)
    nliv_df(res,col='wechatid')
    nliv_df(res,col='qq')
    nliv_df(res,col='alipay')
    nliv_df(res,col='bank_num')
    # res.to_csv(save_path+'case_account.csv',encoding='utf8',index=None,quotechar="'")

def nliv_df(df,col):
    '''列转行'''
    dfs = []
    for key,sub_df in df.groupby(['ajbh','ajmc']):
        ajbh = key[0]
        ajmc = key[1]
        if list(sub_df[col])[0]:
            tmps = (list(sub_df[col])[0]).split('|')
            for acc in tmps:
                acc = format_data(acc)
                dfs.append([ajbh,ajmc,acc])
        else:
            continue

    df = pd.DataFrame(dfs,columns=['ajbh','ajmc',col])
    df.to_csv(save_path+'case_link_%s.csv'%col,encoding='utf8',index=None,quotechar="'")

def read_vertex_case(path):
    df = pd.read_excel(path,sheet_name=3,header=0,dtypr='str')
    df = df.fillna({'ajbh':'A','link_app':" "})
    df['ajbh'].apply(lambda x:x.replace("'",""))
    df.replace(' ','',inplace=True)
    df.replace('\n','',inplace=True)
    df.replace('"','',inplace=True)
    df.replace(',','',inplace=True)
    df.replace("'",'',inplace=True)
    df.replace('\r', '', inplace=True)
    df.to_csv(save_path+'vertex_case.csv',encoding='utf8',index=None,quotechar="'")
# 人员标签	群名片	群创建者	群管理员	删除状态	加群时间	退群时间	来自节点	数据源	最后登录IP	IP区域	最后登录时间

def deal_qq_group_number(file,type='qq',**kwargs):
    qq_cols = ['qq','label','groupcard','is_creator','is_master','del_status','join_time','quit_time',
            'lsat_ip','ip_area','last_time']
    qq_use_cols = [1,4,5,6,7,8,9,10,13,14,15]

    talk_cols = ['qq','send_time','content']
    talk_use_cols = [0,1,4]

    if type == 'qq':
        df = pd.read_excel(file,usecols=qq_use_cols,header=1,names=qq_cols,dtype='str')
        for k,v in kwargs.items():
            df[k]=v
        add_cols= list(kwargs.keys())
        return df[add_cols+qq_cols]
        # return df
    if type == 'qq_group_talk':
        df = pd.read_excel(file, usecols=talk_use_cols, header=1, names=talk_cols,dtype='str')
        for k,v in kwargs.items():
            df[k]=v
        add_cols= list(kwargs.keys())
        return df[add_cols+talk_cols]

def write_csv(df,filename,root='D:\zp\\result\\'):
    save_path = root + filename + '.csv'
    df = df.astype(dtype='str')
    df.to_csv(save_path,index=False,encoding='utf8')

def read_stand_csv(file,root='D:\zp\\result\\'):
    df = pd.read_csv(root+file+'.csv',dtype='str')
    return df

def group_from_file(file):
    matchs = re.findall('[\d]{6,20}', file)
    if matchs:
        return matchs[0]
    else:
        return ''

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

def deal_qq_group_talk():
    all_files =handle_files(u'D:\zp\\bank_relation')
    qq_group_files = filter(lambda x: '成员' in x,all_files)
    qq_group_talk_files = filter(lambda x: '成员' not in x,all_files)
    qq_info = []
    talk_info = []
    res_file = []
    tmp_file = []
    groups = []
    # print(qq_group_files)
    # print(qq_group_talk_files)
    for file in qq_group_files:
        group = group_from_file(file)
        df = deal_qq_group_number(file,type='qq',group=group)
        qq_info.append(df)

    # for file in qq_group_talk_files:
    #     tmp_file.append(file)
    #     if file.endswith(u'xls') or file.endswith(u'xlsx'):
    #         res_file.append(file)

    for file in qq_group_talk_files:
        tmp_file.append(file)
        if file.endswith(u'xls') or file.endswith(u'xlsx'):
            group = group_from_file(file)
            df = deal_qq_group_number(file,type='qq_group_talk', group=group)
            talk_info.append(df)
        else:
            continue

    for file in all_files:
        group = group_from_file(file)
        print(group)

    print(len(qq_group_talk_files))
    print(len(res_file))
    print (len(talk_info))

    # qq_res = pd.concat(qq_info,axis=0).fillna('').drop_duplicates(['group','qq'])
    # qq_talk_res = pd.concat(talk_info,axis=0).fillna('')
    # qq_talk_res = qq_talk_res[qq_talk_res.qq]
    # write_csv(qq_res,'qq_info_res')
    # write_csv(qq_talk_res,'qq_talk_res')

def deal_content_banknum():

    def find_banknum(content):
        pass_keys = ['.jpg','.png']
        # content = str(content).replace(' ', '')
        content = str(content)
        format_content = str(content).replace(' ', '')
        for key in pass_keys:
            if key in content.lower():
                return ''
        matchs = re.findall('[\d]{16,20}', content)
        if matchs:
            return ','.join(matchs)
        else:
            res = re.findall('[\d]{16,20}', format_content)
            if res:
                return ','.join(res)
            else:
                return ''

    df = read_stand_csv('qq_talk_res')
    df['bank'] = df['content'].apply(find_banknum)
    write_csv(df,'talk_banknum_tmp')
    df = df[df.bank != '']

    res = get_col_detail(df,by=['group','qq','send_time'],col='bank')
    write_csv(res,'edge_group_bank_detail')

    res = get_col_detail(df,by=['group'],col='bank')
    write_csv(res,'edge_group_link_banknum_tmp')
    res['tag'] = 1

    group_bank = get_col_count(res,by=['group','bank'],cols=['tag'])
    write_csv(group_bank,'edge_group_link_banknum')
    res1 = get_col_detail(df,by=['qq'],col='bank')
    res1['tag'] = 1
    qq_bank = get_col_count(res1,by=['qq','bank'],cols=['tag'])
    write_csv(qq_bank, 'edge_qq_link_banknum')

if __name__ == '__main__':

    # deal_qq_group_talk()
    df = deal_content_banknum()










