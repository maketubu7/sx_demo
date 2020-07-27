# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 15:35
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : call_msg.py
# @Software: PyCharm
# @content : 节点结构信息，关系依赖信息

from collections import OrderedDict

def get_table_index(tablename):
    table_index = {
        'vertex_case': 10000000,
        'vertex_person': 20000000,
        'vertex_phonenumber': 40000000,
        'vertex_qq': 50000000,
        'vertex_wechat': 600000000,
        'vertex_alipay': 70000000,
        'vertex_qq_group': 80000000,
        'vertex_bankcard': 90000000,
    }
    return table_index[tablename]

# 所有表字段配置l
vertex_table_info = OrderedDict()
vertex_table_info['vertex_case'] = ['asjbh','ajmc','link_app','jyaq','city','larq','ajlb','fxasjdd_dzmc']
vertex_table_info['vertex_person'] = ['zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                                      'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz']
vertex_table_info['vertex_phonenumber'] = ['phone', 'xm','province', 'city']
vertex_table_info['vertex_qq'] = ['qq']
vertex_table_info['vertex_wechat'] = ['wechat']
vertex_table_info['vertex_alipay'] = ['alipay']
vertex_table_info['vertex_qq_group'] = ['groupid','groupname']
vertex_table_info['vertex_bankcard'] = ['yhkh','khh','xm','sfzh','wyyzdh','khrq']

#
# 'edge_case_link_alipay':['ajbh','alipay','start_time','end_time'],
#     'edge_case_link_banknum':['ajbh','bank_num','start_time','end_time'],
#     'edge_case_link_qq':['ajbh','qq','start_time','end_time'],
#     'edge_case_link_wechat':['ajbh','wechatid','start_time','end_time'],
#     'edge_qq_link_group':['groupid', 'qq', 'label', 'groupcard', 'is_creator', 'is_master', 'del_status',
#                 'join_time', 'quit_time', 'last_ip', 'ip_area', 'last_time'],
#     'edge_qqgroup_link_banknum':['groupid','banknum','relate_time','start_time','end_time','relate_num'],
#     'vertex_alipay':['alipay'],
#     'vertex_bankcard':['yhkh','khh','xm','sfzh','wyyzdh','khrq'],
#     'vertex_case':['asjbh','ajmc','link_app','city','"" jyaq','0 larq'],
#     'vertex_qq_group':['groupid','groupname'],
#     'vertex_qq':['qq'],
#     'vertex_wechat':['wechat'],

# 边字段配置
edge_table_info = OrderedDict()
edge_table_info['edge_case_link_person'] = ['sfzh','ajbh','start_time','end_time']
edge_table_info['edge_case_link_phone'] = ['phone','ajbh','start_time','end_time']
edge_table_info['edge_case_link_alipay'] = ['alipay','ajbh','start_time','end_time']
edge_table_info['edge_case_link_banknum'] = ['banknum','ajbh','start_time','end_time']
edge_table_info['edge_case_link_wechat'] = ['wechatid','ajbh','start_time','end_time']
edge_table_info['edge_case_link_qq'] = ['qq','ajbh','start_time','end_time']
edge_table_info['edge_qq_link_group'] = ['qq', 'groupid','start_time', 'end_time','ip_area']
edge_table_info['edge_qqgroup_link_banknum'] = ['banknum','groupid','relate_time','start_time','end_time','relate_num']
edge_table_info['edge_person_link_phone'] = ['phone','sfzh','start_time','end_time']
edge_table_info['edge_qq_friend_qq'] = ['qq1','qq2','start_time','end_time']


#edge_table_info['edge_groupcall'] = ['start_phone', 'end_phone', 'start_time', 'end_time', 'call_total_duration','call_total_times']
#edge_table_info['edge_person_smz_phone'] = ['start_person', 'end_phone', 'start_time', 'end_time']
#edge_table_info['edge_same_hotel_house'] = ['sfzh1', 'sfzh2','start_time','end_time', 'num']
#edge_table_info['edge_person_with_airline_travel'] = ['sfzh1', 'sfzh2','start_time','end_time', 'num']
#edge_table_info['edge_person_with_trainline_travel'] = ['sfzh1', 'sfzh2','start_time','end_time', 'num']
#edge_table_info['edge_person_legal_com'] = ['start_person', 'end_company', 'start_time', 'end_time']
# 所有边关系配置
# key为边表，值依次为，start_id 的依赖表，end_id 的依赖表，start_id 的依赖表，end_id 的依赖表字段

edge_info = {
    'edge_case_link_person': ['vertex_person_jg', 'zjhm','vertex_case_jg', 'asjbh'],
    'edge_case_link_phone': ['vertex_phonenumber_jg', 'phone','vertex_case_jg', 'asjbh'],
    'edge_case_link_alipay': ['vertex_alipay_jg', 'alipay','vertex_case_jg', 'asjbh'],
    'edge_case_link_banknum': ['vertex_bankcard_jg', 'yhkh','vertex_case_jg', 'asjbh'],
    'edge_case_link_wechat': ['vertex_wechat_jg', 'wechat','vertex_case_jg', 'asjbh'],
    'edge_case_link_qq': ['vertex_qq_jg', 'qq','vertex_case_jg', 'asjbh'],
    'edge_qq_link_group': ['vertex_qq_jg', 'qq','vertex_qq_group_jg', 'groupid'],
    'edge_qqgroup_link_banknum': ['vertex_bankcard_jg', 'yhkh','vertex_qq_group_jg', 'groupid'],
    'edge_person_link_phone': ['vertex_person_jg', 'zjhm','vertex_phonenumber_jg', 'phone'],
    'edge_qq_friend_qq': ['vertex_qq_jg', 'qq','vertex_qq_jg', 'qq'],
     }