# -*- coding: utf-8 -*-
# @Time    : 2020/3/6 15:35
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : jg_info.py
# @Software: PyCharm
# @content : 节点结构信息，关系依赖信息 为jg.py的输入依赖信息

from collections import OrderedDict

## 所有节点的索引起始点，
# 目前我们的janusgraph图库 只支持数字型索引，所以按此方式进行jid建立
def get_table_index(tablename):
    table_index = {
        'vertex_person': 0,	# 人 40亿空间
        'vertex_imsi': 4000000000,	# IMSI 20亿
        'vertex_imei': 5000000000,	# IMEI 10亿
        'vertex_autolpn': 6000000000,	# 车牌号码' 10亿
        'vertex_vehicle': 7000000000,	# 机车（车架号）' 10亿
        'vertex_case': 8000000000,	# 案件' 10亿
        'vertex_jail': 9000000000,	# 所' 10亿
        'vertex_hukou': 10000000000,	# 户口'  10亿
        'vertex_internetbar': 11000000000,	# 网吧' 10亿
        'vertex_ip': 12000000000,	# IP地址' 10亿
        'vertex_hotel': 13000000000,	# 宾馆' 10亿
        'vertex_airline': 14000000000,	# 航班班次' 10亿
        'vertex_ssaccount': 15000000000,	# 社保账号' 10亿
        'vertex_trainline': 17000000000,	# 火车班次' 10亿
        'vertex_package': 21000000000,	# 快递(快递单号）' 10亿
        'vertex_certificate': 25000000000,	# 证件' 10亿
        'vertex_school': 27000000000,	# 学校' 10亿
        'vertex_college': 28000000000,	# 学院' 10亿
        'vertex_company': 29000000000,	# 公司' 10亿
        'vertex_vbn': 30000000000,	# 宽带账号' 10亿
        'vertex_alert':31000000000, # 警情10亿
        'vertex_qq':32000000000, # qq10亿
        'vertex_qq_group':33000000000, # qq群10亿
        'vertex_house':34000000000, # 房屋10亿
        'vertex_wechat':35000000000, # 房屋10亿
        'vertex_sina':36000000000, # 房屋10亿
        'vertex_phonenumber': 37000000000,	# 电话 20亿空间 永远在最后，增长最快
    }
    return table_index[tablename]


# 所有节点表的属性字段配置
vertex_table_info = OrderedDict()
vertex_table_info['vertex_person'] = ['zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                                      'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz']
vertex_table_info['vertex_phonenumber'] = ['phone', 'xm','province', 'city']
vertex_table_info['vertex_autolpn'] = ['autolpn','hpzl']
vertex_table_info['vertex_imsi'] = ['imsi']
vertex_table_info['vertex_imei'] = ['imei']
vertex_table_info['vertex_vehicle'] = ['vehicle', 'fdjh', 'cllx', 'csys']
vertex_table_info['vertex_hotel'] = ['lgdm', 'qiyemc', 'yyzz', 'xiangxidizhi']
vertex_table_info['vertex_airline'] = ['airlineid', 'hbh', 'hbrq', 'yjqfsj', 'yjddsj', 'sfd', 'mdd']
vertex_table_info['vertex_trainline'] = ['trianid', 'cc', 'fcrq']
vertex_table_info['vertex_school'] = ['school_id', 'name', 'history_name','school_address']
vertex_table_info['vertex_company'] = ['company', 'dwmc', 'dwdz', 'zzjgdm', 'tyshdm', 'clrq', 'jyzt', 'jyfw',
                                       'dwlx', 'zczb', 'cyrs', 'hylb', 'dwscdz', 'bgxq', 'bgrq']
vertex_table_info['vertex_alert'] = ['jqbh', 'jqxz', 'jqfxdz', 'jyqk','jjsj']
vertex_table_info['vertex_internetbar'] = ['siteid','sitetype','title','address']
vertex_table_info['vertex_case'] = ['asjbh','ajmc','asjfskssj','asjfsjssj','asjly','ajlb','fxasjsj','fxasjdd_dzmc',
                                        'jyaq','ajbs','larq','city','link_app']
vertex_table_info['vertex_package'] = ['packageid','company','orderid','accepttime','inputtime','item_name',
                                       'item_type','senderphone','senderaddress','receiverphone','receiveraddress']
vertex_table_info['vertex_qq_group'] = ['groupid','groupname','count','masterid','groupflag','groupclass','introduction','annment','create_time']
# vertex_table_info['vertex_qq'] = ['qq','nickname','sex','city','mobile','email','realname','csrq','college','area','province','regis_time','regis_ip','study','sign_name']
vertex_table_info['vertex_qq'] = ['qq']
vertex_table_info['vertex_sina'] = ['sina']
vertex_table_info['vertex_wechat'] = ['wechat']
vertex_table_info['vertex_jail'] = ['unitid','unitname','unittype']
vertex_table_info['vertex_house'] = ['house_id','house_addr']
vertex_table_info['vertex_certificate'] = ['certificateid','certificate','certificate_type']
# 边字段配置 所有边的属性信息
# tips 第一个字段为起始节点， 第二个字段为结束节点，比如 sfzh airlineid 人节点->飞机节点
# 与后面的有强逻辑关系， 不可随意更换，
edge_table_info = OrderedDict()
edge_table_info['edge_person_spouse_person'] = ['sfzhnan', 'sfzhnv', 'type_name', 'start_time', 'end_time']
edge_table_info['edge_find_common_greatgrand'] = ['sfzh1', 'sfzh2','type_name', 'start_time','end_time']
edge_table_info['edge_find_common_father'] = ['sfzh1', 'sfzh2','type_name','start_time','end_time']
edge_table_info['edge_find_common_mother'] = ['sfzh1', 'sfzh2','type_name','start_time','end_time']
edge_table_info['edge_find_spouse'] = ['sfzhnan', 'sfzhnv','type_name']
edge_table_info['edge_find_common_grand'] = ['sfzh1', 'sfzh2','type_name','start_time','end_time']
edge_table_info['edge_find_father'] = ['sfzh', 'fqsfzh','type_name','start_time','end_time']
edge_table_info['edge_find_grandparents'] = ['sfzh', 'grandsfzh','type_name', 'start_time','end_time']
edge_table_info['edge_find_greatgrands'] = ['sfzh', 'greatgrandsfzh','type_name', 'start_time','end_time']
edge_table_info['edge_find_mother'] = ['sfzh', 'mqsfzh','type_name','start_time','end_time']
edge_table_info['edge_person_attend_school'] = ['start_person', 'end_school_id','start_time','end_time','xxmc','xymc','bj','zymc','xh','xz']
edge_table_info['edge_person_fatheris_person'] = ['sfzh', 'fqsfzh', 'type_name', 'start_time', 'end_time']
edge_table_info['edge_person_stay_hotel'] = ['sfzh', 'lgdm', 'start_time', 'end_time', 'num']
edge_table_info['edge_person_reserve_trainline'] = ['sfzh', 'trianid', 'cc', 'fcrq', 'cxh','zwh', 'fz', 'dz','start_time','end_time']
edge_table_info['edge_person_guardianis_person'] = ['sfzh', 'jhrsfzh', 'type_name', 'start_time', 'end_time']
edge_table_info['edge_phonenumber_use_imei'] = ['phone', 'imei','start_time','end_time']
edge_table_info['edge_phonenumber_use_imsi'] = ['phone', 'imsi','start_time','end_time']
edge_table_info['edge_person_own_autolpn'] = ['sfzh', 'autolpn']
edge_table_info['edge_person_motheris_person'] = ['sfzh','mqsfzh', 'type_name', 'start_time', 'end_time']
edge_table_info['edge_person_checkin_airline'] = ['sfzh','airlineid','hbh','hbrq','sfd','mdd','start_time','end_time']
edge_table_info['edge_person_arrived_airline'] = ['sfzh','airlineid','hbh','hbrq','sfd','mdd','start_time','end_time']
edge_table_info['edge_person_reserve_airline'] = ['sfzh','airlineid','hbh','hbrq','sfd','mdd','start_time','end_time']
edge_table_info['edge_person_legal_com'] = ['start_person', 'end_company', 'start_time', 'end_time']
edge_table_info['edge_groupcall'] = ['start_phone', 'end_phone', 'start_time', 'end_time', 'call_total_duration','call_total_times']
edge_table_info['edge_groupmsg'] = ['start_phone', 'end_phone', 'start_time', 'end_time', 'message_number']
edge_table_info['edge_phone_call_alert'] = ['phone', 'jqbh', 'bjrsfzh', 'bjrxm', 'jjsj','start_time','end_time']
edge_table_info['edge_case_reportby_person'] = ['sfzh', 'asjbh', 'start_time', 'end_time']
edge_table_info['edge_alert_link_case'] = ['jqbh', 'asjbh', 'link_time','start_time','end_time']
edge_table_info['edge_person_surfing_internetbar'] = ['sfzh', 'siteid', 'start_time','end_time','num']
edge_table_info['edge_phone_send_package'] = ['phone', 'packageid', 'send_time','start_time','end_time']
edge_table_info['edge_phone_receive_package'] = ['phone', 'packageid', 'receive_time','start_time','end_time']
edge_table_info['edge_phone_sendpackage_phone'] = ['start_phone', 'end_phone', 'start_time','end_time','num']
edge_table_info['edge_person_smz_phone'] = ['start_person', 'end_phone', 'start_time', 'end_time']
edge_table_info['edge_person_open_phone'] = ['sfzh', 'phone', 'start_time', 'end_time','is_xiaohu']
edge_table_info['edge_person_link_phone'] = ['sfzh', 'phone', 'start_time', 'end_time','num']
# edge_table_info['edge_qq_link_group'] = ['qq','groupid','quit_time','last_time']
edge_table_info['edge_qq_link_qq'] = ['qq1','qq2','start_time','end_time']
# edge_table_info['edge_person_own_qq'] = ['sfzh', 'qq', 'start_time', 'end_time']
# edge_table_info['find_qq_common_group'] = ['qq1', 'qq2', 'num']
edge_table_info['edge_same_hotel_house'] = ['sfzh1', 'sfzh2','start_time','end_time', 'num']
edge_table_info['edge_person_with_airline_travel'] = ['sfzh1', 'sfzh2','start_time','end_time', 'num']
edge_table_info['edge_person_with_trainline_travel'] = ['sfzh1', 'sfzh2','start_time','end_time', 'num']
edge_table_info['edge_person_with_internetbar_surfing'] = ['sfzh1', 'sfzh2','start_time','end_time', 'num']
edge_table_info['relation_score_res'] = ['sfzh1', 'sfzh2','data','last_tb_score', 'last_zb_score','weight_score']

## 关系库新增
edge_table_info['edge_person_stay_kss'] = ['sfzh', 'jsbh','ajbh','jyaq', 'start_time','end_time']
edge_table_info['edge_person_stay_jds'] = ['sfzh', 'jsbh','ajbh','jyaq', 'start_time','end_time']
edge_table_info['edge_person_kinsfolk_person'] = ['sfzh1', 'sfzh2','type_name','start_time', 'end_time']
edge_table_info['edge_find_common_parents'] = ['sfzh1', 'sfzh2','type_name','start_time', 'end_time']
edge_table_info['edge_person_rent_house'] = ['sfzh', 'house_id','start_time','end_time', 'trade_time','trade_price']
edge_table_info['edge_person_own_house'] = ['sfzh', 'house_id','start_time','end_time', 'trade_time','trade_price']
edge_table_info['edge_person_live_house'] = ['sfzh', 'house_id','start_time','end_time']
edge_table_info['edge_person_link_wechat'] = ['sfzh', 'user_account','start_time','end_time']
edge_table_info['edge_person_link_qq'] = ['sfzh', 'user_account','start_time','end_time']
edge_table_info['edge_person_link_sinabolg'] = ['sfzh', 'user_account','start_time','end_time']
edge_table_info['edge_phone_link_wechat'] = ['phone', 'user_account','start_time','end_time']
edge_table_info['edge_phone_link_qq'] = ['phone', 'user_account','start_time','end_time']
edge_table_info['edge_phone_link_sinabolg'] = ['phone', 'user_account','start_time','end_time']
edge_table_info['edge_person_link_certificate'] = ['sfzh', 'certificate_id','certificate_type','start_time','end_time']
edge_table_info['edge_case_victim_person'] = ['asjbh', 'sfzh', 'start_time','end_time']
edge_table_info['edge_person_same_order'] = ['sfzh1', 'sfzh2', 'order_type', 'start_time','end_time']
edge_table_info['edge_case_link_autolpn'] = ['asjbh', 'autolpn', 'start_time','end_time']



# 所有边关系配置
# key为边表，值依次为，start_id 的依赖表，end_id 的依赖表，start_id 的依赖字段，end_id 的依赖表字段
edge_info = {
    'edge_person_reserve_airline': ['vertex_person', 'zjhm', 'vertex_airline', 'airlineid'],
    'edge_person_spouse_person': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_find_common_greatgrand': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_groupcall': ['vertex_phonenumber', 'phone', 'vertex_phonenumber', 'phone'],
    'edge_person_smz_phone': ['vertex_person', 'zjhm', 'vertex_phonenumber', 'phone'],
    'edge_groupmsg': ['vertex_phonenumber', 'phone', 'vertex_phonenumber', 'phone'],
    'edge_find_spouse': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_find_father': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_find_mother': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_find_grandparents': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_find_greatgrands': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_person_fatheris_person': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_person_stay_hotel': ['vertex_person', 'zjhm', 'vertex_hotel', 'lgdm'],
    'edge_person_reserve_trainline': ['vertex_person', 'zjhm', 'vertex_trainline', 'trianid'],
    'edge_person_guardianis_person': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_find_common_grand': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_person_attend_school': ['vertex_person', 'zjhm', 'vertex_school', 'school_id'],
    'edge_person_own_autolpn': ['vertex_person', 'zjhm', 'vertex_autolpn', 'autolpn'],
    'edge_person_motheris_person': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_find_common_mother': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_person_checkin_airline': ['vertex_person', 'zjhm', 'vertex_airline', 'airlineid'],
    'edge_person_arrived_airline': ['vertex_person', 'zjhm', 'vertex_airline', 'airlineid'],
    'edge_find_common_father': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_same_hotel_house': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_person_with_airline_travel': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_person_with_trainline_travel': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_person_with_internetbar_surfing': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'relation_score_res': ['vertex_person', 'zjhm', 'vertex_person', 'zjhm'],
    'edge_person_legal_com': ['vertex_person', 'zjhm', 'vertex_company', 'company'],
    'edge_phone_call_alert': ['vertex_phonenumber', 'phone', 'vertex_alert', 'jqbh'],
    'edge_case_reportby_person': ['vertex_person', 'zjhm', 'vertex_case', 'asjbh'],
    'edge_alert_link_case': ['vertex_alert', 'jqbh', 'vertex_case', 'asjbh'],
    'edge_person_surfing_internetbar': ['vertex_person', 'zjhm', 'vertex_internetbar', 'siteid'],
    'edge_phone_send_package': ['vertex_phonenumber', 'phone', 'vertex_package', 'packageid'],
    'edge_phone_receive_package': ['vertex_phonenumber', 'phone', 'vertex_package', 'packageid'],
    'edge_phone_sendpackage_phone': ['vertex_phonenumber', 'phone', 'vertex_phonenumber', 'phone'],
    'edge_phonenumber_use_imei': ['vertex_phonenumber', 'phone', 'vertex_imei', 'imei'],
    'edge_phonenumber_use_imsi': ['vertex_phonenumber', 'phone', 'vertex_imsi', 'imsi'],
    'edge_person_open_phone': ['vertex_person', 'zjhm', 'vertex_phonenumber', 'phone'],
    'edge_person_link_phone': ['vertex_person', 'zjhm', 'vertex_phonenumber', 'phone'],
    ##网络标识码
    # 'edge_qq_link_group': ['vertex_qq', 'qq', 'vertex_qq_group', 'groupid'],
    'edge_qq_link_qq': ['vertex_qq', 'qq', 'vertex_qq', 'qq'],
    # 'edge_person_own_qq': ['vertex_person', 'zjhm', 'vertex_qq', 'qq'],
    'find_qq_common_group': ['vertex_qq', 'qq', 'vertex_qq', 'qq'],
    ## 关系库新增
    'edge_person_stay_kss':['vertex_person','zjhm','vertex_jail','unitid'],
    'edge_person_stay_jds':['vertex_person','zjhm','vertex_jail','unitid'],
    'edge_person_kinsfolk_person':['vertex_person','zjhm','vertex_person','zjhm'],
    'edge_person_rent_house':['vertex_person','zjhm','vertex_house','house_id'],
    'edge_person_own_house':['vertex_person','zjhm','vertex_house','house_id'],
    'edge_person_live_house':['vertex_person','zjhm','vertex_house','house_id'],
    'edge_person_link_qq':['vertex_person','zjhm','vertex_qq','qq'],
    'edge_person_link_sinabolg':['vertex_person','zjhm','vertex_sina','sina'],
    'edge_person_link_wechat':['vertex_person','zjhm','vertex_wechat','wechat'],
    'edge_phone_link_qq':['vertex_phonenumber','phone','vertex_qq','qq'],
    'edge_phone_link_sinabolg':['vertex_phonenumber','phone','vertex_sina','sina'],
    'edge_phone_link_wechat':['vertex_person','phone','vertex_wechat','wechat'],
    'edge_person_link_certificate':['vertex_person','zjhm','vertex_certificate','certificate_id'],
    'edge_case_victim_person':['vertex_case','asjbh','vertex_person','zjhm'],
    'edge_person_same_order':['vertex_person','zjhm','vertex_person','zjhm'],
    'edge_case_link_autolpn':['vertex_case','asjbh','vertex_autolpn','autolpn'],
    }

