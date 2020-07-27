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
        'vertex_phonenumber': 31000000000,	# 电话 20亿空间
    }
    return table_index[tablename]


# 所有表字段配置
vertex_table_info = OrderedDict()
vertex_table_info['vertex_person'] = ['zjhm', 'zjlx', 'gj', 'xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                                      'whcd', 'hyzk', 'zzmm', 'hkszdxz', 'sjjzxz']
vertex_table_info['vertex_phonenumber'] = ['phone', 'xm','province', 'city']
vertex_table_info['vertex_imsi'] = ['imsi']
vertex_table_info['vertex_imei'] = ['imei']
vertex_table_info['vertex_autolpn'] = ['autolpn','hpzl']
vertex_table_info['vertex_vehicle'] = ['vehicle', 'fdjh', 'cllx', 'csys']
vertex_table_info['vertex_case'] = ['asjbh', 'ajmc', 'asjfskssj', 'asjfsjssj', 'asjly', 'ajlb', 'fxasjsj',
                                    'fxasjdd_dzmc', 'jyaq', 'asjswry_rs', 'asjssry_rs', 'ssjzrmby', 'slsj',
                                    'sldw_gajgjgdm', 'sldw_gajgmc', 'larq']
vertex_table_info['vertex_jail'] = ['unitid', 'unitname', 'unitname_s', 'unitkindid', 'dwdz']
vertex_table_info['vertex_hukou'] = ['hukouid', 'hukou']
vertex_table_info['vertex_internetbar'] = ['siteid', 'regtime', 'serial', 'sitetype', 'businessstate', 'title',
                                           'address', 'sp', 'servercount', 'terminalcount', 'nettype', 'employeecount',
                                           'ownercertnumber']
vertex_table_info['vertex_ip'] = ['ip', 'tablename']
vertex_table_info['vertex_hotel'] = ['lgdm', 'qiyemc', 'yyzz', 'xxdz']
vertex_table_info['vertex_airline'] = ['airlineid', 'hbh', 'hbrq', 'yjqfsj', 'yjddsj', 'sfd', 'mdd']
vertex_table_info['vertex_ssaccount'] = ['sbkh', 'tablename']
vertex_table_info['vertex_trainline'] = ['trianid', 'cc', 'fcrq']
vertex_table_info['vertex_email'] = ['email', 'nick', 'tablename']
vertex_table_info['vertex_package'] = ['packageid', 'unitcode', 'orderid', 'accepttime', 'inputtime', 'realarrivetime',
                                       'acceptoperatorphone', 'sendoperatorphone', 'sendername', 'sendercardid',
                                       'senderpostcode', 'senderphone', 'sendernation', 'sendercountry',
                                       'senderaddress', 'receivername', 'receivercardid', 'receiverpostcode',
                                       'receiverphone', 'receivernation', 'receivercountry', 'receiveraddress',
                                       'amountreceivable', 'currency', 'itemname', 'itemquantity', 'totalweight',
                                       'totalsize', 'insurancevalue', 'itemremark', 'senderlongitude', 'senderlatitude',
                                       'receiverlongitude', 'receiverlatitude', 'tablename']
vertex_table_info['vertex_qq'] = ['account', 'tablename']
vertex_table_info['vertex_wechat'] = ['account', 'tablename']
vertex_table_info['vertex_certificate'] = ['certificateid', 'zjhm', 'zjlx', 'zjname', 'dz']
vertex_table_info['vertex_school'] = ['school_id', 'name', 'history_name','school_address']
vertex_table_info['vertex_company'] = ['company', 'dwmc', 'dwdz', 'zzjgdm', 'tyshdm', 'clrq', 'jyzt', 'jyfw',
                                       'dwlx', 'zczb', 'cyrs', 'hylb', 'dwscdz', 'bgxq', 'bgrq']


# 边字段配置
edge_table_info = OrderedDict()
edge_table_info['edge_person_reserve_airline'] = ['sfzh', 'airlineid', 'hbh', 'hbrq', 'sfd', 'mdd']
edge_table_info['edge_person_spouse_person'] = ['sfzhnan', 'sfzhnv', 'start_time', 'end_time']
edge_table_info['edge_person_checkin_train'] = ['sfzh', 'tgkkdm', 'tgkkmc', 'tgsj', 'tablename']
edge_table_info['edge_certificate_open_phone'] = ['certificateid', 'phone', 'start_time', 'end_time', 'is_xiaohu',
                                                  'tablename']
edge_table_info['edge_person_has_ssaccount'] = ['sfzh', 'sbkh', 'link_time', 'tablename']
edge_table_info['edge_find_common_greatgrand'] = ['sfzh1', 'sfzh2']
edge_table_info['edge_find_spouse'] = ['sfzhnan', 'sfzhnnv']
edge_table_info['edge_person_work_school'] = ['start_person', 'end_school_id', 'start_time', 'end_time', 'jszc',
                                              'cjgzsj', 'gzxz', 'sjkm', 'tablename']
edge_table_info['edge_find_father'] = ['sfzh', 'fqsfzh']
edge_table_info['edge_find_grandparents'] = ['sfzh', 'grandsfzh']
edge_table_info['edge_find_greatgrands'] = ['sfzh', 'greatgrandsfzh']
edge_table_info['edge_find_mother'] = ['sfzh', 'mqsfzh']
edge_table_info['edge_person_attend_college'] = ['start_person', 'end_college_id', 'start_time', 'end_time', 'zymc',
                                                 'xuehao', 'xuezhi', 'tablename']
edge_table_info['edge_person_fatherIs_person'] = ['sfzh', 'fqsfzh', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_case_SuspectIs_person'] = ['asjbh', 'sfzh', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_certificate_link_phone'] = ['certificateid', 'phone', 'start_time', 'end_time', 'num',
                                                  'tablename']
edge_table_info['edge_vehicle_sameguarantee'] = ['start_person', 'end_person']
edge_table_info['edge_person_stay_hotel'] = ['sfzh', 'lgdm', 'start_time', 'end_time', 'num']
edge_table_info['edge_internetbar_use_ip'] = ['siteid', 'ip', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_reserve_trainline'] = ['sfzh', 'trianid', 'cc', 'spsj', 'fcrq', 'zwh', 'fz', 'dz',
                                                    'tablename']
edge_table_info['edge_wechat_msg_wechat'] = ['start_account', 'end_account', 'start_time', 'end_time', 'num',
                                             'tablename']
edge_table_info['edge_person_arrived_airline'] = ['sfzh', 'airlineid', 'hbh', 'cwdj', 'zwh', 'hbrq', 'yjddsj', 'sfd',
                                                  'mdd', 'tablename']
edge_table_info['edge_phone_send_package'] = ['phone', 'packageid', 'send_time', 'tablename']
edge_table_info['edge_person_guardianIs_person'] = ['sfzh', 'jhrsfzh', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_guaranteeis_person'] = ['start_person', 'end_person', 'start_time', 'end_time',
                                                     'tablename']
edge_table_info['edge_find_common_grand'] = ['sfzh1', 'sfzh2']
edge_table_info['edge_vehicle_has_autolpn'] = ['start_vehicle', 'end_autolpn', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_email_use_phone'] = ['email', 'phone', 'start_time', 'end_time', 'num', 'tablename']
edge_table_info['edge_person_hire_house'] = ['start_person', 'end_house_id', 'fwdz', 'start_time', 'end_time', 'htbh',
                                             'xm', 'hx', 'fwlx', 'phone', 'mph', 'tablename']
edge_table_info['edge_person_maintain_vehicle'] = ['start_person', 'end_vehicle', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_link_hukou'] = ['sfzh', 'hukouid', 'start_time', 'end_time', 'isfl', 'yhzgx', 'tablename']
edge_table_info['edge_person_surfing_internetbar'] = ['sfzh', 'siteid', 'start_time', 'end_time', 'num', 'tablename']
edge_table_info['edge_phone_list_phone'] = ['start_phone', 'end_phone', 'start_time', 'end_time', 'relationship_name',
                                            'relationship_email', 'relationship_address', 'tablename']
edge_table_info['edge_person_attend_school'] = ['start_person', 'end_school_id', 'start_time', 'end_time', 'fymc', 'bj',
                                                'zymc', 'xuezhi', 'tablename']
edge_table_info['edge_person_own_autolpn'] = ['start_person', 'end_autolpn', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_smz_phone'] = ['start_person', 'end_phone', 'start_time', 'end_time', 'xm', 'cnt',
                                            'matches', 'match_src', 'time_rn', 'matches_rn', 'cert_src', 'cert_weigth']
edge_table_info['edge_case_reportby_person'] = ['asjbh', 'sfzh', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_motherIs_person'] = ['sfzh', 'mqsfzh', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_case_suspicious_person'] = ['asjbh', 'sfzh', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_stay_jail_qjs'] = ['sfzh', 'jsbh', 'sylx', 'rsyy', 'start_time', 'end_time', 'fjh',
                                                'tablename']
edge_table_info['edge_person_stay_jail_jls'] = ['sfzh', 'jsbh', 'sylx', 'rsyy', 'start_time', 'end_time', 'fjh',
                                                'rsgjmjid', 'csgjmjid', 'tablename']
edge_table_info['edge_person_open_phone'] = ['sfzh', 'phone', 'start_time', 'end_time', 'is_xiaohu', 'tablename']
edge_table_info['edge_qq_msg_qq'] = ['start_account', 'end_account', 'start_time', 'end_time', 'num', 'tablename']
edge_table_info['edge_vehicle_sameintroducer'] = ['start_person', 'end_person']
edge_table_info['edge_qq_use_phone'] = ['account', 'phone', 'start_time', 'end_time', 'num', 'tablename']
edge_table_info['edge_person_work_com'] = ['start_person', 'end_company', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_visit_person'] = ['tfrsfzh', 'sfzh', 'start_time', 'end_time', 'num', 'tablename']
edge_table_info['edge_person_reserve_coachline'] = ['sfzh', 'coachid', 'spsj', 'zwh', 'gpczmc', 'tablename']
edge_table_info['edge_person_principal_com'] = ['start_person', 'end_company', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_introduceris_person'] = ['start_person', 'end_person', 'start_time', 'end_time',
                                                      'tablename']
edge_table_info['edge_wechat_use_phone'] = ['account', 'phone', 'start_time', 'end_time', 'num', 'tablename']
edge_table_info['edge_find_common_mother'] = ['sfzh1', 'sfzh2']
edge_table_info['edge_same_kss'] = ['sfzh1', 'sfzh2', 'data', 'num', 'start_time', 'end_time']
edge_table_info['edge_same_qjs'] = ['sfzh1', 'sfzh2', 'data', 'num', 'start_time', 'end_time']
edge_table_info['edge_person_buy_vehicle'] = ['start_person', 'end_vehicle', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_invest_com'] = ['start_person', 'end_company', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_link_phone'] = ['sfzh', 'phone', 'start_time', 'end_time', 'linknum', 'tablename']
edge_table_info['edge_phonenumber_use_imei'] = ['phone', 'imei', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_school_sub_college'] = ['start_school_id', 'end_college_id', 'tablename']
edge_table_info['edge_person_hire_autolpn'] = ['start_person', 'end_autolpn', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_phonenumber_use_imsi'] = ['phone', 'imsi', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_phone_sendpackage_phone'] = ['start_phone', 'end_phone', 'start_time', 'end_time', 'num',
                                                   'tablename']
edge_table_info['edge_person_checkin_airline'] = ['sfzh', 'airlineid', 'hbh', 'cwdj', 'zwh', 'hbrq', 'zjsj', 'djk',
                                                  'sfd', 'mdd', 'tablename']
edge_table_info['edge_person_stay_jail_kss'] = ['sfzh', 'jsbh', 'sylx', 'sxzm', 'start_time', 'end_time', 'fjh',
                                                'tablename']
edge_table_info['edge_certificate_link_person'] = ['certificateid', 'sfzh', 'start_time', 'end_time', 'num',
                                                   'tablename']
edge_table_info['edge_person_link_drivinglicense'] = ['sfzh', 'drivinglicense', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_has_nchca'] = ['sfzh', 'nchca', 'link_time', 'tablename']
edge_table_info['edge_find_common_father'] = ['sfzh1', 'sfzh2']
edge_table_info['edge_phone_receive_package'] = ['phone', 'packageid', 'receive_time', 'tablename']
edge_table_info['edge_find_common_guardian'] = ['sfzh1', 'sfzh2']
edge_table_info['edge_person_legal_com'] = ['start_person', 'end_company', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_work_com2'] = ['start_person', 'end_company', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_same_kss_house'] = ['sfzh1', 'sfzh2', 'data', 'num', 'start_time', 'end_time']
edge_table_info['edge_same_qjs_house'] = ['sfzh1', 'sfzh2', 'data', 'num', 'start_time', 'end_time']
edge_table_info['edge_email_send_email'] = ['start_email', 'end_email', 'start_time', 'end_time', 'num', 'tablename']
edge_table_info['edge_case_VictimIs_person'] = ['asjbh', 'sfzh', 'start_time', 'end_time', 'tablename']
edge_table_info['edge_person_open_vbn'] = ['sfzh', 'kdzh', 'start_time', 'end_time', 'is_xiaohu', 'khxm', 'azdz',
                                           'tablename']
edge_table_info['edge_house_hiremate'] = ['start_person', 'end_person', 'fwdz', 'start_time', 'end_time']
edge_table_info['edge_company_workmate'] = ['start_person', 'end_person', 'start_time', 'end_time', 'dwmc']
edge_table_info['edge_find_common_hukou'] = ['sfzh1', 'sfzh2', 'hukou']
edge_table_info['edge_groupmsg'] = ['start_phone', 'end_phone', 'start_time', 'end_time', 'message_number']
edge_table_info['edge_groupcall'] = ['start_phone', 'end_phone', 'start_time', 'end_time', 'call_total_duration',
                                     'call_total_times']
edge_table_info['edge_same_hotel_house'] = ['sfzh1', 'sfzh2', 'num']
edge_table_info['edge_same_hotel'] = ['sfzh1', 'sfzh2', 'num']
edge_table_info['edge_schoolmate'] = ['start_person', 'end_person', 'start_time', 'end_time', 'name']
edge_table_info['edge_phone_sameimei'] = ['start_phone', 'end_phone', 'start_time', 'end_time', 'times']
edge_table_info['edge_person_with_coach_travel'] = ['sfzh1', 'sfzh2', 'start_time', 'end_time', 'num']
edge_table_info['edge_person_with_airline_travel'] = ['sfzh1', 'sfzh2', 'start_time', 'end_time', 'num']
edge_table_info['edge_person_with_trainline_travel'] = ['sfzh1', 'sfzh2', 'start_time', 'end_time', 'num']
edge_table_info['edge_person_with_internetbar_surfing'] = ['sfzh1', 'sfzh2', 'start_time', 'end_time', 'num']
edge_table_info['edge_phone_wechat_phone'] = ['start_phone', 'end_phone', 'account', 'start_time', 'end_time']
edge_table_info['edge_phone_qq_phone'] = ['start_phone', 'end_phone', 'account', 'start_time', 'end_time']
edge_table_info['edge_phone_email_phone'] = ['start_phone', 'end_phone', 'account', 'start_time', 'end_time']
edge_table_info['edge_person_with_car'] = ['start_person', 'end_autolpn', 'count', 'fullcount']
edge_table_info['relation_score_res'] = ['sfzh1', 'sfzh2', 'data', 'last_tb_score']

# 所有边关系配置
# key为边表，值依次为，start_id 的依赖表，end_id 的依赖表，start_id 的依赖表，end_id 的依赖表字段
edge_info = {'edge_person_reserve_airline': ['vertex_person_jg', 'zjhm', 'vertex_airline_jg', 'airlineid'],
    'edge_person_spouse_person': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_checkin_train': ['vertex_person_jg', 'zjhm', 'vertex_traincheckin_jg', 'tgkkdm'],
    'edge_certificate_open_phone': ['vertex_certificate_jg', 'certificateid', 'vertex_phonenumber_jg', 'phone'],
    'edge_person_has_ssaccount': ['vertex_person_jg', 'zjhm', 'vertex_ssaccount_jg', 'sbkh'],
    'edge_find_common_greatgrand': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_groupcall': ['vertex_phonenumber_jg', 'phone', 'vertex_phonenumber_jg', 'phone'],
    'edge_find_spouse': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_work_school': ['vertex_person_jg', 'zjhm', 'vertex_school_jg', 'school_id'],
    'edge_find_father': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_find_mother': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_find_grandparents': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_find_greatgrands': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_attend_college': ['vertex_person_jg', 'zjhm', 'vertex_college_jg', 'college_id'],
    'edge_person_fatherIs_person': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_case_SuspectIs_person': ['vertex_case_jg', 'asjbh', 'vertex_person_jg', 'zjhm'],
    'edge_certificate_link_phone': ['vertex_certificate_jg', 'certificateid', 'vertex_phonenumber_jg', 'phone'],
    'edge_vehicle_sameguarantee': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_stay_hotel': ['vertex_person_jg', 'zjhm', 'vertex_hotel_jg', 'lgdm'],
    'edge_internetbar_use_ip': ['vertex_internetbar_jg', 'siteid', 'vertex_ip_jg', 'ip'],
    'edge_person_reserve_trainline': ['vertex_person_jg', 'zjhm', 'vertex_trainline_jg', 'trianid'],
    'edge_wechat_msg_wechat': ['vertex_wechat_jg', 'account', 'vertex_wechat_jg', 'account'],
    'edge_person_arrived_airline': ['vertex_person_jg', 'zjhm', 'vertex_airline_jg', 'airlineid'],
    'edge_phone_send_package': ['vertex_phonenumber_jg', 'phone', 'vertex_package_jg', 'packageid'],
    'edge_person_guardianIs_person': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_guaranteeis_person': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_find_common_grand': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_vehicle_has_autolpn': ['vertex_vehicle_jg', 'vehicle', 'vertex_autolpn_jg', 'autolpn'],
    'edge_email_use_phone': ['vertex_email_jg', 'email', 'vertex_phonenumber_jg', 'phone'],
    'edge_person_hire_house': ['vertex_person_jg', 'zjhm', 'vertex_house_jg', 'house_id'],
    'edge_person_maintain_vehicle': ['vertex_person_jg', 'zjhm', 'vertex_vehicle_jg', 'vehicle'],
    'edge_person_link_hukou': ['vertex_person_jg', 'zjhm', 'vertex_hukou_jg', 'hukouid'],
    'edge_person_surfing_internetbar': ['vertex_person_jg', 'zjhm', 'vertex_internetbar_jg', 'siteid'],
    'edge_phone_list_phone': ['vertex_phonenumber_jg', 'phone', 'vertex_phonenumber_jg', 'phone'],
    'edge_person_attend_school': ['vertex_person_jg', 'zjhm', 'vertex_school_jg', 'school_id'],
    'edge_person_own_autolpn': ['vertex_person_jg', 'zjhm', 'vertex_autolpn_jg', 'autolpn'],
    'edge_person_stay_jail_jls': ['vertex_person_jg', 'zjhm', 'vertex_jail_jg', 'unitid'],
    'edge_person_smz_phone': ['vertex_person_jg', 'zjhm', 'vertex_phonenumber_jg', 'phone'],
    'edge_case_reportby_person': ['vertex_case_jg', 'asjbh', 'vertex_person_jg', 'zjhm'],
    'edge_person_motherIs_person': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_case_suspicious_person': ['vertex_case_jg', 'asjbh', 'vertex_person_jg', 'zjhm'],
    'edge_person_stay_jail_qjs': ['vertex_person_jg', 'zjhm', 'vertex_jail_jg', 'unitid'],
    'edge_person_open_phone': ['vertex_person_jg', 'zjhm', 'vertex_phonenumber_jg', 'phone'],
    'edge_qq_msg_qq': ['vertex_qq_jg', 'account', 'vertex_qq_jg', 'account'],
    'edge_vehicle_sameintroducer': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_qq_use_phone': ['vertex_qq_jg', 'account', 'vertex_phonenumber_jg', 'phone'],
    'edge_person_work_com': ['vertex_person_jg', 'zjhm', 'vertex_company_jg', 'company'],
    'edge_person_visit_person': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_reserve_coachline': ['vertex_person_jg', 'zjhm', 'vertex_coachline_jg', 'coachid'],
    'edge_person_principal_com': ['vertex_person_jg', 'zjhm', 'vertex_company_jg', 'company'],
    'edge_person_introduceris_person': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_same_hotel_house': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_same_hotel': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_groupmsg': ['vertex_phonenumber_jg', 'phone', 'vertex_phonenumber_jg', 'phone'],
    'edge_wechat_use_phone': ['vertex_wechat_jg', 'account', 'vertex_phonenumber_jg', 'phone'],
    'edge_company_workmate': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_find_common_mother': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_same_kss': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_same_qjs': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_buy_vehicle': ['vertex_person_jg', 'zjhm', 'vertex_vehicle_jg', 'vehicle'],
    'edge_person_invest_com': ['vertex_person_jg', 'zjhm', 'vertex_company_jg', 'company'],
    'edge_house_hiremate': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_link_phone': ['vertex_person_jg', 'zjhm', 'vertex_phonenumber_jg', 'phone'],
    'edge_phonenumber_use_imei': ['vertex_phonenumber_jg', 'phone', 'vertex_imei_jg', 'imei'],
    'edge_school_sub_college': ['vertex_school_jg', 'school_id', 'vertex_college_jg', 'college_id'],
    'edge_person_hire_autolpn': ['vertex_person_jg', 'zjhm', 'vertex_autolpn_jg', 'autolpn'],
    'edge_phonenumber_use_imsi': ['vertex_phonenumber_jg', 'phone', 'vertex_imsi_jg', 'imsi'],
    'edge_phone_sendpackage_phone': ['vertex_phonenumber_jg', 'phone', 'vertex_phonenumber_jg', 'phone'],
    'edge_person_checkin_airline': ['vertex_person_jg', 'zjhm', 'vertex_airline_jg', 'airlineid'],
    'edge_person_stay_jail_kss': ['vertex_person_jg', 'zjhm', 'vertex_jail_jg', 'unitid'],
    'edge_certificate_link_person': ['vertex_certificate_jg', 'certificateid', 'vertex_person_jg', 'zjhm'],
    'edge_person_link_drivinglicense': ['vertex_person_jg', 'zjhm', 'vertex_drivinglicense_jg', 'drivinglicense'],
    'edge_person_has_nchca': ['vertex_person_jg', 'zjhm', 'vertex_nchcAccount_jg', 'nchca'],
    'edge_find_common_father': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_phone_receive_package': ['vertex_phonenumber_jg', 'phone', 'vertex_package_jg', 'packageid'],
    'edge_find_common_guardian': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_legal_com': ['vertex_person_jg', 'zjhm', 'vertex_company_jg', 'company'],
    'edge_person_work_com2': ['vertex_person_jg', 'zjhm', 'vertex_company_jg', 'company'],
    'edge_same_kss_house': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_same_qjs_house': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_email_send_email': ['vertex_email_jg', 'email', 'vertex_email_jg', 'email'],
    'edge_case_VictimIs_person': ['vertex_case_jg', 'asjbh', 'vertex_person_jg', 'zjhm'],
    'edge_person_open_vbn': ['vertex_person_jg', 'zjhm', 'vertex_vbn_jg', 'kdzh'],
    'edge_find_common_hukou': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_phone_sameimei': ['vertex_phonenumber_jg', 'phone', 'vertex_phonenumber_jg', 'phone'],
    'edge_schoolmate': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_with_coach_travel': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_with_airline_travel': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_with_trainline_travel': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_person_with_internetbar_surfing': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'],
    'edge_phone_wechat_phone': ['vertex_phonenumber_jg', 'phone', 'vertex_phonenumber_jg', 'phone'],
    'edge_phone_qq_phone': ['vertex_phonenumber_jg', 'phone', 'vertex_phonenumber_jg', 'phone'],
    'edge_phone_email_phone': ['vertex_phonenumber_jg', 'phone', 'vertex_phonenumber_jg', 'phone'],
    'edge_person_with_car': ['vertex_person_jg', 'zjhm', 'vertex_autolpn_jg', 'autolpn'],
    'relation_score_res': ['vertex_person_jg', 'zjhm', 'vertex_person_jg', 'zjhm'], }