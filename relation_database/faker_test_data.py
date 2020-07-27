# -*- coding: utf-8 -*-
# @Time    : 2020/3/7 13:30
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : company.py
# @Software: PyCharm
# @content : 工商相关信息
import sys, os
from pyspark import SparkConf
from pyspark.sql import SparkSession
# from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time, copy, re, math,random
from datetime import datetime, timedelta,date
import logging
from collections import OrderedDict
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')
#warehouse_location = '/user/hive/warehouse/'
conf=SparkConf().set('spark.driver.maxResultSize', '2g')
conf.set('spark.yarn.executor.memoryOverhead', '10g')
conf.set('spark.yarn.am.cores', 4)
conf.set('spark.executor.memory', '20g')
conf.set('spark.executor.instances', 10)
conf.set('spark.executor.cores', 4)
conf.set('spark.executor.extraJavaOptions', '-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC')
#conf.set("spark.sql.warehouse.dir", warehouse_location)


spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

from common import *
path_prefix = '/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/faker_jg'

def num_to_ch(num):
    """
    功能说明：将阿拉伯数字 ===> 转换成中文数字（适用于[0, 10000)之间的阿拉伯数字 ）
    """
    _nams = [u'赵',u'张',u'诸葛',u'慕容',u'东方',u'上官',u'宇文',u'钟离',
             u'单于',u'刘',u'邓',u'王',u'司马',u'长孙',u'鱼',u'山',
             u'吴',u'韩',u'水',u'柳',u'方',u'林',u'车',u'班',
             u'文',u'金',u'华',u'叶',u'宁',u'井',u'怀',u'白',
             u'曹',u'仇',u'红',u'农',u'东',u'空',u'冷',u'艾',
             u'牧',u'卓',u'龙',u'令狐',u'公羊',u'夏侯',u'夏',u'高']
    num = int(num)
    _MAPPING = (u'零', u'一', u'二', u'三', u'四', u'五', u'六', u'七', u'八', u'九', )
    _P0 = (u'', u'十', u'百', u'千', )
    _S4 = 10 ** 4
    if num < 0 or num >= _S4:
        return None
    if num < 10:
        return random.choice(_nams)+_MAPPING[num]
    else:
        lst = []
        while num >= 10:
            lst.append(num % 10)
            num = num // 10
        lst.append(num)
        c = len(lst)    # 位数
        result = u''
        for idx, val in enumerate(lst):
            if val != 0:
                result += _P0[idx] + _MAPPING[val]
            if idx < c - 1 and lst[idx + 1] == 0:
                result += u'零'
        result = result[::-1]
        if result[:2] == u"一十":
            result = result[1:]
        if result[-1:] == u"零":
            result = result[:-1]
        result = result.replace(u"十","").replace(u"百","")
        return random.choice(_nams)+result


def desens_zjhm(zjhm):
    try:
        pre_zjhm = zjhm[-4:]
        return pre_zjhm + zjhm[4:-4]+zjhm[2:6] + zjhm[:2]
    except:
        return zjhm[1:-1]
def desens_xm(xm):
    try:
        num = random.randint(1,99)
        return num_to_ch(num)
    except:
        pass

def desens_dz(dz):
    try:
        province = [u'喇嘛山',u'峨眉山',u'天山',u'昆仑山',u'奶奶山',u'猫儿山',u'唐古拉山',u'突突山']
        city = [u'赵',u'张',u'诸葛',u'慕容',u'东方',u'上官',u'宇文',u'钟离',
             u'单于',u'刘',u'邓',u'王',u'司马',u'长孙',u'鱼',u'山',
             u'吴',u'韩',u'水',u'柳',u'方',u'林',u'车',u'班',
             u'文',u'金',u'华',u'叶',u'宁',u'井',u'怀']
        num = random.randint(11,99)
        return random.choice(province) + u"省" + random.choice(city) + u"家市" + str(num) + u"号"
    except:
        pass

spark.udf.register('desens_zjhm',desens_zjhm,StringType())
spark.udf.register('desens_xm',desens_xm,StringType())
spark.udf.register('desens_dz',desens_dz,StringType())


vertex_table_info = OrderedDict()
vertex_table_info['vertex_person'] = ['desens_zjhm(zjhm) zjhm', 'zjlx', 'gj', 'desens_xm(xm) xm', 'ywxm', 'zym', 'xb', 'csrq', 'mz', 'jg',
                                      'whcd', 'hyzk', 'zzmm', 'desens_dz(hkszdxz) hkszdxz', 'desens_dz(sjjzxz) sjjzxz']
vertex_table_info['vertex_phonenumber'] = ['desens_zjhm(phone) phone', 'desens_zjhm(xm) xm','province', 'city']
vertex_table_info['vertex_autolpn'] = ['autolpn','hpzl']
vertex_table_info['vertex_imsi'] = ['imsi']
vertex_table_info['vertex_imei'] = ['imei']
vertex_table_info['vertex_domic'] = ['domic_num', 'domic_type']
vertex_table_info['vertex_hotel'] = ['lgdm', 'qiyemc', 'yyzz', 'xiangxidizhi']
vertex_table_info['vertex_airline'] = ['airlineid', 'hbh', 'hbrq', 'yjqfsj', 'yjddsj', 'sfd', 'mdd']
vertex_table_info['vertex_trainline'] = ['trianid', 'cc', 'fcrq']
vertex_table_info['vertex_school'] = ['school_id', 'name', 'history_name','school_address']
vertex_table_info['vertex_company'] = ['company', 'dwmc', 'desens_dz(dwdz) dwdz', 'zzjgdm', 'tyshdm', 'clrq', 'jyzt', 'jyfw',
                                       'dwlx', 'zczb', 'cyrs', 'hylb', 'dwscdz', 'bgxq', 'bgrq']
vertex_table_info['vertex_alert'] = ['jqbh', 'jqxz', 'jqfxdz', 'jyqk','jjsj']
vertex_table_info['vertex_internetbar'] = ['siteid','sitetype','title','desens_dz(address) address']
vertex_table_info['vertex_case'] = ['asjbh','ajmc','asjfskssj','asjfsjssj','asjly','ajlb','fxasjsj','fxasjdd_dzmc',
                                        'jyaq','ajbs','larq','city','link_app']
vertex_table_info['vertex_package'] = ['packageid','company','orderid','accepttime','inputtime',
                                          'senderphone','senderaddress','receiverphone','receiveraddress']
vertex_table_info['vertex_qq_group'] = ['groupid','groupname']
vertex_table_info['vertex_qq'] = ['desens_zjhm(qq) qq','nickname','sex','city','mobile','email','realname','csrq','college','area','province','regis_time','regis_ip','study','sign_name']
vertex_table_info['vertex_qq'] = ['desens_zjhm(qq) qq']
vertex_table_info['vertex_sina'] = ['sina']
vertex_table_info['vertex_wechat'] = ['desens_zjhm(wechat) wechat']
vertex_table_info['vertex_jail'] = ['unitid','unitname','unittype']
vertex_table_info['vertex_house'] = ['house_id','house_addr']
vertex_table_info['vertex_certificate'] = ['certificateid','desens_zjhm(certificate) certificate','certificate_type']
vertex_table_info['vertex_law_docment'] = ['doc_id','case_code','title','sentence_date','trial_court','case_type',
                     'accuser','defendant','case_results','notice_content']
# 边字段配置 所有边的属性信息
# tips 第一个字段为起始节点， 第二个字段为结束节点，比如 desens_zjhm(sfzh) sfzh airlineid 人节点->飞机节点
# 与后面的有强逻辑关系， 不可随意更换，
edge_table_info = OrderedDict()

##家庭关系
edge_table_info['edge_person_spouse_person'] = ['desens_zjhm(sfzhnan) sfzhnan', 'desens_zjhm(sfzhnv) sfzhnv', 'type_name', 'start_time', 'end_time']
edge_table_info['edge_find_common_greatgrand'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2','type_name', 'start_time','end_time']
edge_table_info['edge_find_common_father'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2','type_name','start_time','end_time']
edge_table_info['edge_find_common_mother'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2','type_name','start_time','end_time']
edge_table_info['edge_find_common_grand'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2','type_name','start_time','end_time']
edge_table_info['edge_find_grandparents'] = ['desens_zjhm(sfzh) sfzh', 'desens_zjhm(grandsfzh) grandsfzh','type_name', 'start_time','end_time']
edge_table_info['edge_find_greatgrands'] = ['desens_zjhm(sfzh) sfzh', 'desens_zjhm(greatgrandsfzh) greatgrandsfzh','type_name', 'start_time','end_time']
edge_table_info['edge_person_fatheris_person'] = ['desens_zjhm(sfzh) sfzh', 'desens_zjhm(fqsfzh) fqsfzh', 'type_name', 'start_time', 'end_time']
edge_table_info['edge_person_motheris_person'] = ['desens_zjhm(sfzh) sfzh','desens_zjhm(mqsfzh) mqsfzh', 'type_name', 'start_time', 'end_time']
edge_table_info['edge_person_guardianis_person'] = ['desens_zjhm(sfzh) sfzh', 'desens_zjhm(jhrsfzh) jhrsfzh', 'type_name', 'start_time', 'end_time']
edge_table_info['edge_person_kinsfolk_person'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2','type_name','start_time', 'end_time']
edge_table_info['edge_find_common_parent'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2','type_name','start_time', 'end_time']
## 人关联关系
edge_table_info['edge_person_attend_school'] = ['desens_zjhm(start_person) start_person', 'end_school_id','start_time','end_time','xxmc','xymc','bj','zymc','xh','xz']
edge_table_info['edge_person_stay_hotel'] = ['desens_zjhm(sfzh) sfzh', 'lgdm', 'start_time', 'end_time', 'num']
edge_table_info['edge_person_own_autolpn'] = ['desens_zjhm(sfzh) sfzh', 'autolpn']
edge_table_info['edge_case_reportby_person'] = ['desens_zjhm(sfzh) sfzh', 'asjbh', 'start_time', 'end_time']
edge_table_info['edge_person_surfing_internetbar'] = ['desens_zjhm(sfzh) sfzh', 'siteid', 'start_time','end_time','num']
edge_table_info['edge_person_smz_phone'] = ['desens_zjhm(start_person) start_person', 'desens_zjhm(end_phone) end_phone', 'weight', 'start_time', 'end_time']
edge_table_info['edge_person_open_phone'] = ['desens_zjhm(sfzh) sfzh', 'desens_zjhm(phone) phone', 'start_time', 'end_time','is_xiaohu']
edge_table_info['edge_person_stay_jail'] = ['desens_zjhm(sfzh) sfzh', 'jsbh','ajbh','jyaq', 'start_time','end_time']
edge_table_info['edge_person_link_phone'] = ['desens_zjhm(sfzh) sfzh', 'desens_zjhm(phone) phone', 'start_time', 'end_time','num']
edge_table_info['edge_person_link_phone'] = ['desens_zjhm(sfzh) sfzh', 'desens_zjhm(phone) phone', 'start_time', 'end_time','num']
edge_table_info['edge_person_rent_house'] = ['desens_zjhm(sfzh) sfzh', 'house_id','start_time','end_time', 'trade_time','trade_price']
edge_table_info['edge_person_own_house'] = ['desens_zjhm(sfzh) sfzh', 'house_id','start_time','end_time', 'trade_time','trade_price']
edge_table_info['edge_person_live_house'] = ['desens_zjhm(sfzh) sfzh', 'house_id','start_time','end_time']
edge_table_info['edge_person_link_wechat'] = ['desens_zjhm(sfzh) sfzh', 'desens_zjhm(user_account) user_account','start_time','end_time']
edge_table_info['edge_person_link_qq'] = ['desens_zjhm(sfzh) sfzh', 'desens_zjhm(user_account) user_account','start_time','end_time']
edge_table_info['edge_person_link_sinablog'] = ['desens_zjhm(sfzh) sfzh', 'desens_zjhm(user_account) user_account','start_time','end_time']
edge_table_info['edge_phone_link_sinablog'] = ['desens_zjhm(phone) phone', 'desens_zjhm(user_account) user_account','start_time','end_time']
edge_table_info['edge_person_link_certificate'] = ['desens_zjhm(sfzh) sfzh', 'certificateid','certificate_type','start_time','end_time']
edge_table_info['edge_case_victim_person'] = ['asjbh', 'desens_zjhm(sfzh) sfzh', 'start_time','end_time']
edge_table_info['edge_person_same_order'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2', 'order_type', 'start_time','end_time']
## 电话关联关系
edge_table_info['edge_phonenumber_use_imei'] = ['desens_zjhm(phone) phone', 'imei','start_time','end_time']
edge_table_info['edge_phonenumber_use_imsi'] = ['desens_zjhm(phone) phone', 'imsi','start_time','end_time']
edge_table_info['edge_phone_call_alert'] = ['desens_zjhm(phone) phone', 'jqbh', 'bjrsfzh', 'bjrxm', 'jjsj','start_time','end_time']
edge_table_info['edge_alert_link_case'] = ['jqbh', 'asjbh', 'link_time','start_time','end_time']
edge_table_info['edge_phone_send_package'] = ['desens_zjhm(phone) phone', 'packageid', 'send_time','start_time','end_time']
edge_table_info['edge_phone_receive_package'] = ['desens_zjhm(phone) phone', 'packageid', 'receive_time','start_time','end_time']
edge_table_info['edge_phone_sendpackage_phone'] = ['desens_zjhm(start_phone) start_phone', 'desens_zjhm(end_phone) end_phone', 'start_time','end_time','num']
edge_table_info['edge_phone_link_wechat'] = ['desens_zjhm(phone) phone', 'desens_zjhm(user_account) user_account','start_time','end_time']
edge_table_info['edge_phone_link_qq'] = ['desens_zjhm(phone) phone', 'desens_zjhm(user_account) user_account','start_time','end_time']
edge_table_info['edge_case_link_autolpn'] = ['asjbh', 'autolpn', 'start_time','end_time']
edge_table_info['edge_groupcall'] = ['desens_zjhm(start_phone) start_phone', 'desens_zjhm(end_phone) end_phone', 'start_time', 'end_time', 'call_total_duration','call_total_times']
edge_table_info['edge_groupmsg'] = ['desens_zjhm(start_phone) start_phone', 'desens_zjhm(end_phone) end_phone', 'start_time', 'end_time', 'message_number']
#出行关系
edge_table_info['edge_person_reserve_airline'] = ['desens_zjhm(sfzh) sfzh','airlineid','hbh','hbrq','type_name','sfd','mdd','start_time','end_time']
edge_table_info['edge_person_with_airline_travel'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2','start_time','end_time', 'num']
edge_table_info['edge_person_with_trainline_travel'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2','start_time','end_time', 'num']
edge_table_info['edge_person_reserve_trainline'] = ['desens_zjhm(sfzh) sfzh', 'trianid', 'cc', 'fcrq', 'cxh','zwh', 'fz', 'dz','start_time','end_time']
edge_table_info['edge_same_hotel_house'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2','start_time','end_time', 'num']
edge_table_info['edge_person_with_internetbar_surfing'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2','start_time','end_time', 'num']
##工商关系
edge_table_info['edge_com_shareholder_com'] = ['share_company','company','is_history','start_time','end_time']
edge_table_info['edge_accusercom_link_doc'] = ['doc_id','accuser_company','start_time','end_time']
edge_table_info['edge_defendantcom_link_doc'] = ['doc_id','defendant_company','start_time','end_time']
edge_table_info['edge_com_lawsuit_com'] = ['accuser_company','defendant_company','start_time','end_time']
edge_table_info['edge_com_loan_com'] = ['start_company','end_company','start_time','end_time']
edge_table_info['edge_company_workmate'] = ['desens_zjhm(sfzh1) sfzh1','desens_zjhm(sfzh2) sfzh2','start_time','end_time','dwmc']
edge_table_info['edge_person_legal_com'] = ['desens_zjhm(start_person) start_person', 'end_company', 'start_time', 'end_time']
edge_table_info['edge_person_work_com'] = ['desens_zjhm(start_person) start_person', 'end_company', 'start_time', 'end_time']
## p2p人物关系
edge_table_info['relation_score_res'] = ['desens_zjhm(sfzh1) sfzh1', 'desens_zjhm(sfzh2) sfzh2','data','last_tb_score', 'last_zb_score','weight_score']

def add_jg_path(tablename,standby=False):
    ## jg文件保存地址
    if standby:
        return '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_data_standby/{}'.format(tablename.lower()+"_jg")
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/jg_data/{}'.format(tablename.lower()+"_jg")

def add_save_jg_path(tablename):
    ## jg文件保存地址
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/faker_graph_data/{}'.format(tablename.lower()+"_jg")


def get_network_graph():
    ## 找一个关系比较多的联通网络 通过人 jid=510507988，9163883835 电话 jid=840137820713,340127345952
    ## 全图开始节点 结束节点关系文件 start_jid, end_jid
    res = read_parquet(spark,"/phoebus/_fileservice/users/slmp/shulianmingpin/midfile/jg",'res_tmp')
    person_v = res.where('start_jid = 510507988 or end_jid = 510507988 or start_jid = 9163883835 or end_jid = 9163883835')
    phone_v = res.where('start_jid = 840137820713 or end_jid = 840137820713 or start_jid = 340127345952 or end_jid = 340127345952')
    e_tmp = person_v.unionAll(phone_v).dropDuplicates() ## 初始边节点
    v_tmp = e_tmp.selectExpr('start_jid jid').unionAll(e_tmp.selectExpr('end_jid jid')).dropDuplicates() # 初始点节点
    ## 进行全扩展
    e_start = res.join(v_tmp,res.start_jid==v_tmp.jid,'inner').selectExpr('start_jid','end_jid')
    e_end = res.join(v_tmp,res.end_jid==v_tmp.jid,'inner').selectExpr('start_jid','end_jid')
    e = e_start.unionAll(e_end).dropDuplicates()
    v = e.selectExpr('start_jid jid').unionAll(e.selectExpr('end_jid jid')).dropDuplicates()
    # write_parquet(e,path_prefix,'faker_edge')
    # write_parquet(v,path_prefix,'faker_vertex')
    e = read_parquet(spark,path_prefix,'faker_edge')
    v = read_parquet(spark,path_prefix,'faker_vertex')
    e.persist()
    v.persist()
    for file, cols in edge_table_info.items():
        cols = ['start_jid','end_jid'] + cols
        e_res = read_orc(spark,add_jg_path(file,standby=True))
        if e_res and e_res.take(1):
            e_faker = e_res.join(e,['start_jid','end_jid'],'inner').selectExpr(*cols)
            if e_faker.take(1):
                write_orc(e_faker,add_save_jg_path(file))
    for file, cols in vertex_table_info.items():
        cols = ['jid'] + cols
        v_res = read_orc(spark,add_jg_path(file,standby=True))
        if v_res and v_res.take(1):
            v_faker = v_res.join(v,'jid','inner').selectExpr(*cols)
            if v_faker.take(1):
                write_orc(v_faker,add_save_jg_path(file))
    e.unpersist()
    v.unpersist()


def get_network_graph_detail():
    '''电话通话明细'''
    # cols = ["desens_zjhm(start_phone) start_phone", "desens_zjhm(end_phone) end_phone", "start_time", "end_time", "call_duration",
    #               "homearea", "relatehomeac", "start_lac", "start_lic", "start_lon", "start_lat", "end_lac", "end_lic",
    #               "end_lon", "end_lat", ]
    #
    # real_call_detail = read_orc(spark,add_incr_path('edge_groupcall_detail'))
    # faker_call_detail_tmp = real_call_detail.selectExpr(cols)
    # faker_call = read_orc(spark,add_save_jg_path('edge_groupcall')).selectExpr('start_phone','end_phone')
    # ex_cols = ["start_phone", "end_phone", "start_time", "end_time", "call_duration",
    #               "homearea", "relatehomeac", "start_lac", "start_lic", "start_lon", "start_lat", "end_lac", "end_lic",
    #               "end_lon", "end_lat", ]
    # faker_call_detail = faker_call_detail_tmp.join(faker_call,['start_phone','end_phone'],'inner').selectExpr(*ex_cols)
    # write_orc(faker_call_detail,add_save_path('edge_groupcall_detail',root='faker_graph_data'))
    #
    #
    # msg_cols = ["desens_zjhm(start_phone) start_phone", "desens_zjhm(end_phone) end_phone", "start_time", "end_time", "send_time", "message",
    #               "homearea", "start_lac", "start_lic", "start_lon", "start_lat", "end_lac", "end_lic",
    #               "end_lon", "end_lat", "relatehomeac"]
    # real_msg_detail = read_orc(spark,add_incr_path('edge_groupmsg_detail'))
    # faker_msg_detail_tmp = real_msg_detail.selectExpr(msg_cols)
    # faker_msg = read_orc(spark,add_save_jg_path('edge_groupmsg')).selectExpr('start_phone','end_phone')
    # ex_msg_cols = ["start_phone", "end_phone", "start_time", "end_time", "send_time", "message",
    #               "homearea", "start_lac", "start_lic", "start_lon", "start_lat", "end_lac", "end_lic",
    #               "end_lon", "end_lat", "relatehomeac"]
    # faker_msg_detail = faker_msg_detail_tmp.join(faker_msg,['start_phone','end_phone'],'inner').selectExpr(*ex_msg_cols)
    # write_orc(faker_msg_detail,add_save_path('edge_groupmsg_detail',root='faker_graph_data'))

    hotel_cols = ["desens_zjhm(sfzh) sfzh", "lgdm", "zwmc", "start_time", "end_time"]
    real_hotel_detail = read_orc(spark,add_save_path('edge_person_stay_hotel_detail',root='relation_theme_extenddir'))
    faker_hotel_detail_tmp = real_hotel_detail.selectExpr(hotel_cols)
    faker_hotel = read_orc(spark,add_save_jg_path('edge_person_stay_hotel')).selectExpr('sfzh','lgdm')
    ex_hotel_cols = ["sfzh", "lgdm", "zwmc", "start_time", "end_time"]
    faker_hotel_detail = faker_hotel_detail_tmp.join(faker_hotel,['sfzh','lgdm'],'inner').selectExpr(*ex_hotel_cols)
    write_orc(faker_hotel_detail,add_save_path('edge_person_stay_hotel_detail',root='faker_graph_data'))


if __name__ == "__main__":
    logger.info('========================start time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

    get_network_graph_detail()

    logger.info('========================end time:%s==========================' % (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))