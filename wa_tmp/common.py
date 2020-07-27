#encoding=utf-8
'''
使用必读:
1）此文件是公用udf模块，当前文件定义的函数名若符合'udf_...._int'或'udf_...._string'格式
   都会被注册成udf函数，否则将被视为普通函数，不注册为udf函数
2）_int结尾表明函数返回类型为整形 _string结尾则表明函数返回类型为字符串
3）注册udf的时候自动以去掉首尾的udf_、_int、_string的命名注册为udf函数。例如：
   函数udf_format_data_string真实注册udf的过程为：
   sqlContenxt.registerFunction('format_data', udf_format_data_string, StringType())
   format_data=udf(udf_format_data_string, StringType())
4）在执行脚本生成sqlContenxt和spark对象后，再添加
from common import *
commonUtil = CommonUdf(sqlContenxt)
两行代码
5）真实使用udf过程中，sql语句中可以直接使用注册的udf函数，而在非sql语句中则需要使
   用'commonUtil.'作为前缀调用。例如：
   a) spark.sql('select format_data(SFZH) as SFZH from table_name')
   b) df = df.select(commonUtil.format_data(df.SFZH).alias('SFZH'))
6）使用时，请在spark-submit 添加 --py-files <当前文件名> 选项
'''

import sys
import copy, re, math
import types
import json
import time
from datetime import datetime, timedelta
from pyspark.sql import functions as fun
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, LongType
reload(sys)
sys.setdefaultencoding('utf-8')


#通用函数  option='overwrite | into '
def write_hive_table(spark, df, tabelname, partition=None, option='overwrite'):
    df.createOrReplaceTempView('tmp')
    sql = ''
    if not partition:
        sql = '''insert %s table bbd.%s select * from  tmp''' % (option, tabelname)
    if isinstance(partition,str):
        first_Partition = partition[0]
        sql = '''insert %s table bbd.%s partition(cp=%s) select * from  tmp''' % (option, tabelname, first_Partition)
    if isinstance(partition,list):
        first_Partition = partition[0]
        second_Partition = partition[1]
        sql = '''insert %s table bbd.%s partition(cp_month=%s,cp=%s) select * from  tmp''' % (
        option, tabelname, first_Partition, second_Partition)

    spark.sql(sql)

def pre_partition(cp, days=-1):
   '''
   取当分区的前days天分区,默认取前一天
   '''
   return (datetime.strptime(str(cp),'%Y%m%d00')+ timedelta(days=days)).strftime('%Y%m%d00')

#########################################################################
##                    以上是普通的通用函数                             ##
##--------------------------分割线-------------------------------------##
##                       以下是UDF函数                                 ##
#########################################################################




local_info = locals()
class CommonUdf:
    def __init__(self, spark):
        for skey in local_info:
            if skey.startswith('udf_') and isinstance(local_info[skey], types.FunctionType):
                if skey.endswith('int'):
                    spark.udf.register(skey[4:-4], local_info[skey], IntegerType())
                    self.__dict__[skey[4:-4]] = fun.udf(local_info[skey], IntegerType())
                elif skey.endswith('string'):
                    spark.udf.register(skey[4:-7], local_info[skey], StringType())
                    self.__dict__[skey[4:-7]] = fun.udf(local_info[skey], StringType())

def udf_format_data_string(data):
    '''检格式化数据->去掉所有空格'''
    try:
        if not data or not data.strip():
            return ''

        data = data.replace('\r','').replace('\n','').replace('\t', '').replace(' ','').replace(u'︻','').replace(u'　','').replace('\\d','').strip()
        return data
    except:
        #traceback.print_exc()
        return ''

def udf_format_data_special_string(data):
    '''检格式化数据 不去掉中间空格'''
    if not data or not data.strip():
        return ''
    try:
        data = data.replace('\r','').replace('\n','').replace('\t', '').replace(u'︻','').upper().strip()
        return data
    except:
        return ''


def udf_format_timestamp_string(data):
    '''检格式化数据->去掉所有空格'''
    try:
        if not data or not data.strip():
            return '0'

        data = data.replace(u'(', '').replace('\n', '').replace('\t', '').replace(' ', '').replace(u')','').strip()
        if len(data) == 10 and data.isdigit():
            return data
        return '0'

    except:
        # traceback.print_exc()
        return '0'

def udf_format_zjhm_string(data):
    '''格式化证件号码，如身份证，驾驶证等其他证件号码'''
    if not data or not data.strip():
        return ''
    try:
        data = data.replace('\r','').replace('\n','').replace('\t', '').replace(' ','').replace('　','').upper().strip()
        return data
    except:
        return ''

def udf_verify_zjhm_int(zjhh):
    '''校验普通的证件 '''
    try:
        zjhh = zjhh.strip().upper()
        for ch in zjhh:
            if u'\u4e00' <= ch <=u'\u9fa5':
                return 0
        if 5<= len(zjhh) <= 25:
            return 1
        return 0
    except:
        return 0

value_list = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
ret_list = '10X98765432'
def udf_verify_sfz_int(sfz):
    '''
    身份证号校验  需要跟用户确认 -> 错误身份证是否过滤掉
    return:
        0 校验不合法
        1 校验合法
    '''
    try:
        sfz = sfz.strip().upper()
        if len(sfz) == 18:
            start_2 = int(sfz[:2])
            birthday = sfz[6:14]
            #北京从11开始 国外91结束,限定出生年是在1900年以后的
            if start_2 > 10 and int(birthday[:4]) > 1900:
                #7-14为日期格式
                datetime.strptime(birthday, '%Y%m%d')
                sum = 0
                for i in range(17):
                    sum += int(sfz[i]) * value_list[i]
                if ret_list[sum % 11] == sfz[17]:
                    return 1
    except:
        return 0
    return 0

def udf_format_phone_string(data):
    if not data or not data.strip():
        return ''
    try:
        data = data.replace('+','').replace('-','').replace('-','').replace('\r','').replace('\n','').replace('\t', '').replace(' ','').replace('　','').replace('#','').replace('*','').replace('|','').replace('z','').strip()
        data = data.replace('+86','')
        return data
    except:
        return ''

def udf_verify_simple_phonenumber_int(phonenumber):
    try:
        phonenumber = udf_format_phone_string(phonenumber)
        if phonenumber.isdigit and len(phonenumber) >= 7:
            return 1
    except:
        return 0


def udf_verify_phonenumber_int(phonenumber):
    '''
    电话号码校验
    return:
        0 校验不合法
        1 校验合法
    '''
    try:
        phonenumber = udf_format_phone_string(phonenumber)
        if not phonenumber:
            return 0

        length = len(phonenumber)
        #增加区号4位+8位座机号的电话
        if length == 7 and not phonenumber.startswith('0'):
            return 0
        #todo 增加过滤带区号的服务号如 03511008611 35110086  等等
        if length >= 7 and (length <=11 or (length==12 and phonenumber.startswith('0')))\
           and not phonenumber.startswith('00') \
           and not phonenumber.startswith('106') and not phonenumber.startswith('100') \
           and not phonenumber.startswith('101') and not phonenumber.startswith('123') \
           and not phonenumber.startswith('0351100') and not phonenumber.startswith('351100') \
           and not phonenumber.startswith('400') and phonenumber.isdigit() :
            return 1
    except:
        pass
    return 0

def udf_verify_cell_phone_int(phonenumber):
    '''
    手机号校验
    return:
        0 校验不合法
        1 校验合法
    '''
    try:
        phonenumber = udf_format_phone_string(phonenumber)
        if not phonenumber:
            return 0
        if len(phonenumber)==11 and phonenumber.startswith('1')\
           and not phonenumber.startswith('106') and not phonenumber.startswith('100') \
           and not phonenumber.startswith('0351100') and not phonenumber.startswith('351100') \
           and phonenumber.isdigit():
            return 1
    except:
        pass
    return 0

def udf_timestamp2str_string(timestamp,default=''):
    '''时间戳转字符串时间格式'''
    try:
        timestamp = int(timestamp)
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
    except:
        pass
    return default

def udf_verify_man_int(sfzh):
    '''根据身份证判断是否男 1 是 0 否'''
    try:
        return 1 if int(sfzh[-2])%2==1 else 0
    except:
        pass
    return 0

def udf_format_xb_string(sfzh,default=''):
    '''根据身份证返回性别(在身份证校验之后的情况下不需要再校验) 1:男 ,2:女,4:未说明(治安标准)'''
    try:
        return u'男' if int(sfzh[-2])%2==1 else u'女'
    except:
        pass
    return ''

def udf_format_csrq_from_sfzh_string(sfzh):
    ''' 根据身份证返回出生日期     '''
    try:
        if len(sfzh.strip())==18:
                return sfzh.strip()[6:14]
        else:
            return ''
    except:
        pass
    return ''

def udf_format_start_sfzhs_int(sfzh1,sfzh2):
    ''' 根据身份证返回出生日期     '''
    start_time = None
    try:
        min_csrq = sfzh1.strip()[6:14]
        max_csrq = sfzh2.strip()[6:14]
        start_time = min_csrq if min_csrq > max_csrq else max_csrq
        ret = udf_str2timestamp_string(datetime.strptime(start_time, '%Y%m%d').strftime('%Y-%m-%d %H:%M:%S'))
        return int(ret)
    except:
        pass
    return 0

def udf_format_starttime_from_sfzh_int(sfzh):
    ''' 根据身份证返回出生日期 返回时间戳    '''
    try:
        sfzh = sfzh.strip()
        csrq = sfzh[6:14]
        ret = udf_str2timestamp_string(datetime.strptime(csrq, '%Y%m%d').strftime('%Y-%m-%d %H:%M:%S'))
        return int(ret)
    except:
        pass
    return 0

def udf_format_csrq_string(sfzh,cssf='',default=''):
    ''' 根据身份证返回出生日期 和出生时分秒返回出生时间    '''
    sfzh = sfzh.strip()
    cssf = cssf.strip()
    if len(cssf)==6:
        return  '%s-%s-%s %s:%s:%s'%(sfzh[6:10],sfzh[10:12],sfzh[12:14],cssf[0:2],cssf[2:4],cssf[4:6])
    else:
        return '%s-%s-%s 00:00:00'%(sfzh[6:10],sfzh[10:12],sfzh[12:14])


emailRegex = '([a-zA-Z0-9]{1}[a-zA-Z0-9\.\+\-_]{0,63}@{1}[a-zA-Z0-9\-_]+(\.[a-zA-Z0-9\-_]+){1,4})'
def udf_format_email_string(data):
    '''获取邮箱 返回list >未注册函数'''
    try:
        data = data.strip()
        email = re.findall(emailRegex,data)
        if email:
            res = email[0][0]
            except_email = ['no-reply@','noreply@','service@','support@','info@','Postmaster@',
                            'customer_service','admin@','VOICE=','notification','10086@','10000sx@','10000cq@']
            for item in except_email:
                if res.startswith(item):
                    return ''
            return res
    except:
        pass
    return ''

def udf_verify_email_int(data):
    '''检验邮箱 '''
    try:
        data = data.strip()
        emails = re.findall(emailRegex, data)
        except_email = ['no-reply@','noreply@','service@','support@','info@','Postmaster@',
                            'customer_service','admin@','VOICE=','notification','10086@','10000sx@','10000cq@']
        for email in emails:
            bValid = True
            for item in except_email:
                if email[0].startswith(item):
                   bValid = False
            if bValid:
                return 1
    except:
        pass
    return 0

def udf_deal_swrq_string(swrq):
    ''' 返回死亡日期  '''
    try:
        tmp = udf_valid_datetime2_string(swrq)
        if tmp:
             return tmp[:10].replace('-','')
    except:
        return ''
    return ''
 ##__del__
def udf_deal_swrq_old_string(swrq,default=''):
    ''' 返回死亡日期  '''
    try:
        swrq = time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(swrq.strip(),'%Y%m%d'))
        return swrq
    except:
        return default

def udf_coalesce_str_string(*cols):
    '''
    从左至右取第一个不为空值的字符串，支持变参
    '''
    for col in cols:
        if col and col.strip():
            return col
    return ''

def udf_valid_vehicle_id_string(sID):
    '''
    取有效机车号
    '''
    try:
        if not sID:
            return ''

        sID = sID.strip().replace("-", "").upper()[:17]
        if len(sID) < 17:
            return ''

        if not re.match('[A-Z0-9]{17}', sID):
            return ''
    except:
        return ''

    return sID

def udf_verify_hphm_int(hphm):
    '''
    验证车牌号是否合法
    '''
    try:
        if not hphm :
            return 0

        hphm = hphm.strip().replace("-", "").upper()

        if re.match(u'[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼使领]{0,1}[A-Z]{1}[A-Z0-9]{4}[A-Z0-9挂学警港澳]{1}', hphm):
            return 1

    except:
        pass

    return 0

def udf_valid_hphm_string(hphm, hpzl=None):
    '''
    取有效车牌号，根据号牌类型摩托车号码后添加'_MT'
    '''
    if not hphm or not hphm.strip():
        return ''

    hphm = hphm.strip().upper()
    if udf_verify_hphm_int(hphm):
        #摩托车加上摩托车标志
        if hpzl and hpzl in ['07', '08', '09', '10', '11', '12', '17', '19', '21', '24']:
            hphm = hphm+'_MT'

        if hphm[0] not in u'京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼使领':
            return u'晋'+hphm
        else:
            return hphm
    else:
        return ''

def udf_verify_school_int(school_name):
    '''
    学校验证：抽取包含"学、院、所、研究、幼儿园、小、初、中、高、专、职、大、"字符的学校
    '''
    if u'班' in school_name:
        return 0

    for sWord in [u'幼儿园', u'幼儿班', u'托儿所', u'小学',u'村校', u'村小', u'初小', u'附小',
                      u'中学', u'学校', u'初中',u'初级中学', u'中心校',
                      u'高中',u'高级中学', u'一中', u'二中', u'三中', u'四中',
                      u'职业教育', u'农校', u'大学', u'职校',u'技校', u'学院']:
        if sWord in school_name:
            return 1

    return 0

def udf_verify_college_int(college_name):
    '''
    学院验证
    '''
    for sWord in [u'院', u'系']:
        if sWord in college_name:
            return 1

    return 0

def udf_date2timestampstr_string(col):
    return str(udf_valid_datetime_int(col))


def udf_multidate2timestamp_int(col):
    try:
        if not col or not col.strip():
            return 0
        if len(col) == 10:
            return int(col)
        else:
            return udf_valid_datetime_int(col)
    except:
        return 0


def udf_valid_datetime_int(col):
    '''
    时间标准化 准换成时间戳的形式
    '''
    if not col or not col.strip():
        return 0

    if col.count('.') == 1:
        tmp_value = col[:col.index('.')]
    else:
        tmp_value = col

    tmp_value = tmp_value.strip().replace('/', '-').replace('.', '-')

    ret = None
    try:
        if '-' in tmp_value and ':' not in tmp_value :
            info = tmp_value.split()
            if len(info) > 1:
                tmp_value = ' '.join(info)
                if len(info[1]) == 2:
                    ret = datetime.strptime(tmp_value,'%Y-%m-%d %H')
                elif len(info[1]) == 4:
                    ret = datetime.strptime(tmp_value,'%Y-%m-%d %H%M')
                elif len(info[1]) == 6:
                    ret = datetime.strptime(tmp_value,'%Y-%m-%d %H%M%S')
            else:
                ret = datetime.strptime(tmp_value,'%Y-%m-%d')
        elif '-' in tmp_value and ':' in tmp_value:
            if tmp_value.count(':') == 1:
                ret = datetime.strptime(tmp_value,'%Y-%m-%d %H:%M')
            elif tmp_value.count(':') == 2:
                ret = datetime.strptime(tmp_value,'%Y-%m-%d %H:%M:%S')
        else:
            int(tmp_value)
            if len(tmp_value) == 8:
                ret = datetime.strptime(tmp_value,'%Y%m%d')
            elif len(tmp_value) == 10:
                ret = datetime.strptime(tmp_value,'%Y%m%d%H')
            elif len(tmp_value) == 12:
                ret = datetime.strptime(tmp_value,'%Y%m%d%H%M')
            elif len(tmp_value) == 14:
                ret = datetime.strptime(tmp_value,'%Y%m%d%H%M%S')

        if ret:
            return udf_str2timestamp_string(ret.strftime('%Y-%m-%d %H:%M:%S'))
    except:
        pass

    return 0

def udf_str2timestamp_string(data):
    '''字符串转时间搓'''
    try:
        res = str(time.mktime(time.strptime(data, '%Y-%m-%d %H:%M:%S')))
        return int(res[:res.index('.')])
    except:
        return 0


def udf_valid_datetime2_string(col):
    '''
    时间标准化 字符串的形式
    '''
    if not col or not col.strip():
        return ''

    if col.count('.') == 1:
        tmp_value = col[:col.index('.')]
    else:
        tmp_value = col

    tmp_value = tmp_value.strip().replace('/', '-').replace('.', '-')

    ret = None
    try:
        if '-' in tmp_value and ':' not in tmp_value :
            info = tmp_value.split()
            if len(info) > 1:
                tmp_value = ' '.join(info)
                if len(info[1]) == 2:
                    ret = datetime.strptime(tmp_value,'%Y-%m-%d %H')
                elif len(info[1]) == 4:
                    ret = datetime.strptime(tmp_value,'%Y-%m-%d %H%M')
                elif len(info[1]) == 6:
                    ret = datetime.strptime(tmp_value,'%Y-%m-%d %H%M%S')
            else:
                ret = datetime.strptime(tmp_value,'%Y-%m-%d')
        elif '-' in tmp_value and ':' in tmp_value:
            if tmp_value.count(':') == 1:
                ret = datetime.strptime(tmp_value,'%Y-%m-%d %H:%M')
            elif tmp_value.count(':') == 2:
                ret = datetime.strptime(tmp_value,'%Y-%m-%d %H:%M:%S')
        else:
            int(tmp_value)
            if len(tmp_value) == 8:
                ret = datetime.strptime(tmp_value,'%Y%m%d')
            elif len(tmp_value) == 10:
                ret = datetime.strptime(tmp_value,'%Y%m%d%H')
            elif len(tmp_value) == 12:
                ret = datetime.strptime(tmp_value,'%Y%m%d%H%M')
            elif len(tmp_value) == 14:
                ret = datetime.strptime(tmp_value,'%Y%m%d%H%M%S')

        if ret:
            return ret.strftime('%Y-%m-%d %H:%M:%S')
    except:
        pass

    return ''

def udf_full_phonenum_string(phonenum, area='023'):
    '''
    获取完整的11位座机号
        phonenum:电话号码
        area:区号
    '''
    if not phonenum:
        return phonenum

    phonenum = udf_format_phone_string(phonenum)
    if phonenum.startswith('0'):
        return phonenum

    if not area or not area.strip():
        area = '023'

    area = area.strip()
    if not area.startswith('0') and (len(area) == 3 or len(area) == 2):
        area = '0'+area

    if len(phonenum) == 7 or len(phonenum) == 8:
        return area+phonenum

    return phonenum

def udf_verify_company_name_int(company_name, type=0):
    '''
    公司名校验
    '''
    try:
        if not company_name or len(company_name.strip()) < 5:
            return 0

        if type:
            for sKeyWord in [u'人力', u'人才', u'劳务', u'劳动',  u'中介', u'企业咨询', u'企业管理']:
                if sKeyWord in company_name:
                    return 0

        for sKeyWord in [u'股份', u'公司', u'供销所', u'服务所',u'重庆市']:
            if sKeyWord in company_name:
                return 1

        for sKeyWord in [u'厂', u'场', u'集团', u'培训机构', u'站', u'经营部', u'总公司', u'事务所', u'公安局', u'派出所',u'管理局',u'管理处',u'检察院',u'人民法院',u'统计局']:
            if company_name.strip().endswith(sKeyWord):
                return 1
    except:
        pass

    return 0

def udf_verify_person_name_int(person_name,type=0):
    '''
    人名校验
    '''
    try:
        if not person_name or len(person_name.strip()) > 5:
            return 0

        if type:
            for sKeyWord in [u'股份', u'公司', u'有限', u'管理',u'重庆市']:
                if sKeyWord in person_name:
                    return 0
    except:
        pass

    return 1
def udf_verify_timestamp_int(timestamp):
    '''时间戳校验'''
    try:
        timestamp = str(timestamp).strip()
        if time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(timestamp))) :
            return 1

    except:
        pass
    return 0

def udf_verify_im_account_int(data):
    '''im账号校验'''
    try:
        if not data or not data.strip():
            return 0
        data = data.strip()
        if  len(data) >= 5 and data.replace('UID=','',1).isdigit() :
            return 1
    except:
        return 0
    return 0

def udf_format_im_account_string(data):
    '''im账号格式化验'''
    try:
        if not data or not data.strip():
            return ''
        return  data.replace('UID=','').strip()
    except:
        pass
    return ''

def udf_format_zjcx_string(jsz):
    '''校验准驾车型 错误返回默认值'''
    ZJCX_TYPE = ['A','B','C','Z','D','E','F','J','L','M','N','P','G','H','K']
    try:
        jsz = jsz.strip().upper()
        if jsz and jsz[0] in ZJCX_TYPE:
            return jsz
    except:
       pass
    return ''

def udf_verify_hukouhao_int(data):
    '''户口号校验'''
    try:
        if not data or not data.strip():
            return 0
        data = data.strip()
        if  len(data) ==9 and data.isdigit():
            return 1
    except:
        return 0
    return 0

def udf_verify_common_daima_int(data):
    '''通用的代码校验  如酒店、网吧代码'''
    try:
        if not data or not data.strip():
            return 0
        data = data.strip()
        if  len(data) >= 5 and data.isdigit() :
            return 1
    except:
        return 0
    return 0


def udf_verify_coachline_int(data):
    '''校验汽车路线'''
    try:
        if not data or not data.strip():
            return 0
        data = data.strip()
        if  len(data) >= 2 and data.isdigit() :
            return 1
    except:
        return 0
    return 0

def udf_timestamp2cp_int(timestamp,format="%Y%m%d00"):
    stamp = int(timestamp)
    d = datetime.fromtimestamp(stamp)
    return int(d.strftime(format))

def udf_timestamp2month_int(timestamp,format="%Y%m0000"):
    try:
        stamp = int(timestamp)
        d = datetime.fromtimestamp(stamp)
        return int(d.strftime(format))
    except:
        return ''


def udf_verify_vbn_int(data):
    '''校验宽带账号
    长度大于等于5 且除了+-符号全是数据或者字母 或者数字字母组合
    '''
    try:
        data = data.replace('+','').replace('-','').strip()
        if not data:
            return 0
        if  len(data) >= 5 and data.isalnum():
            return 1
    except:
        return 0
    return 0

def udf_format_wechat_string(data):
    try:
        data = data.replace('+','').replace('-','').replace('=','').replace('\\','').replace('\t','').strip()
        return data
    except:
        return ""

def udf_verify_wechat_int(data):
    '''校验宽带账号
    长度大于等于5 且除了+-符号全是数据或者字母 或者数字字母组合
    '''
    try:
        format = '^[0-9a-zA-Z_]{1,}$'
        data = udf_format_wechat_string(data)
        if re.compile(format).search(data):
            return 1
    except:
        return 0
    return 0

fwdzRegex = '(\d{0,3}\-\d{0,4})'
def udf_verify_fzdw_mph_int(data):
    '''房屋地址是否包含门牌号 '''
    try:
        data = data.strip()
        mph = re.findall(fwdzRegex,data)
        if mph:
            return 1
    except:
        pass
    return 0

def udf_format_lineID_string(cc,fcrq,yjqfrq=''):
    '''获取车次ID'''
    try:
        if not cc.strip() and not fcrq.strip() and not yjqfrq.strip():
            return ''
        cc = cc.strip().upper().replace(' ','')
        tmp = udf_valid_datetime2_string(fcrq)
        if not tmp:
            tmp = udf_valid_datetime2_string(yjqfrq)
        if not tmp:
            return ''
        date_str= tmp.strip()[:10].replace('-','')
        return cc+date_str
    except:
        return ''

def udf_diff_weeks_int(start_date, end_date):
    '''
    计算两个日期之间间隔了几周
    :param start_date: start week sunday format '2020030800'
    :param end_date: end week sunday
    :return:  Interval weeks
    '''
    format = '%Y%m%d00'
    try:
        start = datetime.strptime(str(start_date),format)
        end = datetime.strptime(str(end_date),format)
        diff_days = (end - start).days
        return int(diff_days/7) + 1
    except:
        pass
    return 0

complies = {'MY':'^(\+?0*60)\d{8,11}',
            'PH':'^(\+?0*63)\d{8,11}',
            'SG':'^(\+?0*65)\d{8,11}',
            'TH':'^(\+?0*66)\d{8,11}',
            'VN':'^(\+?0*84)\d{8,11}',
            'HK':'^(\+?0*852)\d{8,11}',
            'MO':'^(\+?0*853)\d{8,11}',
            'KH':'^(\+?0*855)\d{8,11}',
            'LA':'^(\+?0*856)\d{8,11}',
            'MM':'^(\+?0*95)\d{8,11}'}

countries = {'MY':u'马来西亚',
            'PH':u'菲律宾',
            'SG':u'新加坡',
            'TH':u'泰国',
            'VN':u'越南',
            'HK':u'香港',
            'MO':u'澳门',
            'KH':u'柬埔寨',
            'LA':u'老挝',
            'MM':u'缅甸',}

def udf_verify_oversea_phone_string(phone):
    '''判断是否东南亚电话，并返回国家区域'''
    try:
        for k,v in complies.items():
            if re.compile(v).search(phone):
                return countries.get(k)
        return ''
    except:
        return ''


def udf_timestamp2hourtype_int(timestamp):
    try:
        timestamp = int(timestamp)
        d = datetime.fromtimestamp(timestamp)
        return int(d.strftime('%H'))
    except:
        pass


def udf_format_lgdm_string(data):
    '''检格式化数据->去掉所有空格'''
    try:
        if not data or not data.strip():
            return ''
        data = data.replace(' ','').replace('\t','').strip()
        return data[:10]
    except:
        # traceback.print_exc()
        return ''

def udf_find_ids_dup_int(data1,data2):
    '''
    判断是否有相同id
    :param data1:
    :param data2:
    :return:
    '''
    try:
        data1 = set(data1)
        data2 = set(data2)
        return len(data1 & data2)
    except:
        return 0

def udf_list_length_int(data):
    try:
        data = set(data)
        return len(data)
    except:
        return 0

def col_filter(row, indices):
    cols = row.split('\t')
    if len(cols) < indices[-1] + 1:
        return
    tmp = [cols[_] for _ in indices]
    return tmp

def add_path(tablename, import_time='*',is_relate=False):
    jz_file = ['dw_evt','voic_busi','phone_owner','roam_busi','ods_tb_cdr','vpmn_busi','dw_sms','elefen_calrec']
    relate_update_files = ['res_his_dcveh','res_his_accmobile','res_his_tr_accacc','res_his_payowner','per_his_partnerorder','res_his_truiduid']
    relate_day_files = ['add_day_dcnetbar']
    relate_path_total = '/phoebus/_fileservice/users/zhjw/sjzl/daml/data/cdm/dws/his/{}/final/total/{}/*' #关系主题库地址
    relate_path_update = '/phoebus/_fileservice/users/zhjw/sjzl/daml/data/cdm/dws/his/{}/final/update/{}/*' #关系主题库地址
    relate_path_day = '/phoebus/_fileservice/users/zhjw/sjzl/daml/data/cdm/dws/day/{}/final/total/{}/*'
    sjzl_path = '/phoebus/_fileservice/users/zhjw/sjzl/daml/data/ods/{}/import/{}/*'
    jz_path = '/phoebus/_fileservice/users/jz/jizhen/daml/data/ods/{}/import/{}/*'
    if is_relate:
        return relate_path_total.format(tablename.upper(),import_time)
    for file in jz_file:
        if file in tablename:
            return jz_path.format(tablename.upper(), import_time)
    for file in relate_update_files:
        if file in tablename:
            return relate_path_update.format(tablename.upper(), import_time)
    for file in relate_day_files:
        if file in tablename:
            return relate_path_day.format(tablename.upper(), import_time)
    return sjzl_path.format(tablename.upper(), import_time)

def add_save_path(tablename, cp='',root='extenddir'):
    '''
    保存hive外表文件，分区默认为cp=2020，root为extenddir
    :param tablename:
    :param cp:
    :return:
    '''
    if cp:
        tmp_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/{}/{}/cp={}'
        return tmp_path.format(root,tablename.lower(), cp)
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/{}/{}/cp=2020'.format(root,tablename.lower())

def add_incr_path(tablename,cp=''):
    '''
    保存每天的增量文件，读取一般为所有
    :param tablename:
    :param cp:
    :return:
    '''
    if cp:
        tmp_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/incrdir/{}/cp={}'
        return tmp_path.format(tablename.lower(), cp)
    return '/phoebus/_fileservice/users/slmp/shulianmingpin/incrdir/{}/*'.format(tablename.lower())

def create_tmpview_table(spark,tablename,cp='',root='extenddir'):
    '''
    读取所有或分区内容，默认为hive外表文件夹
    :param tablename:
    :param cp: cp='' 读取所有内容
    :return:
    '''
    tmp_path = '/phoebus/_fileservice/users/slmp/shulianmingpin/{}/{}/{}'
    if not cp:
        df = spark.read.orc(tmp_path.format(root,tablename,'*'))
    else:
        df = spark.read.orc(tmp_path.format(root,tablename, cp))
    df.createOrReplaceTempView(tablename)

def create_uniondf(spark,table_list,tmp_sql):
    '''
    :param spark:
    :param table_list: 所需原始表及其字段对应信息
    :param tmp_sql: 原始sql语句，根据table_list进行格式化
    :return: table_List所有df的合集
    '''
    dfs = []
    for info in table_list:
        sql = tmp_sql.format(**info)
        init(spark, info['tablename'], if_write=False)
        df = spark.sql(sql)
        dfs.append(df)
    union_df = reduce(lambda x,y:x.unionAll(y),dfs)
    return union_df

def create_extend_uniondf(spark,table_list,tmp_sql,root='extenddir'):
    '''
    :param spark:
    :param table_list: 已存在表及其字段对应信息
    :param tmp_sql: 原始sql语句，根据table_list进行格式化
    :return: table_List所有df的合集
    '''
    dfs = []
    for info in table_list:
        sql = tmp_sql.format(**info)
        create_tmpview_table(spark, info['tablename'], root=root)
        df = spark.sql(sql)
        dfs.append(df)
    union_df = reduce(lambda x,y:x.unionAll(y),dfs)
    return union_df


def init(spark,tablename,import_time='',cond='',drop_col=None,if_write=True,is_relate=False):
    '''
    创建数据临时表
    :param spark:
    :param tablename: 源数据数据名
    :param import_time: 是否指定读取某个import_time文件夹
    :param cond: 预先筛选条件
    :param drop_col: 去重列
    :param if_write: 是否写入本地，默认写
    :param is_relate: 是否为关系主题库数据源，默认false
    :return: 创建该数据集的临时表，以tablename注册
    '''
    indices = table_indices[tablename]
    schema = table_schemas[tablename]
    filepath = add_path(tablename,is_relate=is_relate)
    if import_time:
        filepath = add_path(tablename,import_time=import_time,is_relate=is_relate)
    print(filepath,indices,schema)
    save_path = add_save_path(tablename)
    df = spark.sparkContext.textFile(filepath) \
        .map(lambda row: col_filter(row, indices)) \
        .filter(lambda row: row) \
        .toDF(schema)
    if cond:
        df = df.where(cond)
    if drop_col:
        df = df.dropDuplicates(drop_col)
    if if_write:
        df.write.format('orc').mode('overwrite').save(save_path)
    if not if_write:
        df.createOrReplaceTempView(tablename)
    print('init %s down'%tablename)

def init_history(spark,tablename,import_times,cond='',drop_col=None,if_write=True):
    '''
    获取分区数据很多的的历史数据
    :param spark:
    :param tablename: 原始表名小写
    :param import_times: 对应同一天的所有的导入时间戳列表[t1,t2,t3]
    :param cond: 筛选条件
    :param drop_col: 去重列
    :param if_write: 是否保存原始数据 默认保存
    :return:
    '''
    cp=str(udf_timestamp2cp_int(import_times[0],format='%Y%m%d00'))
    indices = table_indices[tablename]
    schema = table_schemas[tablename]
    save_path = add_save_path(tablename, cp=cp)

    dfs = []
    for import_time in import_times:
        filepath = add_path(tablename,import_time=import_time)
        print(filepath)
        try:
            df = spark.sparkContext.textFile(filepath) \
                .map(lambda row: col_filter(row, indices)) \
                .filter(lambda row: row) \
                .toDF(schema)
        except:
            continue
        if cond:
            df = df.where(cond)
        if drop_col:
            df = df.dropDuplicates(drop_col)
        dfs.append(df)
    res = reduce(lambda x,y:x.unionAll(y),dfs)
    if if_write:
        res.write.format('orc').mode('overwrite').save(save_path)
    if not if_write:
        res.createOrReplaceTempView(tablename)
    print('init %s %s down'%(tablename,cp))

def init_dw_history(spark,tablename,import_times,if_write=True):
    '''
    获取分区数据很多的的历史数据
    :param spark:
    :param tablename: 原始表名小写
    :param import_times: 对应同一天的所有的导入时间戳列表[t1,t2,t3]
    :param if_write: 是否保存原始数据 默认保存
    :return:
    '''
    _root_path = 'midfile/call_msg_tmp'
    def write_orc(df, path, mode='overwrite'):
        df.write.format('orc').mode(mode).save(path)
    def read_orc(path):
        df = spark.read.orc(path)
        return df
    def union_df(dfs):
        return reduce(lambda x,y:x.unionAll(y),dfs)

    cp=str(udf_timestamp2cp_int(import_times[0],format='%Y%m%d00'))
    indices = table_indices[tablename]
    schema = table_schemas[tablename]
    save_path = add_save_path(tablename, cp=cp)
    dfs = []
    i = 0
    for import_time in import_times:
        filepath = add_path(tablename,import_time=import_time)
        try:
            df = spark.sparkContext.textFile(filepath) \
                .map(lambda row: col_filter(row, indices)) \
                .filter(lambda row: row) \
                .toDF(schema)
            dfs.append(df)
            write_mode = 'overwrite' if i == 0 else 'append'
            if len(dfs) == 150:
                tmp = union_df(dfs)
                write_orc(tmp,add_save_path(tablename,cp=cp,root=_root_path),mode=write_mode)
                dfs = []
                i += 1
        except:
            continue
    write_orc(union_df(dfs), add_save_path(tablename, cp=cp, root=_root_path), mode='append')
    res = read_orc(add_save_path(tablename,cp=cp,root=_root_path))
    if if_write:
        res.write.format('orc').mode('overwrite').save(save_path)
    if not if_write:
        res.createOrReplaceTempView(tablename)
    print('init %s %s down'%(tablename,cp))

def get_all_dict(spark):
    ''' 所有的数据字典 '''
    return spark.read.csv('/phoebus/_fileservice/users/slmp/shulianmingpin/all_dict.csv',sep='\t',header=True)

if __name__ == '__main__':
    print(udf_format_zjhm_string('51231231'))
