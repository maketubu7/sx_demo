# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import requests, json,urllib,time
import threading,functools
import pandas as pd

##参数为qq 以及对应id
friend_root_url = 'http://15.6.46.234:8081/inet/query/beginadvancequery?analyseResourceIds=&analyseResourceNames=&appTypeNames=QQ&appTypes=1030001&caseDesc=&caseId=613612&caseName=20200511%E6%89%93%E5%87%BB%E7%BD%91%E7%BB%9C%E9%BB%91%E4%BA%A7%E8%B7%91%E5%88%86%E6%94%AF%E4%BB%98%E5%B9%B3%E5%8F%B0%E6%83%85%E6%8A%A5%E4%B8%93%E6%A1%88&caseTaskId=679274&caseTaskName=20200511%E6%89%93%E5%87%BB%E7%BD%91%E7%BB%9C%E9%BB%91%E4%BA%A7%E8%B7%91%E5%88%86%E6%94%AF%E4%BB%98%E5%B9%B3%E5%8F%B0%E6%83%85%E6%8A%A5%E4%B8%93%E6%A1%88&caseType=0&endTime=1591252708&endTimeStr=2020-06-04+14%3A38%3A28&isQueryCache=1&location=&locationStr=&memberId=&needDelivery=false&paramCode=accountname&paramName=QQ%E5%8F%B7&paramValue={}&priValue=false&remark=&secondParamCode=&secondParamCodeName=&secondParamName=&secondParamValue=&secondQuery=false&sourceCodes=10091063%2C10091012&sourceCodesStr=%E5%A5%BD%E5%8F%8B%E4%BF%A1%E6%81%AF%2C%E7%BE%A4%E8%81%8A&startTime=1588660709&startTimeStr=2020-05-05+14%3A38%3A29&taskId=0&userId=0'
friend_info_url = 'http://15.6.46.234:8081/inet/result/tablepage?opId={}&sourceCode=10091063&pagesize=3000&page=1&type=QUERY&appType=1030001'

##参数为qq 以及对应id
groupid_root_url = 'http://15.6.46.234:8081/inet/query/beginadvancequery?analyseResourceIds=&analyseResourceNames=&appTypeNames=QQ&appTypes=1030001&caseDesc=&caseId=613612&caseName=20200511%E6%89%93%E5%87%BB%E7%BD%91%E7%BB%9C%E9%BB%91%E4%BA%A7%E8%B7%91%E5%88%86%E6%94%AF%E4%BB%98%E5%B9%B3%E5%8F%B0%E6%83%85%E6%8A%A5%E4%B8%93%E6%A1%88&caseTaskId=679274&caseTaskName=20200511%E6%89%93%E5%87%BB%E7%BD%91%E7%BB%9C%E9%BB%91%E4%BA%A7%E8%B7%91%E5%88%86%E6%94%AF%E4%BB%98%E5%B9%B3%E5%8F%B0%E6%83%85%E6%8A%A5%E4%B8%93%E6%A1%88&caseType=0&endTime=1591326969&endTimeStr=2020-06-05+11%3A16%3A09&isQueryCache=1&location=&locationStr=&memberId=&needDelivery=false&paramCode=accountname&paramName=QQ%E5%8F%B7&paramValue={}&priValue=false&remark=&secondParamCode=&secondParamCodeName=&secondParamName=&secondParamValue=&secondQuery=false&sourceCodes=10091063%2C10091012&sourceCodesStr=%E5%A5%BD%E5%8F%8B%E4%BF%A1%E6%81%AF%2C%E7%BE%A4%E8%81%8A&startTime=1588734970&startTimeStr=2020-05-06+11%3A16%3A10&taskId=0&userId=0&_=1591327131419 HTTP/1.1'
groupid_info_url = 'http://15.6.46.234:8081/inet/result/groupchatpage?opId={}&startTimeStr=&endTimeStr=&keyWord=&keyAccount=&page=1&pagesize=3000&appType=1030001&type=QUERY&sort=desc&mediaType=00&_=1591328923424'

##参数为groupid 以及对应id
groupmember_root_url = 'http://15.6.46.234:8081/inet/query/beginadvancequery?analyseResourceIds=&analyseResourceNames=&appTypeNames=QQ&appTypes=1030001&caseDesc=&caseId=613612&caseName=20200511%E6%89%93%E5%87%BB%E7%BD%91%E7%BB%9C%E9%BB%91%E4%BA%A7%E8%B7%91%E5%88%86%E6%94%AF%E4%BB%98%E5%B9%B3%E5%8F%B0%E6%83%85%E6%8A%A5%E4%B8%93%E6%A1%88&caseTaskId=679274&caseTaskName=20200511%E6%89%93%E5%87%BB%E7%BD%91%E7%BB%9C%E9%BB%91%E4%BA%A7%E8%B7%91%E5%88%86%E6%94%AF%E4%BB%98%E5%B9%B3%E5%8F%B0%E6%83%85%E6%8A%A5%E4%B8%93%E6%A1%88&caseType=0&endTime=1591337834&endTimeStr=2020-06-05+14%3A17%3A14&isQueryCache=1&location=&locationStr=&memberId=&needDelivery=false&paramCode=groupid&paramName=%E7%BE%A4%E5%8F%B7&paramValue={}&priValue=false&remark=&secondParamCode=&secondParamCodeName=&secondParamName=&secondParamValue=&secondQuery=false&sourceCodes=10091068&sourceCodesStr=%E7%BE%A4%E6%88%90%E5%91%98&startTime=1588745835&startTimeStr=2020-05-06+14%3A17%3A15&taskId=0&userId=0&_=1591339136472'
groupmember_info_url = 'http://15.6.46.234:8081/inet/result/tablepage?opId={}&sourceCode=10091068&pagesize=3000&page=1&type=QUERY&appType=1030001&_=1591339151454'

##参数为wechat 以及对应id
wechat_friend_root_url = 'http://15.6.46.234:8081/inet/query/beginadvancequery?analyseResourceIds=&analyseResourceNames=&appTypeNames=%E5%BE%AE%E4%BF%A1&appTypes=1030036&caseDesc=&caseId=613612&caseName=20200511%E6%89%93%E5%87%BB%E7%BD%91%E7%BB%9C%E9%BB%91%E4%BA%A7%E8%B7%91%E5%88%86%E6%94%AF%E4%BB%98%E5%B9%B3%E5%8F%B0%E6%83%85%E6%8A%A5%E4%B8%93%E6%A1%88&caseTaskId=679274&caseTaskName=20200511%E6%89%93%E5%87%BB%E7%BD%91%E7%BB%9C%E9%BB%91%E4%BA%A7%E8%B7%91%E5%88%86%E6%94%AF%E4%BB%98%E5%B9%B3%E5%8F%B0%E6%83%85%E6%8A%A5%E4%B8%93%E6%A1%88&caseType=0&endTime=1591337834&endTimeStr=2020-06-05+14%3A17%3A14&isQueryCache=1&location=&locationStr=&memberId=&needDelivery=false&paramCode=accountname&paramName=%E5%BE%AE%E4%BF%A1%E5%8F%B7&paramValue={}&priValue=false&remark=&secondParamCode=&secondParamCodeName=&secondParamName=&secondParamValue=&secondQuery=false&sourceCodes=10091063%2C10091012&sourceCodesStr=%E5%A5%BD%E5%8F%8B%E4%BF%A1%E6%81%AF%2C%E7%BE%A4%E8%81%8A&startTime=1588745835&startTimeStr=2020-05-06+14%3A17%3A15&taskId=0&userId=0&_=1591342166736'
wechat_friend_info_url = 'http://15.6.46.234:8081/inet/result/tablepage?opId={}&sourceCode=10091063&pagesize=10&page=3000&type=QUERY&appType=1030036&_=1591342342037'

##参数为wechat 以及对应id
wechat_group_root_url = 'http://15.6.46.234:8081/inet/query/beginadvancequery?analyseResourceIds=&analyseResourceNames=&appTypeNames=%E5%BE%AE%E4%BF%A1&appTypes=1030036&caseDesc=&caseId=613612&caseName=20200511%E6%89%93%E5%87%BB%E7%BD%91%E7%BB%9C%E9%BB%91%E4%BA%A7%E8%B7%91%E5%88%86%E6%94%AF%E4%BB%98%E5%B9%B3%E5%8F%B0%E6%83%85%E6%8A%A5%E4%B8%93%E6%A1%88&caseTaskId=679274&caseTaskName=20200511%E6%89%93%E5%87%BB%E7%BD%91%E7%BB%9C%E9%BB%91%E4%BA%A7%E8%B7%91%E5%88%86%E6%94%AF%E4%BB%98%E5%B9%B3%E5%8F%B0%E6%83%85%E6%8A%A5%E4%B8%93%E6%A1%88&caseType=0&endTime=1591347932&endTimeStr=2020-06-05+17%3A05%3A32&isQueryCache=1&location=&locationStr=&memberId=&needDelivery=false&paramCode=accountname&paramName=%E5%BE%AE%E4%BF%A1%E5%8F%B7&paramValue={}&priValue=false&remark=&secondParamCode=&secondParamCodeName=&secondParamName=&secondParamValue=&secondQuery=false&sourceCodes=10091063%2C10091012&sourceCodesStr=%E5%A5%BD%E5%8F%8B%E4%BF%A1%E6%81%AF%2C%E7%BE%A4%E8%81%8A&startTime=1588755933&startTimeStr=2020-05-06+17%3A05%3A33&taskId=0&userId=0&_=1591348467217'
wechat_group_info_url = 'http://15.6.46.234:8081/inet/result/groupchatpage?opId={}&startTimeStr=&endTimeStr=&keyWord=&keyAccount=&page=1&pagesize=3000&appType=1030036&type=QUERY&sort=desc&mediaType=00&_=1591348270275'


cookie = None
root_header = {
        'Host': '15.6.46.234:8081',
        'Proxy-Connection': 'keep-alive',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36 FH/1.1.0.0 (553A42F6E74BAAE2)(53C9BE9B6308AABF)',
        'Referer': 'http://15.6.46.234:8081/inet/query/loadingpage',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cookie': cookie
        }


info_header = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding':'gzip, deflate, sdch',
        'Accept-Language':'zh-CN,zh;q=0.8',
        'Cache-Control':'max-age=0',
        'Cookie':cookie,
        'Host':'15.6.46.234:8081',
        'Proxy-Connection':'keep-alive',
        'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36 FH/1.1.0.0 (553A42F6E74BAAE2)(53C9BE9B6308AABF)',
        }

save_path = 'D:\\Users\\User\\Desktop\\sunapp_data\\{}'

def add_save_path(name,suffix='.txt'):
    return save_path.format(name+suffix)

def split_arr(keys,num):
    threadKeys = []
    nums = round(len(keys) / num)
    for i in range(num):
        if i == num -1:
            threadKeys.append(keys[i*nums:len(keys)])
        else:
            threadKeys.append(keys[i * nums:(i + 1) * nums])
    return threadKeys

def get_keys(path):
    with open(path, 'r') as f:
        lines = f.readlines()
        f = lambda qq: qq.replace('\r\n', '').replace('\r', '').replace('\n', '').strip()
        res = set([f(v) for v in lines])
    return res

def write_json2txt(res,name):
    with open(add_save_path(name),'w') as f:
        json.dump(res,f,ensure_ascii=False)

def read_json(path):
    with open(path,'r') as f:
        res = json.load(f)
    return res

def get_allinfo(path):
    res = []
    with open(add_save_path(path)) as f:
        print(path)
        res.append(json.load(f,strict=False))
    df = pd.DataFrame(functools.reduce(lambda a,b:a+b,res))
    df.to_csv(save_path.format(path+".csv"),header=True,index=None)

def get_ids(keys,root_url,index=1):
    ids = []
    for key in keys:
        try:
            url = urllib.parse.unquote(root_url,'utf8').format(key)
            text = requests.get(url,headers=root_header).text
            id = str(int(json.loads(text)['result']) + index)
            ids.append({'key':key,'id':id})
        except:
            pass
    return ids

def get_info_from_ids(ids,info_url,pre_header=None,target_header='FRIAPPID',file_header=None):
    all_res = []
    all_error = []
    i = 0
    for info in ids:
        key = info['key']
        pre_headr = pre_header if pre_header else ['result']
        target_header = target_header if target_header else ['APPID']
        file_header = file_header if file_header else ['id1','id2']
        try:
            find_url = urllib.parse.unquote(info_url,'utf8').format(info['id'])
            f_text = requests.get(find_url,headers=info_header).text
            values = json.loads(f_text)
            for k in pre_headr:
                values = values[k]
            i += 1
            print("处理到%s个id %s"%(str(i),key))
            if values:
                for info in values:
                    v = info[target_header]
                    all_res.append({file_header[0]:key,file_header[1]:v})
        except:
            print('id %s 出现异常'%key)
            all_error.append({'id':key})
    return all_res,all_error

def qq_friend(root_url,info_url,keys,res_name,error_name,time_out=20):
    ids = get_ids(keys,root_url)
    time.sleep(time_out)
    all_res,all_error = get_info_from_ids(ids,info_url,pre_header=['result','tableData','results'],file_header=['qq1','qq2'])
    write_json2txt(all_res, res_name)
    write_json2txt(all_error, error_name)

def qq_groupids(root_url,info_url,keys,res_name=None,error_name=None,time_out=20):
    ids = get_ids(keys,root_url)
    time.sleep(time_out)
    all_res, all_error = get_info_from_ids(ids, info_url, pre_header=['result', 'results'],target_header='groupId',
                                           file_header=['qq', 'groupid'])
    if res_name and error_name:
        write_json2txt(all_res, res_name)
        write_json2txt(all_error, error_name)
    return set([row.get('groupid') for row in all_res])

def qq_group_member(root_url,info_url,keys,res_name,error_name,time_out=20):
    ids = get_ids(keys,root_url)
    time.sleep(time_out)

    all_res, all_error = get_info_from_ids(ids, info_url, pre_header=['result','tableData','results'],
                                           target_header='APPID',file_header=['groupid', 'qq'])
    write_json2txt(all_res, res_name)
    write_json2txt(all_error, error_name)

if __name__ == '__main__':

    # keys = get_keys('D:\\Users\\User\\Desktop\\case_data\\QQ.txt')
    # qq_friend(friend_root_url,friend_info_url,keys,'qq_friend','qq_friend_error',time_out=20)
    # groupids = qq_groupids(groupid_root_url,groupid_info_url,keys,'qq_groupid','qq_groupid_error',time_out=20)
    # qq_friend(groupmember_root_url,groupmember_info_url,groupids,'qq_group_member','qq_group_member_error',time_out=20)
    # #
    # def prinf(a):
    #     print(a)
    # threadNum = 4
    # threads = []
    # tmp = [1,2,3,4]
    # threadKeys = split_arr(keys,threadNum)
    # for i in range(threadNum):
    #     # t = threading.Thread(target=prinf,args=(friend_root_url,friend_info_url,threadKeys[i],'qq_friend_%s'%i,'qq_friend_error_%s'%i,20,))
    #     t = threading.Thread(target=prinf,args=(tmp[i],))
    #     threads.append(t)
    # for t in threads:
    #     t.start()
    # for t in threads:
    #     t.join()
    # get_allinfo('qq_friend')
    # get_allinfo('qq_groupid')
    # get_allinfo('qq_groupmember')
    get_allinfo('wechat_friend')