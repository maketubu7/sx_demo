# -*- coding: utf-8 -*-
# @Time    : 2020/3/14 14:58
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : person_link_info.py
# @content : 人员关联证件信息
# @Software: PyCharm

person_link_phone = [
    ###所有存在人 电话关联的表
    #婚姻等级表
    {'tablename':'ods_gov_mca_civadm_marreg','zjhm':'man_cert_no','phone':'man_ctct_tel',
     'link_time':'coll_time'},
    {'tablename':'ods_gov_mca_civadm_marreg','zjhm':'woman_cert_no','phone':'woman_ctct_tel',
     'link_time':'coll_time'},
    # 机动车登记信息
    {'tablename':'ods_bdq_tb_zy_qgbdqjdcdjxx','zjhm':'ssjdc_soyr_zjhm','phone':'ssjdc_soyr_lxdh',
     'link_time':'cjsj'},
    # 幼儿园信息表
    {'tablename':'ods_gov_edu_kindergarten_stu','zjhm':'guar1_cert_no','phone':'guar1_ctct_tel',
     'link_time':'coll_time'},
    {'tablename':'ods_gov_edu_kindergarten_stu','zjhm':'guar2_cert_no','phone':'guar2_ctct_tel',
     'link_time':'coll_time'},
    #学校信息
    {'tablename':'ods_gov_edu_scho_info','zjhm':'admi_priper_cert_no','phone':'admi_priper_ctct_tel',
     'link_time':'coll_time'},
    {'tablename':'ods_gov_edu_scho_info','zjhm':'party_priper_cert_no','phone':'party_priper_ctct_tel',
     'link_time':'coll_time'},
    {'tablename':'ods_gov_edu_scho_info','zjhm':'busi_prin_per_cert_no','phone':'busi_prin_per_ctct_tel',
     'link_time':'coll_time'},
    #大学生信息
    {'tablename':'ods_gov_edu_stud_info','zjhm':'guar_cert_no','phone':'guar_ctct_tel',
     'link_time':'coll_time'},
    {'tablename': 'ods_gov_edu_stud_info', 'zjhm': 'father_cert_no', 'phone': 'father_ctct_tel',
     'link_time': 'coll_time'},
    {'tablename': 'ods_gov_edu_stud_info', 'zjhm': 'mother_cert_no', 'phone': 'mother_ctct_tel',
     'link_time': 'coll_time'},
    # 工商信息表
    {'tablename': 'ods_gov_ind_indcompr_reginf', 'zjhm': 'law_repr_cred_num', 'phone': 'law_repr_mob',
     'link_time': 'coll_time'},

    # 警情基本信息
    {'tablename': 'ods_pol_crim_enfcas_pol_info', 'zjhm': 'cp_per_cert_no', 'phone': 'cp_ctct_tel',
     'link_time': 'coll_time'},
    # 案件基本信息
    {'tablename': 'ods_pol_crim_enfo_case_info', 'zjhm': 'reporter_cert_no', 'phone': 'reporter_ctct_way',
     'link_time': 'coll_time'},

    # 航班订票信息
    {'tablename': 'ods_soc_civ_avia_rese', 'zjhm': 'cred_num', 'phone': 'pass_ctct_tel',
     'link_time': 'coll_time'},

    # 铁路订票信息
    {'tablename': 'ods_soc_traf_raiway_saltic_data', 'zjhm': 'cred_num', 'phone': 'ctct_tel',
     'link_time': 'coll_time'},
]