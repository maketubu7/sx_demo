# -*- coding: utf-8 -*-
# @Time    : 2020/3/14 14:58
# @Author  : Deng Wenxing
# @Email   : dengwenxingae86@163.com
# @File    : person_schema_info.py
# @Software: PyCharm

oversea_person_schema = [
    ## ods_pol_sec_oversea_pers_lodg	境外人员住宿信息
    {'tablename':'ods_pol_sec_oversea_pers_lodg','zjhm':'cred_num','xm':"concat(for_first_name,' ',for_last_name)",
     'zym':'""','mz':'""','whcd':'""','hyzk':'""','gj':'nation_name','jg':'""',
     'zzmm':'""','hkszdxz':'""','sjjzxz':'""','ywxm':'concat(for_first_name," ",for_last_name)','rank_time':'admi_time','table_sort':1
     }
]

inbord_person_schema_bk = [
    ###所有包含人的信息的表
    {'tablename':'ods_pol_per_actu_popu','zjhm':'cred_num','xm':"name",'zym':'fur_name','mz':'nation_desig',
     'whcd':'eduhis_name','hyzk':'mar_sta_name','gj':'nation_name','zzmm':'polsta_name','hkszdxz':'domic_addr_name',
     'sjjzxz':'actu_live_addr_addr_name','jg':'native_addi_name','ywxm':'""','rank_time':'coll_time','table_sort':1
     },
    ##婚姻登记 ods_gov_mca_civadm_marreg
    {'tablename':'ods_gov_mca_civadm_marreg','zjhm':'man_cert_no','xm':"man_name",'zym':'""','mz':'man_nation_desig',
     'whcd':'man_educ_leve_name','hyzk':'已婚','gj':'man_nation_name','zzmm':'""','hkszdxz':'man_domic_addr_name',
     'sjjzxz':'man_pred_addr_name','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':20
     },
    {'tablename':'ods_gov_mca_civadm_marreg','zjhm':'woman_cert_no','xm':"woman_name",'zym':'""','mz':'woman_nation_desig',
     'whcd':'woman_educ_leve_name','hyzk':'已婚','gj':'woman_nation_name','zzmm':'""','hkszdxz':'woman_domic_addr_name',
     'sjjzxz':'woman_pred_addr_name','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':20
     },
    ## 出生证信息 ods_gov_nca_bircer_info
    {'tablename':'ods_gov_nca_bircer_info','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""',
     'sjjzxz':'fami_addr_addr_name','jg':'""','ywxm':'""','rank_time':'sign_date','table_sort':30
     },
    {'tablename':'ods_gov_nca_bircer_info','zjhm':'mother_cert_no','xm':"mother_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""',
     'sjjzxz':'actu_live_addr_addr_name','jg':'""','ywxm':'""','rank_time':'sign_date','table_sort':30
     },
    {'tablename':'ods_gov_nca_bircer_info','zjhm':'father_cert_no','xm':"father_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""',
     'sjjzxz':'actu_live_addr_addr_name','jg':'""','ywxm':'""','rank_time':'sign_date','table_sort':30
     },
    ##三大运营商机主信息 ods_pol_sec_thrmajope_own_info
    {'tablename':'ods_pol_sec_thrmajope_own_info','zjhm':'cred_num','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""',
     'sjjzxz':'addr_name','jg':'""','ywxm':'""','rank_time':'open_acc_date','table_sort':40
     },
    ## ods_soc_sbd_phone_owner	机主表
    {'tablename':'ods_soc_sbd_phone_owner','zjhm':'cred_num','xm':"user_name",'zym':'""','mz':'nation_desig',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'polsta_name','hkszdxz':'cred_addr_name',
     'sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'birt_time','table_sort':40
     },
    ##ods_pol_sec_inbord_pers_lodg	境内人员住宿信息
    {'tablename':'ods_pol_sec_inbord_pers_lodg','zjhm':'cred_num','xm':"name",'zym':'""','mz':'nation_desig',
     'whcd':'""','hyzk':'""','gj':'nation_name','zzmm':'""','hkszdxz':'domic_addr_name',
     'sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'admi_time','table_sort':50
     },
    ##ods_bdq_tb_zy_qgbdqjdcdjxx	机动车登记表
    {'tablename':'ods_bdq_tb_zy_qgbdqjdcdjxx','zjhm':'ssjdc_soyr_zjhm','xm':"ssjdc_soyr_xm",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'larq','table_sort':60
     },
    ##ods_gov_edu_kindergarten_stu	幼儿园学生信息
    {'tablename':'ods_gov_edu_kindergarten_stu','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'domic_addr_name',
     'sjjzxz':'""','jg':'native_addi_name','ywxm':'""','rank_time':'coll_time','table_sort':70
     },
    {'tablename':'ods_gov_edu_kindergarten_stu','zjhm':'guar1_cert_no','xm':"guar1_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""',
     'sjjzxz':'guar1_addr_addr_name','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':70
     },
    {'tablename':'ods_gov_edu_kindergarten_stu','zjhm':'guar2_cert_no','xm':"guar2_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'domic_addr_name',
     'sjjzxz':'guar2_addr_addr_name','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':70
     },
    ##ods_gov_edu_scho_info	学校信息 only 2
    {'tablename':'ods_gov_edu_scho_info','zjhm':'admi_priper_cert_no','xm':"admi_priper_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':80
     },
    {'tablename':'ods_gov_edu_scho_info','zjhm':'party_priper_cert_no','xm':"party_priper_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':80
     },
    {'tablename':'ods_gov_edu_scho_info','zjhm':'busi_prin_per_cert_no','xm':"busi_prin_per_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':80
     },
    ##ods_gov_edu_stud_info	大中小学生信息
    {'tablename':'ods_gov_edu_stud_info','zjhm':'cred_num','xm':"name",'zym':'fur_name','mz':'nation_desig',
     'whcd':'educ_leve_name','hyzk':'""','gj':'""','zzmm':'polsta_name','hkszdxz':'domic_addr_name',
     'sjjzxz':'pred_addr_name','jg':'native_addi_name','ywxm':'""','rank_time':'coll_time','table_sort':31
     },
    ## only 2
    {'tablename':'ods_gov_edu_stud_info','zjhm':'guar_cert_no','xm':"guar_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':31
     },
    {'tablename':'ods_gov_edu_stud_info','zjhm':'father_cert_no','xm':"father_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'father_pred_addr_name','jg':'""',
     'ywxm':'""','rank_time':'coll_time','table_sort':31
     },
    {'tablename':'ods_gov_edu_stud_info','zjhm':'mother_cert_no','xm':"mother_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'mother_pred_addr_name','jg':'""',
     'ywxm':'""','rank_time':'coll_time','table_sort':31
     },
    ##ods_gov_ind_indcompr_reginf	工商主体登记信息
    {'tablename':'ods_gov_ind_indcompr_reginf','zjhm':'law_repr_comm_cert_code','xm':"lent_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'actu_live_addr_addr_name','jg':'""',
     'ywxm':'""','rank_time':'coll_time','table_sort':32
     },
    ## only 1
    {'tablename':'ods_gov_ind_indcompr_reginf','zjhm':'main_resp_pers_cert_no','xm':'""','zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':32
     },
    ##ods_gov_sse_socsec_payfee_info	社保缴费信息 only 2
    {'tablename':'ods_gov_sse_socsec_payfee_info','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':33
     },
    ##ods_pol_crim_dethou_detes_info	拘留所在押人员信息
    {'tablename':'ods_pol_crim_dethou_detes_info','zjhm':'cred_num','xm':"name",'zym':'""','mz':'nation_desig',
     'whcd':'educ_leve_name','hyzk':'""','gj':'nation_name','zzmm':'polsta_name','hkszdxz':'acc_loc_addr_name',
     'sjjzxz':'pred_addr_name','jg':'native_addi_name','ywxm':'""','rank_time':'coll_time','table_sort':3
     },
    # ods_pol_crim_enfcas_pol_info	警情基本信息 only 2
    {'tablename':'ods_pol_crim_enfcas_pol_info','zjhm':'cp_per_cert_no','xm':"cp_per_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':34
     },
    # ods_pol_crim_enfo_case_info	案件基本信息 only 2
    {'tablename':'ods_pol_crim_enfo_case_info','zjhm':'reporter_cert_no','xm':"reporter_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':34
     },
    # ods_pol_pri_detesta_outper_info	拘留所出所人员信息 only 2
    {'tablename':'ods_pol_pri_detesta_outper_info','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_pol_pri_detsta_outper_info	看守所出所人员信息 only 2
    {'tablename':'ods_pol_pri_detsta_outper_info','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_pol_pri_drcent_heaexa	戒毒所入所健康检查信息 only 2
    {'tablename':'ods_pol_pri_drcent_heaexa','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_pol_pri_drcper_rooadj	戒毒所人员监室调整信息 only 2
    {'tablename':'ods_pol_pri_drcper_rooadj','zjhm':'cred_num','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_pol_pri_drutre_outper_info	戒毒所出所人员信息 only 2
    {'tablename':'ods_pol_pri_drutre_outper_info','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_pol_pri_drutrpla_pers_info	戒毒所人员信息
    {'tablename':'ods_pol_pri_drutrpla_pers_info','zjhm':'cred_num','xm':"name",'zym':'""','mz':'nation_desig',
     'whcd':'educ_leve_name','hyzk':'mar_sta_name','gj':'natio_name','zzmm':'polsta_name','hkszdxz':'domic_addr_name',
     'sjjzxz':'pred_addr_name','jg':'native_addi_name','ywxm':'""','rank_time':'coll_time','table_sort':4
     },
    # ods_pol_pri_hist_detes_deares	看守所历史在押人员处理结果 only 2
    {'tablename':'ods_pol_pri_hist_detes_deares','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_pol_pri_jaient_heaexa	看守所入所健康检查信息 only 2
    {'tablename':'ods_pol_pri_jaient_heaexa','zjhm':'cred_num','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_pol_pri_jaiper_rooadj	看守所人员监室调整信息 only 2
    {'tablename':'ods_pol_pri_jaiper_rooadj','zjhm':'cred_num','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_pol_pri_prient_heaexa	拘留所入所健康检查信息 only 2
    {'tablename':'ods_pol_pri_prient_heaexa','zjhm':'cred_num','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_pol_pri_priper_rooadj	拘留所人员监室调整信息 only 2
    {'tablename':'ods_pol_pri_priper_rooadj','zjhm':'cred_num','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_pol_sec_netbar_intper_info	网吧上网人员信息
    {'tablename':'ods_pol_sec_netbar_intper_info','zjhm':'cred_num','xm':"inte_per_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'nation_name','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_soc_civ_avia_arrport_data	民航进港数据
    {'tablename':'ods_soc_civ_avia_arrport_data','zjhm':'cred_num','xm':"pass_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'actu_live_addr_addr_name','jg':'domic_admin_name',
     'ywxm':'pass_for_name','rank_time':'coll_time','table_sort':35
     },
    # ods_soc_civ_avia_leaport_data	民航离港数据
    {'tablename':'ods_soc_civ_avia_leaport_data','zjhm':'cred_num','xm':"pass_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'pass_for_name',
     'rank_time':'coll_time','table_sort':35
     },
    # ods_soc_civ_avia_rese	民航订票
    {'tablename':'ods_soc_civ_avia_rese','zjhm':'cred_num','xm':"pass_name",'zym':'""','mz':'""','whcd':'""','hyzk':'""',
     'gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'domic_admin_name',
     'ywxm':'concat(pass_for_first_name," ",pass_for_last_name)','rank_time':'coll_time','table_sort':35
     },
    # ods_soc_sbd_pertax_info	个人交税信息 only 2
    {'tablename':'ods_soc_sbd_pertax_info','zjhm':'paytax_pers_cred_num','xm':"paytax_pers_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_soc_traf_raista_entper	火车站进站人员信息
    {'tablename':'ods_soc_traf_raista_entper','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
    # ods_soc_traf_raiway_saltic_data	铁路售票数据
    {'tablename':'ods_soc_traf_raiway_saltic_data','zjhm':'cred_num','xm':"name",'zym':'""','mz':'nation_desig',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'domic_addr_name','jg':'""','ywxm':'""',
     'rank_time':'coll_time','table_sort':35
     },
    # ods_pol_pri_dethou_detees_info	看守所在押人员信息
    {'tablename':'ods_pol_pri_dethou_detees_info','zjhm':'cred_num','xm':"name",'zym':'""','mz':'nation_desig',
     'whcd':'educ_leve_name','hyzk':'mar_sta_name','gj':'""','zzmm':'polsta_name','hkszdxz':'domic_addi_detail_addr',
     'sjjzxz':'addr_desig','jg':'native_addi_name','ywxm':'""','rank_time':'coll_time','table_sort':35
     },
]


inbord_person_schema = [
    ###所有包含人的信息的表
    {'tablename':'ods_pol_per_actu_popu','zjhm':'cred_num','xm':"name",'zym':'fur_name','mz':'nation_desig',
     'whcd':'eduhis_name','hyzk':'mar_sta_name','gj':'nation_name','zzmm':'polsta_name','hkszdxz':'domic_addr_name',
     'sjjzxz':'actu_live_addr_addr_name','jg':'native_addi_name','ywxm':'""','rank_time':'coll_time','table_sort':1
     },
    #婚姻登记 ods_gov_mca_civadm_marreg
    {'tablename':'ods_gov_mca_civadm_marreg','zjhm':'man_cert_no','xm':"man_name",'zym':'""','mz':'man_nation_desig',
     'whcd':'man_educ_leve_name','hyzk':'"已婚"','gj':'man_nation_name','zzmm':'""','hkszdxz':'man_domic_addr_name',
     'sjjzxz':'man_pred_addr_name','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':20
     },
    {'tablename':'ods_gov_mca_civadm_marreg','zjhm':'woman_cert_no','xm':"woman_name",'zym':'""','mz':'woman_nation_desig',
     'whcd':'woman_educ_leve_name','hyzk':'"已婚"','gj':'woman_nation_name','zzmm':'""','hkszdxz':'woman_domic_addr_name',
     'sjjzxz':'woman_pred_addr_name','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':20
     },
    # 出生证信息 ods_gov_nca_bircer_info
    {'tablename':'ods_gov_nca_bircer_info','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""',
     'sjjzxz':'fami_addr_addr_name','jg':'""','ywxm':'""','rank_time':'sign_date','table_sort':30
     },
    {'tablename':'ods_gov_nca_bircer_info','zjhm':'mother_cert_no','xm':"mother_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""',
     'sjjzxz':'fami_addr_addr_name','jg':'""','ywxm':'""','rank_time':'sign_date','table_sort':30
     },
    {'tablename':'ods_gov_nca_bircer_info','zjhm':'father_cert_no','xm':"father_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""',
     'sjjzxz':'fami_addr_addr_name','jg':'""','ywxm':'""','rank_time':'sign_date','table_sort':30
     },
    ##ods_pol_sec_inbord_pers_lodg	境内人员住宿信息
    {'tablename':'ods_pol_sec_inbord_pers_lodg','zjhm':'cred_num','xm':"name",'zym':'""','mz':'nation_desig',
     'whcd':'""','hyzk':'""','gj':'nation_name','zzmm':'""','hkszdxz':'domic_addr_name',
     'sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'admi_time','table_sort':50
     },
    ##ods_bdq_tb_zy_qgbdqjdcdjxx	机动车登记表
    {'tablename':'ods_bdq_tb_zy_qgbdqjdcdjxx','zjhm':'ssjdc_soyr_zjhm','xm':"ssjdc_soyr_xm",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'larq','table_sort':60
     },
    ##ods_gov_edu_kindergarten_stu	幼儿园学生信息
    {'tablename':'ods_gov_edu_kindergarten_stu','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'domic_addr_name',
     'sjjzxz':'""','jg':'native_addi_name','ywxm':'""','rank_time':'coll_time','table_sort':70
     },
    {'tablename':'ods_gov_edu_kindergarten_stu','zjhm':'guar1_cert_no','xm':"guar1_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""',
     'sjjzxz':'guar1_addr_addr_name','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':70
     },
    {'tablename':'ods_gov_edu_kindergarten_stu','zjhm':'guar2_cert_no','xm':"guar2_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'domic_addr_name',
     'sjjzxz':'guar2_addr_name','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':70
     },
    #ods_gov_edu_scho_info	学校信息 only 2
    {'tablename':'ods_gov_edu_scho_info','zjhm':'admi_priper_cert_no','xm':"admi_priper_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':80
     },
    {'tablename':'ods_gov_edu_scho_info','zjhm':'party_priper_cert_no','xm':"party_priper_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':80
     },
    {'tablename':'ods_gov_edu_scho_info','zjhm':'busi_prin_per_cert_no','xm':"busi_prin_per_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':80
     },
    #ods_gov_edu_stud_info	大中小学生信息
    {'tablename':'ods_gov_edu_stud_info','zjhm':'cred_num','xm':"name",'zym':'fur_name','mz':'nation_desig',
     'whcd':'educ_leve_name','hyzk':'""','gj':'""','zzmm':'polsta_name','hkszdxz':'domic_addr_name',
     'sjjzxz':'pred_addr_name','jg':'native_addi_name','ywxm':'""','rank_time':'coll_time','table_sort':31
     },
    ## only 2
    {'tablename':'ods_gov_edu_stud_info','zjhm':'guar_cert_no','xm':"guar_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':31
     },
    {'tablename':'ods_gov_edu_stud_info','zjhm':'father_cert_no','xm':"father_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'father_pred_addr_name','jg':'""',
     'ywxm':'""','rank_time':'coll_time','table_sort':31
     },
    {'tablename':'ods_gov_edu_stud_info','zjhm':'mother_cert_no','xm':"mother_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'mother_pred_addr_name','jg':'""',
     'ywxm':'""','rank_time':'coll_time','table_sort':31
     },
    #ods_gov_ind_indcompr_reginf	工商主体登记信息
    {'tablename':'ods_gov_ind_indcompr_reginf','zjhm':'law_repr_cred_num','xm':"lent_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'law_repr_residence_addr_name','jg':'""',
     'ywxm':'""','rank_time':'coll_time','table_sort':32
     },
    # ods_pol_crim_enfo_case_info	案件基本信息 only 2
    {'tablename':'ods_pol_crim_enfo_case_info','zjhm':'reporter_cert_no','xm':"reporter_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':34
     },
    # ods_soc_civ_avia_leaport_data	民航离港数据
    {'tablename':'ods_soc_civ_avia_leaport_data','zjhm':'cred_num','xm':"pass_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'pass_for_name',
     'rank_time':'coll_time','table_sort':35
     },
    # ods_soc_civ_avia_rese	民航订票
    {'tablename':'ods_soc_civ_avia_rese','zjhm':'cred_num','xm':"pass_name",'zym':'""','mz':'""','whcd':'""','hyzk':'""',
     'gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'domic_admin_name',
     'ywxm':'concat(pass_for_first_name," ",pass_for_last_name)','rank_time':'coll_time','table_sort':36
     },
    # ods_soc_traf_raista_entper	火车站进站人员信息
    {'tablename':'ods_soc_traf_raista_entper','zjhm':'cert_no','xm':"name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""','rank_time':'coll_time','table_sort':37
     },
    # ods_soc_traf_raiway_saltic_data	铁路售票数据
    {'tablename':'ods_soc_traf_raiway_saltic_data','zjhm':'cred_num','xm':"name",'zym':'""','mz':'nation_desig',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'domic_addr_name','jg':'""','ywxm':'""',
     'rank_time':'coll_time','table_sort':38
     },
    # 网吧上网信息 ods_pol_sec_netbar_intper_info
    {'tablename':'ods_pol_sec_netbar_intper_info','zjhm':'cred_num','xm':"inte_per_name",'zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""','ywxm':'""',
     'rank_time':'coll_time','table_sort':39
     },
    # ods_soc_sbd_phone_owner	机主表
    {'tablename':'ods_soc_sbd_phone_owner','zjhm':'cred_num','xm':'user_name','zym':'""','mz':'nation_desig',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""',
     'ywxm':'""','rank_time':'ente_net_time','table_sort':40
     },
    # ods_soc_civ_avia_arrport_data	民航进港数据
    {'tablename':'edge_person_arrived_airline_detail','zjhm':'sfzh','xm':'""','zym':'""','mz':'""',
     'whcd':'""','hyzk':'""','gj':'""','zzmm':'""','hkszdxz':'""','sjjzxz':'""','jg':'""',
    'ywxm':'""','rank_time':'hbrq','table_sort':88
     },
]