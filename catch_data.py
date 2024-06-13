import os
from logging import exception

import pandas as pd
import numpy as np
import json
import warnings
import pymysql
import matplotlib.pyplot as plt
from datetime import date
import base64
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from pymongo import MongoClient


# path=input('==========输入数据库保存的路径：')
# path='C:/Users/HP/Desktop/yto/项目测试路径'
# os.chdir(path)
card=pd.read_excel('data/评分卡.xlsx')
dic=pd.read_excel('data/MySQL使用文档(完整版).xlsx')
warnings.filterwarnings('ignore')
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False

fms_risk=['tb_credit_report_person_xuanyuan_score',
          'tb_credit_report_person_today_loan_apply',
          'tb_credit_report_person_zebra_spread',
          'tb_credit_report_person_loan_apply',
          'tb_credit_report_person_aggregate_risk',
          'tb_credit_report_person_anti_fraud',
          'tb_credit_report_person_bond_score',
          'tb_credit_report_person_cash_credit',
          'tb_credit_report_person_online_state',
          'tb_credit_report_person_online_time',
          'tb_credit_report_person_portrait',
          'tb_credit_report_person_repayment_level',
          'tb_credit_report_person_special_list']

file_translate={
'tb_credit_report_person_xuanyuan_score':'轩辕分',
'tb_credit_report_person_aggregate_risk':'综合风险画像',
'tb_credit_report_person_anti_fraud':'反欺诈评分',
'tb_credit_report_person_bond_score':'共债分',
'tb_credit_report_person_cash_credit':'现金贷信用分',
'tb_credit_report_person_online_state':'在网状态',
'tb_credit_report_person_online_time':'在网时长',
'tb_credit_report_person_portrait':'画像指标',
'tb_credit_report_person_repayment_level':'还款等级',
'tb_credit_report_person_special_list':'特殊名单',
'tb_credit_report_person_today_loan_apply':'当日借贷申请',
'tb_credit_report_person_zebra_spread':'斑马扩散',
'tb_credit_report_person_loan_apply':'借贷申请行为'}

missing_dict={
'综合风险画像':['12月内手机号码申请命中互联网金融平台数','圈团2风险等级V1'],
'共债分':['共债等级[1,10]','共债分','最近 30 天查询非银机构数','最近 60 天查询非银机构数'],
'借贷申请行为':['身份证号查询，近15天通过非银申请的次数','身份证号查询，近3个月通过非银周末申请的业务线数量','身份证号查询，近1个月通过非银周末申请的业务线数量','身份证号查询，近15天通过非银周末申请的业务线数量','身份证号查询，近15天通过非银周末申请的次数'],
'斑马扩散':['斑马扩散风险等级','关系网络平均风险等级'],
'画像指标':['[0,6]等级越高近3月非免息的信用消费的金额越高','[0,5]等级越高历史90天内逾期天数越久，信用等级越差','[0,5]等级越高历史90天内逾期产品越多，履约等级越差','[0,5]等级越高历史60天内逾期天数越久，信用等级越差','[0,5]等级越高历史60天内逾期产品越多，履约等级越差','[0,5]等级越高历史365天内逾期天数越久，信用等级越差','[0,5]等级越高历史365天内逾期产品越多，履约等级越差','[0,5]等级越高历史30天内逾期天数越久，信用等级越差','[0,5]等级越高历史180天内逾期产品越多，履约等级越差','[0,5]等级越高历史30天内逾期产品越多，履约等级越差','[0,5]等级越高历史180天内逾期天数越久，信用等级越差'],
'反欺诈评分':'反欺诈分',
'在网状态':'在网状态',
'在网时长':'在网时长',
'轩辕分':'轩辕分',
'现金贷信用分':'现金贷分',
'还款等级':'[1,25]越大还款越强'
}

col_in_table={'共债分':['共债分','共债等级[1,10]', '历史查询总机构数'],
              '反欺诈评分':'反欺诈分',
              '现金贷信用分':'现金贷分',
              '在网时长':'在网时长',
              '轩辕分':'轩辕分',
              '斑马扩散':'斑马扩散风险等级'}

specialist_dict={'特殊名单':['是否存在疑似电商虚假交易风险','是否存在疑似金融黑产风险','是否存在疑似羊毛党风险','是否存在疑似准入风险','是否存在公开负面信息风险','是否存在信贷欺诈风险','是否存在信贷逾期风险','是否存在异常聚集风险','是否存在多头申请风险'],
            '反欺诈评分':[ '疑似资料伪造包装', '疑似涉黄涉恐行为','疑似资料仿冒行为', '疑似营销活动欺诈', '疑似身份信息不符', '疑似公开信息失信', '疑似金融黑产相关', '疑似手机猫池欺诈','疑似风险设备环境', '疑似资金紧张人群', '疑似异常支付行为', '疑似线上支付白户', '疑似线上养号小号', '疑似线上涉赌行为','疑似存在失联风险', '疑似账号被盗风险']}

history2current={'PersonXuanyuanScore':'tb_credit_report_person_xuanyuan_score',
'PersonBondScore':'tb_credit_report_person_bond_score',
'PersonPortrait':'tb_credit_report_person_portrait',
'PersonRepaymentLevel':'tb_credit_report_person_repayment_level',
'PersonAntiFraud':'tb_credit_report_person_anti_fraud',
'PersonAggregateRisk':'tb_credit_report_person_aggregate_risk',
'PersonZebraSpread':'tb_credit_report_person_zebra_spread',
'PersonSpecialList':'tb_credit_report_person_special_list',
# 'PersonLoanApply':'tb_credit_report_person_today_loan_apply',
'PersonCashCredit':'tb_credit_report_person_cash_credit',
'PersonOnlineTime':'tb_credit_report_person_online_time',
'PersonOnlineState':'tb_credit_report_person_online_state'}

client = MongoClient('mongodb://ods_prd_ro:u%257e577HnvpEY*h6@s-uf62656caf0435e4.mongodb.rds.aliyuncs.com:3717,s-uf6a510d41341184.mongodb.rds.aliyuncs.com:3717/?authSource=ods')
db = client['ods']

breaks={'共债分':[-np.inf,0,40,50,60,70,100],
       '在网时长':[-np.inf,0,5,np.inf],
       '反欺诈分':[-np.inf,30,40,50,100],
       '斑马扩散风险等级':[-np.inf,0,2,5,10],
       '轩辕分':[-np.inf,500,620,650,850],
       '共债等级[1,10]':[-np.inf,2,3,4,5,10],
       '历史查询总机构数':[-np.inf,0,2,6,np.inf],
       '现金贷分':[-np.inf,500,550,600,650,850]}


type_dic=dict(zip(dic['字段说明'],dic['字段类型']))


def read_database(query):
    conn = pymysql.connect(host='martin-ma.xyz', user='fms_user', password='^uC@5OKqkzXd', database='fms_db',port=53306)
    cursor = conn.cursor()
    cursor.execute(query)
    query_result = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    data = pd.DataFrame(query_result, columns=columns)
    data=data.drop_duplicates()
    return data


# ###########################导入电商贷客户信息####################################
###电商贷客户使用用户编码与账单列表进行链接
dsd_query = '''
    SELECT f.*,MAX(l.overdue_flag)AS overdue_flag,MAX(l.create_date) AS create_date,
     CONCAT(f.user_name, IF(AES_DECRYPT(FROM_BASE64(f.id_no),'NGE6JS5sL2xwdg==') IS NULL,'', CONCAT(
                        CONCAT('-',LEFT(AES_DECRYPT(FROM_BASE64(f.id_no),'NGE6JS5sL2xwdg=='),4),'****',RIGHT(AES_DECRYPT(FROM_BASE64(f.id_no),'NGE6JS5sL2xwdg=='),4))
                    ))) user_name_id
    FROM (
    SELECT DISTINCT f.apply_user_code,f.apply_enterprise_code,f.apply_person_idcard AS id_no,user_name,
    CASE WHEN f.product_code='1561746239815249920' THEN 'DSD' WHEN f.product_code='1377219161257963520' THEN 'XW'  ELSE f.product_code END AS product_code
    FROM tb_factoring f
    UNION
    SELECT DISTINCT f.apply_user_code,f.apply_enterprise_code,f.apply_user_id_number AS id_no,apply_user_name,
    CASE WHEN f.product_code='1561746239815249920' THEN 'DSD' WHEN f.product_code='1377219161257963520' THEN 'XW'  ELSE f.product_code END AS product_code
    FROM tb_credit_apply f
    )f
    LEFT JOIN tb_bill b ON b.user_code=f.apply_user_code
    LEFT JOIN (SELECT business_id,overdue_flag,create_date FROM tb_leasing)l ON l.business_id=b.bill_code
    WHERE overdue_flag IS NOT NULL AND f.product_code='DSD'
    GROUP BY f.id_no

    '''

####################导入小微保客户信息####################################
###小微保客户使用企业编码与账单列表进行链接
xw_query ='''
SELECT DISTINCT f.*,max(l.overdue_flag)AS overdue_flag,min(f.create_date),
 CONCAT(f.user_name, IF(AES_DECRYPT(FROM_BASE64(f.id_no),'NGE6JS5sL2xwdg==') IS NULL,'', CONCAT(
        CONCAT('-',LEFT(AES_DECRYPT(FROM_BASE64(f.id_no),'NGE6JS5sL2xwdg=='),4),'****',RIGHT(AES_DECRYPT(FROM_BASE64(f.id_no),'NGE6JS5sL2xwdg=='),4))
         ))) user_name_id
from (
SELECT DISTINCT f.apply_user_code,f.apply_enterprise_code,f.apply_person_idcard AS id_no,user_name,create_date,
case when f.product_code='1561746239815249920' then 'DSD' WHEN f.product_code='1377219161257963520' THEN 'XW'  else f.product_code END AS product_code
FROM tb_factoring f
UNION
SELECT DISTINCT f.apply_user_code,f.apply_enterprise_code,f.apply_user_id_number as id_no,apply_user_name,create_date,
case when f.product_code='1561746239815249920' then 'DSD' WHEN f.product_code='1377219161257963520' THEN 'XW'  else f.product_code END AS product_code
FROM tb_credit_apply f
)f
LEFT JOIN tb_bill b ON b.user_code=f.apply_user_code
LEFT JOIN tb_leasing l ON l.enterprise_code=f.apply_enterprise_code
WHERE l.overdue_flag IS NOT NULL AND f.product_code='XW'
GROUP BY f.id_no
'''
def output_custom_data(excute_code):
    user_code=  read_database(excute_code)
    # 将 DataFrame 保存为 Excel 文件
    user_code['id_no'],_,_=decode(user_code['id_no'])
    return user_code

def translate_columns(data,table_name):
    ## 将列名转为中文
    global dic
    current_dictable=dic[dic['表名']==table_name]
    current_dic=dict(zip(current_dictable['字段名'],current_dictable['字段说明']))
    current_dic = {key: (value if value else key) for key, value in current_dic.items()}
    conv_data=data.rename(columns=current_dic)
    return conv_data

def translate_dataframe(json_data,file,data):
    '''
    将数据库中的字段利用数据字典翻译成中文
    :param file: 当前表格名称
    :save：结果保存至当前文件路径
    '''
    print('当前文件为',file)
    table_name=file+'.xlsx'
    history=json_data[json_data['interface_name']==file]
    history = pd.json_normalize(history['snapshot_data'])

    ##将历史json数据转换为df
    data_col=list(data.columns)
    data_lower=[item.lower().replace('_','') if isinstance(item, str) else item for item in data_col]
    data_dict=dict(zip(data_lower,data_col))
    data=data.rename(columns=dict(zip(data_col,data_lower)))
    history_col=list(history.columns)
    history_lower=[item.lower().replace('_','') if isinstance(item, str) else item for item in history_col]
    history_dict=dict(zip(history_lower,history_col))
    history=history.rename(columns=dict(zip(history_col,history_lower)))
    if history.shape[0]==0:conv_data=data
    else:conv_data=pd.concat([data,history],axis=0,join='inner')
    conv_data=conv_data.rename(columns=data_dict)

    # ## 将列名转为中文
    # current_dictable=dic[dic['表名']==file]
    # current_dic=dict(zip(current_dictable['字段名'],current_dictable['字段说明']))
    # current_dic = {key: (value if value else key) for key, value in current_dic.items()}
    conv_data=translate_columns(conv_data,file)

    #将文件名称翻译为中文,并另存为其他路径
    file_name=file_translate[file]+'.xlsx'
    conv_data['创建时间']=pd.to_datetime(conv_data['创建时间'])
    conv_data['身份证'],_,_=decode(conv_data['身份证'])
    conv_data=conv_data.drop_duplicates(subset=['创建时间','身份证'])

    # conv_data.to_excel(f'/translate/{file_name}',index=False)
    print(f'最新数据大小为{data.shape},历史数据大小为{history.shape}')
    print(f'{table_name}合并完成，合并之后表格大小为{conv_data.shape}')
    return conv_data,file_name


def sexual(id_number):
    try:
        # 提取出生年份码（第7到第14位）
        gender = int(id_number[-2])%2
        return gender
    except:return 'id_no error'

def count_birth(id_number):
# 确保身份证号码是18位数字
    try:
        # 提取出生年份码（第7到第14位）
        birth_year = int(id_number[6:10])
        birth_month = int(id_number[10:12])
        birth_date = int(id_number[12:14])
        return date(birth_year,birth_month,birth_date)
    except:return 'id_no error'

def decode(data_id_no):
    password = b'NGE6JS5sL2xwdg=='
    aes = AES.new(password,AES.MODE_ECB)
    id_nos=[]
    birth_date=[]
    gender=[]
    for id_no in data_id_no:
        try:
            en_text = id_no.encode() #将字符串转换成bytes数据
            en_text = base64.decodebytes(en_text) #将进行base64解码，返回值依然是bytes
            den_text = aes.decrypt(en_text)
            id_number=str(den_text.decode("utf-8")).replace('\x0e','')
        except:
            id_number=id_no ##如果id_no不是身份证编译结果则直接存入id_no
        id_nos.append(id_number)
        birth_day=count_birth(id_number)
        birth_date.append(birth_day)
        gender.append(sexual(id_number))
    return id_nos,birth_date,gender

##############################导入最新的朴道数据####################################
def get_data():
    global fms_risk
    # try:
    dsd_user=output_custom_data(dsd_query)
    xw_user=output_custom_data(xw_query)
    with pd.ExcelWriter('data/user_info.xlsx') as w:
        dsd_user.to_excel(w,sheet_name='eloan_user_code',index=False)
        xw_user.to_excel(w,sheet_name='xw_user_code',index=False)
    load_json=input('==========是否解析历史json文件,(y/n):')

    os.makedirs(name='data/朴道数据',exist_ok=True)
    for table in fms_risk:
        excute_code=f'select * from fms_risk.{table}'
        df = read_database(excute_code)
        if load_json=='y':
            json_data = pd.read_json('data/history.json')
            json_data['snapshot_data'] = json_data['snapshot_data'].apply(json.loads)
            json_data['snapshot_data'] = [item[0] if item else np.nan for item in json_data['snapshot_data']]
            json_data['interface_name'] = json_data['interface_name'].replace(history2current)
            excute_code=f'select * from fms_risk.{table}'

            df = read_database(excute_code)
            translate_df,file_name=translate_dataframe(json_data,table,df)
        else:
            file_name=file_translate[table]+'.xlsx'
            history=pd.read_excel(f'data/朴道数据/{file_name}')
            current_df=translate_columns(df,table)
            user_id,_,_=decode(current_df['身份证'])
            current_df.drop(columns=['身份证'],inplace=True)
            current_df.insert(loc=0,column='身份证',value=user_id)
            # print('current_df:',current_df.columns,'\nhistory:',history.columns)
            translate_df=pd.concat([current_df,history],axis=0,join='outer')
        # 将 DataFrame 保存为 Excel 文件
        translate_df=translate_df.drop_duplicates(subset=['创建时间','身份证'])
        translate_df.to_excel(f'data/朴道数据/{file_name}', index=False)
        print(f'{file_name}导出成功')

    if input('是否获取秒贷朴道数据(y/n):')=='y':
        for key, value in history2current.items():
            if key=='PersonCashCredit':
                key+='Plus'
            try:
                table = 'PUDAO_' + key + '_PRODUCT_CACHE'
                print(table)
                collection = db[table]
                cursor = collection.find({})
                # 创建DataFrame
                df = pd.DataFrame(cursor)
                df1 = pd.json_normalize(df.param).rename(
                    columns={'cert_name': '姓名', 'cert_no': '身份证', 'mobile': '手机号'})
                df1['创建时间'] = df['createDate']
                df2 = pd.json_normalize(df.realRespContent)
                current_table = dic[dic['表名'] == value]
                current_dic = dict(zip(current_table['字段名'], current_table['字段说明']))
                df2 = df2.rename(columns=current_dic)
                df_all = pd.concat([df1, df2], axis=1)
                df_all['创建时间'] = pd.to_datetime(df_all['创建时间'], format='%Y%m%d%H%M%S')
                df_all.to_excel(f'data/朴道数据/秒贷客户/{file_translate[value]}.xlsx',index=False)
            #         print(df2.columns)
            except exception as e:
                print(e)
                pass




