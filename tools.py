import pandas as pd
import scorecardpy as sc
from catch_data import *
import os
def merge_dataframes(path=None,user_id=None):
    '''
    将朴道数据合并规则和策略需要的特征宽表
    :param path: 朴道数据表格的存放路径
    :param user_id: 用户身份证
    :return: 合并宽表
    '''
    global missing_dict,specialist_dict,file_translate
    if path is None: path = os.getcwd()
    # 获取当前目录下所有文件名
    path=path+'/data/朴道数据'
    file_names = os.listdir(path)
    # 筛选包含.xlsx扩展名和包含$的文件名
    files =list(file_translate.values())
    merged_data = pd.DataFrame()
    for dict_keys in files:
        try:
            cols=[]
            table=dict_keys+'.xlsx'
            if dict_keys in missing_dict.keys():col =missing_dict[dict_keys]
            elif dict_keys in specialist_dict.keys():col =specialist_dict[dict_keys]
            tempt_data=pd.read_excel(f'{path}/{table}')
            tempt_data['身份证']=tempt_data['身份证'].astype(str)
            indx=tempt_data.groupby('身份证')['创建时间'].idxmax()
            print(table,'success')
            tempt_data=tempt_data.loc[indx.values]
            print(tempt_data.shape)
            if user_id is not None:
                tempt_data=tempt_data[tempt_data['身份证'].isin(user_id)]
            if type(col)==str:cols.append(col)
            else:cols=col
            cols.append('身份证')
            if merged_data.shape[0]==0:
                merged_data['姓名']=tempt_data['姓名']
                merged_data[cols]=tempt_data[cols]
            else:merged_data=pd.merge(merged_data,tempt_data[cols],on='身份证',how='outer')
        except Exception as e:
            print(e)
            pass
        user_id=merged_data['身份证']
        merged_data=merged_data.drop(columns=['身份证'])
        merged_data.insert(0,'身份证',user_id)
    return merged_data

