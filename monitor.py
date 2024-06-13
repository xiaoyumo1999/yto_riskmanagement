import datetime

from tools import *
from rules import *
from catch_data import *
import scorecardpy as sc


def count_psi(col,merged_data):
    psi = pd.DataFrame()
    psi_merged=merged_data.copy()
    psi_merged[col]=(psi_merged['当月人数占比']-psi_merged['上月人数占比'])*np.log(psi_merged['当月人数占比']/psi_merged['上月人数占比'])
    ##将psi计算结果为inf的值替换为0
    psi_merged=psi_merged.replace(np.inf,0)
    psi_merged=psi_merged.groupby('创建时间').agg({col:'sum'}).reset_index()[::-1]
    if psi.shape[0]==0:
        psi=psi_merged[['创建时间',col]]
        psi['对照时间']=psi['创建时间'].shift(-1)
        psi['时间']=psi.apply(lambda row: f'{row["创建时间"]} vs {row["对照时间"]}' if pd.notnull(row["对照时间"]) else None, axis=1)

        psi.drop(columns=['对照时间','创建时间'],inplace=True)
        psi=psi.dropna(subset='时间').set_index('时间')
    else:
        psi_merged['对照时间']=psi_merged['创建时间'].shift(-1)
        ##如果对照时间是空，也就是最后一期则没必要计算psi
        psi_merged['时间']=psi_merged.apply(lambda row: f'{row["创建时间"]} vs {row["对照时间"]}' if pd.notnull(row["对照时间"]) else None, axis=1)

        psi_merged.drop(columns=['对照时间','创建时间'],inplace=True)
        psi_merged=psi_merged.dropna(subset='时间').set_index('时间')
        psi=pd.concat([psi_merged,psi],axis=1)
    return psi


###定义特征分布函数distribution
def count_distribution(col,merged_data):
    distribution = pd.DataFrame()
    merged = merged_data.copy()
    merged = merged.rename(columns={col:'箱体'})
    a=merged.pivot(index='箱体', columns='创建时间', values='当月人数占比').T
    a=a.sort_index(ascending=False).T.reset_index()
    a.insert(0, '字段名',[col]*len(a))
    if distribution.shape[0]==0:distribution=a
    else:distribution=pd.concat([a,distribution],axis=0)
    return distribution


### 定义计算有效性函数validation
def count_validation(col,merged_data):
    ##只计算当月的分箱结果
    global breaks
    validation = pd.DataFrame()
    tempt_data=merged_data.copy()
#     current_month_start = pd.Timestamp(datetime.now().replace(day=1).strftime('%Y-%m-%d'))
#     tempt_data=tempt_data[tempt_data['创建时间']>=current_month_start]
    idx = tempt_data.groupby('id_no')['创建时间'].idxmax()
    tempt_data=tempt_data.loc[idx]
    tempt_data['overdue_flag']=tempt_data['overdue_flag'].astype('int').fillna(-1)
    a=sc.woebin(tempt_data[[col,'overdue_flag']],y='overdue_flag',breaks_list=breaks)
    a=a[col]
    if validation.shape[0]==0:validation=a
    else:validation=pd.concat([a,validation],axis=0)
    return validation

def total_count(series):
    return len(series)

def missing_count(series):
    series.replace(-1,np.nan,inplace=True)
    return series.isna().sum()

def missing_rate(series):
    # 计算缺失值的数量
    missing = missing_count(series)
    total = series.shape[0]
    # 计算缺失率
    rate = missing / total
    return rate

def count_missing(col,merged_data):
    missing = pd.DataFrame()
    tempt_data=merged_data.copy()
    tempt_data['创建时间']=pd.to_datetime(tempt_data['创建时间']).dt.to_period('M')
    a=tempt_data[[col,'创建时间']].groupby('创建时间').agg({
        col:[total_count,missing_count,missing_rate]
    })
    a.columns=['总样本数','缺失值','缺失率']
    a.insert(0, '字段名',[col]*len(a))
    if missing.shape[0]==0:missing=a
    else:missing=pd.concat([a,missing],axis=0)
    missing=missing.sort_values(by=['字段名','创建时间'],ascending=[False,False])
    return missing


def operator(col,table,merged_data):
    global  breaks
    print("当前表格为:", table, ",当前字段为:", col)
    tempt_data = merged_data[['姓名', col, '创建时间']].dropna(subset=col)
    tempt_data['创建时间'] = pd.to_datetime(tempt_data['创建时间']).dt.to_period('M')
    tempt_data[col] = pd.cut(tempt_data[col], bins=breaks[col])

    a = tempt_data.groupby(['创建时间', col]).agg({'姓名': 'count'}).reset_index()
    b = tempt_data.groupby(['创建时间']).agg({'姓名': 'count'}).reset_index()

    data_merge = pd.merge(a, b, on='创建时间')
    data_merge['当月人数占比'] = data_merge['姓名_x'] / data_merge['姓名_y']

    before_merge = data_merge.copy()
    before_merge['创建时间'] = data_merge['创建时间'].add(pd.offsets.MonthEnd(1))

    before_merge = before_merge.rename(columns={'当月人数占比': '上月人数占比'})
    merged = pd.merge(data_merge[['创建时间', col, '当月人数占比']], before_merge[['创建时间', col, '上月人数占比']],
                      on=['创建时间', col])
    psi = count_psi(col, merged)
    distribution = count_distribution(col, merged)
    missing = count_missing(col, merged_data)
    validation = count_validation(col, merged_data)
    return psi, distribution, missing,validation


def calculate_change(col, tempt_data):
    colname = col + '_绝对值变动'
    tempt_data[col] = tempt_data[col].fillna(998)
    tempt_data[colname] = tempt_data[col] - tempt_data[col].shift(1)
    tempt_data[colname] = tempt_data[colname].fillna(999)
    return tempt_data[['user_name_id', '创建时间', colname]]


def calculate_pct_change(col, tempt_data):
    colname = col + '_变动百分比'
    tempt_data[col] = tempt_data[col].fillna(998)
    tempt_data[colname] = tempt_data[col].pct_change() * 100
    tempt_data[colname] = tempt_data[colname].fillna(999)
    return tempt_data[['user_name_id', '创建时间', colname]]


def count_reshuffle(col, tempt_data):
    global type_dic
    tempt_data = tempt_data[['user_name_id', '创建时间', col]].sort_values(by=['创建时间', 'user_name_id'],
                                                                           ascending=[True, False])
    tempt_data = tempt_data.reset_index(drop=True)

    if type_dic[col] == 1:
        result = tempt_data.groupby('user_name_id').apply(lambda x: calculate_change(col, x)).reset_index(drop=True)
    elif type_dic[col] == 2:
        result = tempt_data.groupby('user_name_id').apply(lambda x: calculate_pct_change(col, x)).reset_index(drop=True)
    return result

#############################################特殊名单报表######################################
def list_monitor(user_type,user_id,current_time):
    global specialist_dict
    risk_monitor = pd.DataFrame()
    os.makedirs(f'data/result/监测结果/{user_type}', exist_ok=True)
    with pd.ExcelWriter(f'data/result/监测结果/{user_type}/{current_time}/个体名单异动监测.xlsx') as writer:
        for item in ['特殊名单']:
            print(item)
            data = pd.read_excel(f'data/朴道数据/{item}.xlsx')
            data['创建时间'] = pd.to_datetime(data['创建时间']).dt.to_period('M')
            data = pd.merge(data, user_id, left_on='身份证', right_on='id_no', how='right').drop_duplicates(
                subset=['身份证', '创建时间'])
            for col in specialist_dict[item]:
                print('当前字段为:', col)
                tempt_data = data[[col, '创建时间', 'user_name_id']]
                tempt_data = tempt_data[tempt_data[col] > 0]
                result = tempt_data.groupby('创建时间')['user_name_id'].apply(
                            lambda x: '|'.join(x.dropna().astype(str))).rename(col)
                result = pd.DataFrame(result)
                if risk_monitor.shape[0] == 0:
                    risk_monitor = result
                else:
                    risk_monitor = pd.merge(risk_monitor, result, on='创建时间', how='outer')
            risk_monitor=risk_monitor.sort_index(ascending=False)
            risk_monitor.to_excel(writer, sheet_name=f'{item}名单异动', index=True)


####################################3###个人特征异动报表##############################
def calculate_change(col, tempt_data):
    colname = col + '_绝对值变动'
    tempt_data[col] = tempt_data[col].fillna(998)
    tempt_data[colname] = tempt_data[col] - tempt_data[col].shift(1)
    tempt_data[colname] = tempt_data[colname].fillna(999)
    return tempt_data[['user_name_id', '创建时间', colname]]


def calculate_pct_change(col, tempt_data):
    colname = col + '_变动百分比'
    tempt_data[col] = tempt_data[col].fillna(998)
    tempt_data[colname] = tempt_data[col].pct_change() * 100
    tempt_data[colname] = tempt_data[colname].fillna(999)
    return tempt_data[['user_name_id', '创建时间', colname]]


def count_reshuffle(col, tempt_data):
    global type_dic
    tempt_data = tempt_data[['user_name_id', '创建时间', col]].sort_values(by=['创建时间', 'user_name_id'],
                                                                           ascending=[True, False])
    tempt_data = tempt_data.reset_index(drop=True)
    if type_dic[col] == 1:
        result = tempt_data.groupby('user_name_id').apply(lambda x: calculate_change(col, x)).reset_index(drop=True)
    elif type_dic[col] == 2:
        result = tempt_data.groupby('user_name_id').apply(lambda x: calculate_pct_change(col, x)).reset_index(drop=True)
    else :print(type_dic[col])
    return pd.DataFrame(result)

def personal_stats(user_type,user_id,current_time):
    global missing_dict
    concatent=pd.DataFrame()
    with pd.ExcelWriter(f'data/result/监测结果/{user_type}/{current_time}/个体特征异动监测.xlsx') as writer:
        for table in missing_dict.keys():
            reshffle=pd.DataFrame()
            data=pd.read_excel(f'data/朴道数据/{table+".xlsx"}')
            # user_id=pd.read_excel(f'data/user_info.xlsx', sheet_name=user_type)
            data['创建时间']=pd.to_datetime(data['创建时间']).dt.to_period('M')
            data=pd.merge(data,user_id,left_on='身份证',right_on='id_no',how='inner').drop_duplicates(subset=['身份证','创建时间'])
            values=missing_dict[table]
            if type(values)==list:
                for value in values:
                    print(f'当前表格为:{table},字段为:{value}')
                    a=count_reshuffle(value,data)
                    if reshffle.shape[0]==0:reshffle=a
                    else:reshffle=pd.merge(a,reshffle,on=['user_name_id','创建时间'],how='outer')
            else:
                print(f'当前表格为:{table},字段为:{values}')
                a=count_reshuffle(values,data)
                if reshffle.shape[0]==0:reshffle=a
                else:reshffle=pd.merge(a,reshffle,on=['user_name_id','创建时间'],how='outer')
            reshffle.drop_duplicates(inplace=True)
            reshffle=reshffle.sort_values(by=['创建时间','user_name_id'],ascending=[False,True])
            if concatent.shape[0]==0:concatent=reshffle
            else:concatent=pd.merge(reshffle,concatent,on=['user_name_id','创建时间'],how='outer')
            reshffle.to_excel(writer,sheet_name=f'{table}变动情况',index=False)
        concatent.to_excel(writer,sheet_name='个人特征异动汇总',index=False)

#######################################客群特征异动####################################
def crowd_stats(user_type,user_id,current_time):
    psi=pd.DataFrame()
    distribution=pd.DataFrame()
    missing=pd.DataFrame()
    validation=pd.DataFrame()
    for table in col_in_table.keys():
        data=pd.read_excel(f'data/朴道数据/{table}.xlsx')
        ##只保留电商贷客户信息,并去除重复项
        merged_data=pd.merge(data,user_id,left_on='身份证',right_on='id_no',how='inner').drop_duplicates('身份证')
        values=col_in_table[table]
        if type(values)==list:
            for value in values:
                print("当前处理特征为：",value)
                t_psi, t_distribution, t_missing, t_validation=operator(value,table,merged_data)
                if psi.shape[0]==0:
                    psi=t_psi
                    distribution=t_distribution
                    missing=t_missing
                    validation=t_validation
                else:
                    psi=pd.merge(psi,t_psi,on='时间')
                    distribution=pd.concat([distribution,t_distribution],axis=0)
                    missing=pd.concat([missing,t_missing],axis=0)
                    validation=pd.concat([validation,t_validation],axis=0)
        else:
            print("当前处理特征为：",values)
            t_psi, t_distribution, t_missing, t_validation=operator(values,table,merged_data)
            if psi.shape[0]==0:
                psi=t_psi
                distribution=t_distribution
                missing=t_missing
                validation=t_validation
            else:
                psi=pd.merge(psi,t_psi,on='时间')
                distribution=pd.concat([distribution,t_distribution],axis=0)
                missing=pd.concat([missing,t_missing],axis=0)
                validation = pd.concat([validation, t_validation], axis=0)
    with pd.ExcelWriter(f'data/result/监测结果/{user_type}/{current_time}/客群特征异动监测.xlsx') as writer:
        psi.to_excel(writer, sheet_name='psi')
        distribution.to_excel(writer, sheet_name='特征分布', index=False)
        validation.to_excel(writer, sheet_name='特征有效性', index=False)
        missing.to_excel(writer, sheet_name='特征覆盖率')

