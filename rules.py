import pandas as pd
import numpy as np
from catch_data import *
def strategy(data,key='身份证'):
    '''
    该函数为反欺诈规则配置函数
    :param data:外部数据合并宽表
    :param key: 用于匹配客户的主键，身份证、姓名、手机或用户编码
    :return: 是否命中某条规则，每个人命中规则总数
    '''
    global specialist_dict
    result=pd.DataFrame()
    result[key]=data[key]
    try:
        result['12月内手机号码申请命中互联网金融平台数']=data['12月内手机号码申请命中互联网金融平台数']>=6
        result['共债等级[1,10]']=data['共债等级[1,10]']>=7
        result['最近 30~60 天查询非银机构数']=(data['最近 60 天查询非银机构数']-data['最近 30 天查询非银机构数'])>=3
        result['身份证号查询，近15天通过非银申请的次数']=data['身份证号查询，近15天通过非银申请的次数']>=5
        result['身份证号查询，近1个月通过非银周末申请的业务线数量']=data['身份证号查询，近1个月通过非银周末申请的业务线数量']>=4
        result['身份证号查询，近3个月通过非银周末申请的业务线数量']=data['身份证号查询，近3个月通过非银周末申请的业务线数量']>=8
        result['圈团2风险等级V1']=data['圈团2风险等级V1']>=3
        result['[0,6]等级越高近3月非免息的信用消费的金额越高']=data['[0,6]等级越高近3月非免息的信用消费的金额越高']>=4
        result['斑马扩散风险等级']=data['斑马扩散风险等级']>=5
        result['关系网络平均风险等级']=data['关系网络平均风险等级']>=5
        result['[0,5]等级越高历史180天内逾期天数越久，信用等级越差']=data['[0,5]等级越高历史180天内逾期天数越久，信用等级越差']>=3
        result['[0,5]等级越高历史180天内逾期产品越多，履约等级越差']=data['[0,5]等级越高历史180天内逾期产品越多，履约等级越差']>=3
        result['[0,5]等级越高历史30天内逾期产品越多，履约等级越差']=data['[0,5]等级越高历史30天内逾期产品越多，履约等级越差']>=5
        result['[0,5]等级越高历史30天内逾期天数越久，信用等级越差']=data['[0,5]等级越高历史30天内逾期天数越久，信用等级越差']>=3
        result['[0,5]等级越高历史365天内逾期产品越多，履约等级越差']=data['[0,5]等级越高历史365天内逾期产品越多，履约等级越差']>=3
        result['[0,5]等级越高历史365天内逾期天数越久，信用等级越差']=data['[0,5]等级越高历史365天内逾期天数越久，信用等级越差']>=4
        result['[0,5]等级越高历史60天内逾期产品越多，履约等级越差']=data['[0,5]等级越高历史60天内逾期产品越多，履约等级越差']>=2
        result['[0,5]等级越高历史60天内逾期天数越久，信用等级越差']=data['[0,5]等级越高历史60天内逾期天数越久，信用等级越差']>=3
        result['[0,5]等级越高历史90天内逾期产品越多，履约等级越差']=data['[0,5]等级越高历史90天内逾期产品越多，履约等级越差']>=2
        result['[0,5]等级越高历史90天内逾期天数越久，信用等级越差']=data['[0,5]等级越高历史90天内逾期天数越久，信用等级越差']>=3
        result['反欺诈分']=data['反欺诈分']>80
        data['在网状态']=data['在网状态'].fillna(-1)
        result['在网状态']=(data['在网状态']!=1)&(data['在网状态']!=-1)
        result['在网时长']=(data['在网时长']<5)&(data['在网时长']!=-1)
        result['轩辕分']=data['轩辕分']<550
        result['现金贷分']=data['现金贷分']<510
        result['现金贷&轩辕分']=(data['现金贷分']<600)&(data['轩辕分']<600)
        cols=specialist_dict['特殊名单']
    except Exception as e:
        print(e)
        pass
    for col in data.columns:
        try:
            if col in cols:result[col]=data[col]>0
        except :pass
    result=result.replace(False,0)
    result=result.replace(True,1)
    result.insert(1,'命中次数',result.iloc[:,1:].sum(axis=1))
    
    return result

def data_to_score(data,key='身份证'):
    '''
    将合并完成的数据通过评分卡文件转为分数
    :param data:merged_data
    :param col:主键
    :return:返回df：模型卡特征对应的分数
    '''
    global card
    data=data.fillna(-1)
    final_score=pd.DataFrame()
    final_score['主键']=data[key]
    for target_col in data.columns:
        if target_col in card.variable.tolist():
            print(target_col)
            current_score=[]
            test_card=card[card.variable==target_col].reset_index(drop=True)
            for j in range(len(data)):
                score=data[target_col][j]
                for i in range(len(test_card)):
                    if score>=test_card.lower_bound[i] and score<test_card.upper_bound[i]:
                        current_score.append(test_card.points[i])
            final_score[target_col]=current_score
    final_score['最终评分']=final_score.iloc[:,1:].sum(axis=1)
    return final_score

def compute_credit_limit(score, repay_level, hit_count,flag='inner'):
    '''
    用于计算客户的额度
    :param score: dataframe:评分列评分卡评分或者现金贷与轩辕分均值
    :param repay_level: Series:还款能力等级
    :param flag: 内部客户还是外部客户
    :return: 返回额度list
    '''
    credit_limit = []
    if flag == 'inner':
        for i in range(len(score)):
            if score[i] > 700 and hit_count[i] ==0:
                if repay_level[i] > 700:
                    credit_limit.append(200000)
                elif repay_level[i] > 650:
                    credit_limit.append(150000)
                elif repay_level[i] > 600:
                    credit_limit.append(100000)
                elif repay_level[i] > 550:
                    credit_limit.append(50000)
            #                 elif repay_level[i]>0:credit_limit.append(50000)
                else:credit_limit.append(np.nan)

            elif score[i] > 650 and hit_count[i] ==0:
                if repay_level[i] > 700:
                    credit_limit.append(83300)
                elif repay_level[i] > 650:
                    credit_limit.append(62500)
                elif repay_level[i] > 600:
                    credit_limit.append(41600)
                elif repay_level[i] > 550:
                    credit_limit.append(20800)
            #                 elif repay_level[i]>0:credit_limit.append(20800)
                else:credit_limit.append(np.nan)

            elif score[i] > 600 and hit_count[i] ==0:
                if repay_level[i] > 700:
                    credit_limit.append(29800)
                elif repay_level[i] > 650:
                    credit_limit.append(22300)
                elif repay_level[i] > 600:
                    credit_limit.append(14900)
                elif repay_level[i] > 550:
                    credit_limit.append(7400)
            #                 elif repay_level[i]>0:credit_limit.append(7400)
                else:credit_limit.append(np.nan)

            elif score[i] > 550 and hit_count[i] ==0:
                if repay_level[i] > 700:
                    credit_limit.append(22200)
                elif repay_level[i] > 650:
                    credit_limit.append(16600)
                elif repay_level[i] > 600:
                    credit_limit.append(11100)
                elif repay_level[i] > 550:
                    credit_limit.append(5500)
            #                 elif repay_level[i]>0:credit_limit.append(5500)
                else:credit_limit.append(np.nan)

            else:
                credit_limit.append(np.nan)

    if flag == 'outer':
        for i in range(len(score)):
            if score[i] > 700:
                if repay_level[i] > 700:
                    credit_limit.append(100000)
                elif repay_level[i] > 650:
                    credit_limit.append(75000)
                elif repay_level[i] > 600:
                    credit_limit.append(50000)
                elif repay_level[i] > 550:
                    credit_limit.append(25000)
            #                 elif repay_level[i]>0:credit_limit.append(25000)
                else:credit_limit.append(np.nan)

            elif score[i] > 650:
                if repay_level[i] > 700:
                    credit_limit.append(41600)
                elif repay_level[i] > 650:
                    credit_limit.append(31200)
                elif repay_level[i] > 600:
                    credit_limit.append(20800)
                elif repay_level[i] > 550:
                    credit_limit.append(10400)
            #                 elif repay_level[i]>0:credit_limit.append(10400)
                else:credit_limit.append(np.nan)

            elif score[i] > 600:
                if repay_level[i] > 700:
                    credit_limit.append(14900)
                elif repay_level[i] > 650:
                    credit_limit.append(11100)
                elif repay_level[i] > 600:
                    credit_limit.append(7400)
                elif repay_level[i] > 550:
                    credit_limit.append(3700)
            #                 elif repay_level[i]>0:credit_limit.append(3700)
                else:credit_limit.append(np.nan)

            elif score[i] > 550:
                if repay_level[i] > 700:
                    credit_limit.append(11100)
                elif repay_level[i] > 650:
                    credit_limit.append(8300)
                elif repay_level[i] > 600:
                    credit_limit.append(5500)
                elif repay_level[i] > 550:
                    credit_limit.append(2700)
            #                 elif repay_level[i]>0:credit_limit.append(2700)
                else:credit_limit.append(np.nan)

            else:
                credit_limit.append(np.nan)
    return credit_limit


