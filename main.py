import tools
import rules
import catch_data
import os
import datetime
import pandas as pd
import numpy as np
import monitor

if __name__ == '__main__':
    if input('==========是否获取朴道数据?(y/n):')=='y':
        catch_data.get_data()##刷新所有朴道数据
    merged_data=tools.merge_dataframes(path=None)
    os.makedirs('/data/result', exist_ok=True)
    merged_data.to_excel('data/result/merged_data.xlsx',index=False)
    # print('反欺诈策略运行中................')
    strategy_result = rules.strategy(merged_data,key='身份证')##查看命中的指标个数
    # strategy_result.to_excel('result/strategy.xlsx',index=False)
    print('计算客户的评分..................')
    final_score=rules.data_to_score(merged_data,key='身份证')##计算最终评分，并返回评分细项
    # print(final_score)
    merged_data=pd.merge(merged_data,strategy_result[['身份证','命中次数']],on='身份证',how='left')
    merged_data=pd.merge(merged_data,final_score[['主键','最终评分']],left_on='身份证',right_on='主键',how='outer')##将评分合并到原始的merged数据
    merged_data['轩辕现金贷均值']=np.mean(merged_data[['轩辕分','现金贷分']],axis=1)
    merged_data['授予额度']=rules.compute_credit_limit(merged_data['最终评分'],merged_data['轩辕现金贷均值'],merged_data['命中次数'],flag='inner')
    print('正在保存结果.................')
    # final_score.to_excel('result/scorecard.xlsx',index=False)
    current_date=datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    with pd.ExcelWriter(f'data/result/准入结果/{current_date}.xlsx') as writer:
        merged_data.to_excel(writer,sheet_name='merged_data',index=True)
        strategy_result.to_excel(writer,sheet_name='strategy_result',index=True)
        final_score.to_excel(writer,sheet_name='final_score',index=True)

    if input('是否进行数据监测?(y/n):')=='y':
        current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        for user_type in ['eloan_user_code','xw_user_code']:
            os.makedirs(f'data/result/监测结果/{user_type}/{current_time}', exist_ok=True)
            user_id = pd.read_excel(f'data/user_info.xlsx', sheet_name=user_type)
            monitor.list_monitor(user_type, user_id, current_time)
            monitor.personal_stats(user_type, user_id,current_time)
            monitor.crowd_stats(user_type, user_id,current_time)




