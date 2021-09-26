#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pymongo import ASCENDING,DESCENDING
import pandas as pd
import matplotlib.pyplot as plt
from database import DB_CONN
from stock_util import get_trading_dates

'''
实现股票池，条件是0<PE<30,按照PE正序排列，最多取出100只股票
再平衡周期为7个交易日
'''



    

daily=DB_CONN["daily"]
daily_hfq=DB_CONN["daily_hfq"]


def stock_pool(begin_date,end_date,adjust_interval=None,pe_range=None):
    '''
    股票池选股逻辑
    参数： begin_date:
           end_date;
    return: 所有调整日，以及调整日和代码对应的dict
    '''
    if adjust_interval is None:
        adjust_interval=7
        
    if pe_range is None:
        pe_range=[0,30]


    # PE的排序方式，ASCENDING 从小到大，DESCENDING 从大到小
    sort=ASCENDING
    #股票池内的股票数量
    pool_size=100

    #返回值：调整日和当期股票代表列表
    adjust_date_codes_dict=dict()
    #返回值，所有的调整日列表
    all_adjust_dates=[]

    #获取指定时间范围内的所有交易日列表，按照日期正序排列
    all_dates=get_trading_dates(begin_date=begin_date,end_date=end_date)

    #上一期的所有股票代码
    last_phase_codes=[]
    #在调整日调整股票池
    for _index in range(0,len(all_dates),adjust_interval):
        #保存调整日
        adjust_date=all_dates[_index]
        all_adjust_dates.append(adjust_date)

        print("调整日期：%s"% adjust_date, flush=True)

        #查询出调整当日，0<pe<30,且非停牌的股票
        #最重要的一点是，按照pe进行排序，只取前100只股票
        daily_cursor=daily.find(
            {"date":adjust_date,"peTTM":{"$lt":pe_range[1],"$gt":pe_range[0]},"tradestatus":"1"},sort=[("peTTM",sort)],
            projection={"code":True},
            limit=pool_size
        )

        #拿到所有的股票代码
        codes=[x["code"] for x in daily_cursor]

        #本期股票列表
        this_phase_codes=[]
        #如果上期股票代码列表不为空，则查询出上次股票池中正在停牌的股票
        if len(last_phase_codes)>0:
            suspension_cursor=daily.find(
                #查询是股票代码，日期和是否为交易，这里tradestatus是"0"
                {"code":{"$in":last_phase_codes},"date":adjust_date,"tradestatus":"0"},
                #只需要使用股票代码
                projection={"code":True}
            )
            #拿到股票代码
            suspension_codes=[x["code"] for x in suspension_cursor]

            #保留股票池中正在停牌的股票代码
            this_phase_codes=suspension_codes
        print("上期停牌",flush=True)
        print(this_phase_codes,flush=True)

        #用新的股票将剩余位置补齐
        this_phase_codes+=codes[0:pool_size-len(this_phase_codes)]

        #建立该调整日和股票列表的对应关系
        adjust_date_codes_dict[adjust_date]=this_phase_codes

        print("最终出票",flush=True)
        print(this_phase_codes,flush=True)

    #返回结果
    return all_adjust_dates, adjust_date_codes_dict


def find_out_stocks(last_phase_codes,this_phase_codes):
    '''
    找到上期入选本期被调出的股票，这些股票将必须卖出
    last_phase_codes:上期的股票列表
    this_phase_codes：本期的股票列表
    return: 被调出的股票列表
    '''
    out_stocks=[]

    for code in last_phase_codes:
        if code not in this_phase_codes:
            out_stocks.append(code)

    return out_stocks


def statistic_stock_pool(begin_date,end_date,adjust_interval=None,pe_range=None):
    '''统计股票池的收益'''

    #找到指定时间范围内的股票池数据，这里的时间范围可以改变
    adjust_dates,codes_dict=stock_pool(begin_date,end_date,adjust_interval,pe_range)

    #用DataFrame保存收益，profit是股票池收益，sh.000300是沪深300指数，hs300是用来对比的沪深300的涨跌幅
    df_profit=pd.DataFrame(columns=["profit","hs300"])

    #统计开始的第一天，股票池的收益和沪深300的涨跌幅都是0
    df_profit.loc[adjust_dates[0]]={"profit":0,"hs300":0}

    #找到沪深300第一天的值，后面的累计涨跌幅都是与之比较
    hs300_begin_value=daily.find_one({"code":"sh.000300","index":True,"date":adjust_dates[0]})["close"]

    '''
    通过净值的方式计算累计收益：
    累计收益=期末净值-1
    第N期净值的计算方法：
    net_value(n)=net_value(n-1)+net_value(n-1)*profit(n)
    '''
    #设定初始净值为1
    net_value=1
    #在所有调整日上统计收益，循环从1开始，因为每次计算都要用到当期和上期
    for _index in range(1,len(adjust_dates)-1):
        #上一期的调整日
        last_adjust_date=adjust_dates[_index-1]
        #当期的调整日
        current_adjust_date=adjust_dates[_index]

        #上一期的股票代码列表
        codes=codes_dict[last_adjust_date]

        #构建股票代码和后复权买入价格的股票
        buy_daily_cursor=daily_hfq.find(
            {"code":{"$in":codes},"date":last_adjust_date},
            projection={"close":True,"code":True}
        )
        code_buy_close_dict=dict([(buy_daily["code"],buy_daily["close"]) for buy_daily in buy_daily_cursor])

        '''
        找到上期股票在当前调整日的收盘价
        这里用的是后复权的价格，保持价格连续性
        当前的调整日，也就是上期的结束日
        '''
        sell_daily_cursor=daily_hfq.find(
            {"code":{"$in":codes},"date":current_adjust_date},
            #只需要用到收盘价来计算收益
            projection={"close":True,"code":True}
        )

        #初始化所有股票的收益之和
        profit_sum=0
        #参与收益统计的股票数量
        count=0
        #循坏累加所有股票的收益
        for sell_daily in sell_daily_cursor:
            #股票代码
            code=sell_daily["code"]

            #如果该股票存在股票池开始时的收盘价，则参与收益统计
            if code in code_buy_close_dict:
                #选入股票池时的价格
                buy_close=code_buy_close_dict[code]
                #当前的价格
                sell_close=sell_daily["close"]
                #累加所有股票的收益
                profit_sum+=(sell_close-buy_close)/buy_close

                #参数计算的股票数加1
                count +=1

        #如果股票数量大于0，才统计当期收益
        if count>0:
            #计算平均收益
            profit=round(profit_sum/count,4)

            #当前沪深300的值
            hs300_close=daily.find_one(
                {"code":"sh.000300","index":True,"date":current_adjust_date}
            )["close"]

            #计算净值和累计收益放到DataFrame中
            net_value=net_value*(1+profit)
            df_profit.loc[current_adjust_date]={
                #乘以100，改为百分比
                "profit":round((net_value-1)*100,4),
                #乘以100，改百分比
                "hs300":round((hs300_close-hs300_begin_value)*100/hs300_begin_value,4)
            }

    #作图
    df_profit.plot(title="Stock Pool Evaluation Result",kind="line",figsize=[20,8])
    plt.show()
        
   




