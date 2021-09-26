#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from datetime import datetime
import time
import baostock as bs
import pandas as pd

def query_stock_basic(code=None, code_name=None):
    """
    证券基本资料
    方法说明：获取证券基本资料，可以通过参数设置获取对应证券代码、证券名称的数据。
    返回类型：pandas的DataFrame类型。
    :param code:A股股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。可以为空；
    :param code_name:股票名称，支持模糊查询，可以为空。
    """
    lg = bs.login()

    rs = bs.query_stock_basic(code=code, code_name=code_name)


    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)

    bs.logout()
    return result

def query_history_k_data_plus(symbol, timeframe, adj=None, start_date=None, end_date=None):
    """
    获取k线数据
    注意：
        股票停牌时，对于日线，开、高、低、收价都相同，且都为前一交易日的收盘价，成交量、成交额为0，换手率为空。
    :param symbol:股票代码，sh或sz+6位数字代码，或者指数代码，如：sh601398。sh：上海；sz：深圳。此参数不可为空；
    :param timeframe:k线周期，"5m"为5分钟，"15m"为15分钟，"30m"为30分钟，"1h"为1小时，"1d"为日，"1w"为一周，"1M"为一月。指数没有分钟线数据；周线每周最后一个交易日才可以获取，月线每月最后一个交易日才可以获取。
    :param adj:复权类型，默认是"3"不复权；前复权:"2"；后复权:"1"。已支持分钟线、日线、周线、月线前后复权。 BaoStock提供的是涨跌幅复权算法复权因子，具体介绍见：复权因子简介或者BaoStock复权因子简介。
    :param start_date:开始日期（包含），格式“YYYY-MM-DD”，为空时取2015-01-01；
    :param end_date:结束日期（包含），格式“YYYY-MM-DD”，为空时取最近一个交易日；
    :return:返回一个列表
    """
    frequency = ''
    if timeframe == "5m":
        frequency = "5"
    elif timeframe == "15m":
        frequency = "15"
    elif timeframe == "30m":
        frequency = "30"
    elif timeframe == "1h":
        frequency = "60"
    elif timeframe == "1d":
        frequency = "d"
    elif timeframe == "1w":
        frequency = 'w'
    elif timeframe == "1M":
        frequency = "m"

    fields = ''
    if 'm' in timeframe or 'h' in timeframe:
        fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"
    elif "d" in timeframe:
        fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST,peTTM,pbMRQ,psTTM,pcfNcfTTM"
    elif 'w' in timeframe or 'M' in timeframe:
        fields = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"

    stock_name = symbol
    adjust_flag = "3" if not adj else adj

    lg = bs.login()

    rs = bs.query_history_k_data_plus(
        code=stock_name,
        fields=fields,
        start_date=start_date,
        end_date=end_date,
        frequency=frequency,
        adjustflag=adjust_flag
    )
    if rs is not None:
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
            global result
            result = pd.DataFrame(data_list, columns=rs.fields)
    bs.logout()
    return result


def crawl(begin_date=None,end_date=None,code=None,indexs=None):
    '''抓取股票的日K数据，主要包含了不复权和后复权'''

    #通过tushare的基本信息API，获取所有股票的基本信息
    stock_df=query_stock_basic(code)
    #将基本信息的索引列表转为股票代码列表
    codes=list(stock_df["code"])

    #当前日期
    now=datetime.now().strftime("%Y-%m-%d")

    #如果没有指定开始日期，则默认为2018-01-01
    if begin_date is None:
        begin_date="2018-01-01"

    #如果没有指定结束日期，则默认为当前日期
    if end_date is None:
        end_date=now

    if indexs is None:
        indexs=0

    for n, code in enumerate(codes[indexs:]):
        #抓取后复权的价格
        df=query_history_k_data_plus(code,"1d",adj='1',start_date=begin_date,end_date=end_date)
        if df is not None:
            df.to_csv("D:\sjq\{}.csv".format(n))
            print(n)
            
crawl("2020-01-01","2020-12-31")


df_01=pd.read_csv(r"D:\sjq\0.csv",index_col=0)
number=5227
for i in range(1,2279):
    a=pd.read_csv(r"D:\sjq\{}.csv".format(i),index_col=0)
    df_01=pd.concat([df_01,a])
print(df_01.shape)

df_02=pd.read_csv(r"D:\sjq\2279.csv",index_col=0)
for i in range(2279,5227):
    a=pd.read_csv(r"D:\sjq\{}.csv".format(i),index_col=0)
    df_02=pd.concat([df_02,a])
print(df_02.shape)


df_01["index"]=True
df_02["index"]=True

df_01.to_csv("D:\sjq\df_01.csv",index=False)
df_02.to_csv("D:\sjq\df_02.csv",index=False)


# In[ ]:




