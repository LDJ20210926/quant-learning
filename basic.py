#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pymongo import UpdateOne
from database import DB_CONN
import tushare as ts
from datetime import datetime
import time
import baostock as bs
import pandas as pd
import json


# In[ ]:


class DailyCrawler:
    def __init__(self):
        '''初始化'''
        #创建daily数据集
        self.daily_01=DB_CONN["daily_01"]
        #创建daily_hfq数据集
        self.daily_02=DB_CONN["daily_02"]
        
        self.daily=DB_CONN["daily"]
        
    
    @staticmethod
    def get_data(code_name,begin_date,out_date,fr):   
   
        lg = bs.login()

        rs = bs.query_history_k_data_plus(code_name,
            "date,code,open,high,low,close,preclose,volume,amount,pctChg",
            start_date=begin_date,end_date=out_date,frequency=fr)

        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)

        bs.logout()
        return result


    @staticmethod
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
        
    def crawl_index(self,begin_date=None,end_date=None):
        '''
        抓取指数行情的日K数据
        指数行情的日K数据的主要作用：
        1.用来生成交易日历
        2.回测时作为收益的对比基准
        '''
        #指定抓取的指数列表，可以增加和改变列表里的值
        
        
        index_codes=["000001.SH","000300.SH","399001.SZ","399005.SZ","399006.SZ"]
            
        
        #当前日期
        now=datetime.now().strftime("%Y-%m-%d")
        #如果没有指定开始，则默认为2018-01-01
        if begin_date is None:
            begin_date="2018-01-01"
        
        #如果没有指定结束日，则默认为当前日期
        if end_date is None:
            end_date=now
            
        #按照指数的代码，抓取所有信息
        for code in index_codes:
            cod="sh."+str(code).split(".SH")[0] if str(code).endswith(".SH") else "sz."+str(code).split(".SZ")[0]
            #抓取一个指数在时间区间的数据
            print(cod)
            df_daily=self.get_data(cod,begin_date,end_date,"d")
            #保存数据
            self.save_data(code,df_daily,self.daily,{"index":True})
            
    def save_data(self,code,df_daily,collection,extra_fields=None):
        '''将从网上抓取的数据保存在本地MongoDB中
        
        df_daily: 包含日线数据的DataFrame
        collection: 要保存的数据集
        extra_fields:除了k线数据中保存的字段，需要额外保存的字段
        '''
        #数据更新的请求列表
        update_requests=[]
        if df_daily is not None:
            data=json.loads(df_daily.T.to_json()).values()
            
            
            for row in data:
                
                #如果指定了其他字段，则更新dict
                if extra_fields is not None:
                    row.update(extra_fields)

                #生成一条数据库的更新请求
                #注意：
                #需要在code、date、index三个字段上增加索引，否则随着数据增加，写入会变慢
                #创建索引的命令式：
                # db.daily_createIndex({"code":1,"date":1,"index":1})
                update_requests.append(UpdateOne(
                    {"code":row["code"],"date":row["date"],"index":row["index"]},
                    {"$set":row},
                    upsert=True
                ))
            #如果写入的请求列表不为空，则保存在数据库中
            if len(update_requests)>0:
                #批量写入到数据库中，批量写入可以降低网络IO，提高速度
                update_result=collection.bulk_write(update_requests,ordered=False)
                time.sleep(0.1)
                print("保存日线数据，代码：%s,插入：%4d条，更新：%4d条"%
                     (code,update_result.upserted_count,update_result.modified_count),flush=True)

    
    def crawl(self,begin_date=None,end_date=None,code=None,indexs=None):
        '''抓取股票的日K数据，主要包含了不复权和后复权'''
        
        #通过tushare的基本信息API，获取所有股票的基本信息
        stock_df=self.query_stock_basic(code)
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

        for code in codes[indexs:]:
            #抓取后复权的价格
            df_daily_02=self.query_history_k_data_plus(code,"1d",adj='1',start_date=begin_date,end_date=end_date)
            self.save_data(code,df_daily_02,self.daily_02,{"index":False})
         

            #抓取后复权的价格
            #df_daily_hfq=ts.pro_bar(code,adj="hfq",start_date=begin_date,end_date=end_date)
            #self.save_data(code,df_daily_hfq,self.daily_hfq,{"index":False})
          
    @staticmethod
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

        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)

        bs.logout()
        return result


# 赋权是为了让数据可比
# 后复权是第一天不变，后面的变大
# 前赋权当天的价格不变，前面的价格会变小

# In[ ]:


dc=DailyCrawler()


# In[ ]:




