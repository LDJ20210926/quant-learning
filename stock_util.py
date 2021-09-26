#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pymongo import ASCENDING
from database import DB_CONN
from datetime import datetime,timedelta


# In[20]:


def get_trading_dates(begin_date=None,end_date=None):
    '''
    获取指定日期范围的按照正序排列的交易日列表
    如果没有指定范围，则获取从当前交易日向前365个自然日内的所有交易日
    '''
    #当前日期
    now=datetime.now()
    #开始日期，默认今天向前的365个自然日
    if begin_date is None:
        #当前日期减去365天
        one_year_ago=now- timedelta(days=365)
        #转为str类型
        begin_date=one_year_ago.strftime("%Y-%m-%d")
        
    if end_date is None:
        end_date=now.strftime("%Y-%m-%d")
        
    #用上证综指000001作为查询条件，因为指数是不会停牌的，所以可以查询到所有的交易日
    daily_cursor=DB_CONN.daily_hfq.find(
        {"code":"sh.000001","date":{"$gte":begin_date,"$lte":end_date},"index":True},
        sort=[("date",ASCENDING)],
        projection={"date":True,"_id":False}
    )
    #转换为日期列表
    dates=[x["date"] for x in daily_cursor]
    
    return dates

def get_all_codes():
    '''获取股票代码'''
    
    return DB_CONN.daily_hfq.distinct("code")


# In[10]:


get_all_codes()


# In[ ]:




