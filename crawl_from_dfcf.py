#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import urllib3
from database import DB_CONN
from stock_util import get_all_codes
import baostock as bs
import pandas as pd
import os
user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"

def crawl_finance_report():
    #现获取所有的股票列表
    codes=get_all_codes()
    code_list=[]
    for code in codes:
        recode=code.split("sh.")[1] if code.startswith("sh.") else code.split("sz.")[1] 
        code_list.append(recode)
    
    #创建连接池
    conn_pool=urllib3.PoolManager()
    
    #抓取的财务地址
    url="http://datacenter-web.eastmoney.com/api/data/get?st=REPORTDATE&sr=-1&ps=50&p=1&sty=ALL&filter=(SECURITY_CODE%3D%22300300%22)&token=894050c76af8597a853f5b408b759f5d&type=RPT_LICO_FN_CPD"
    
    for i,code in enumerate(code_list):
        #替换股票代码，抓取该只股票的财务信息
        response=conn_pool.request("GET",url.replace("300300",str(code)))
        #解析抓取结果
        result=json.loads(response.data.decode("utf-8"))
        
        #取出数据
        if result["result"] is not None:
            content=result["result"]["data"]
            lists=[]
            for row in content:
                doc={
                    #报告期
                    "report_data": row["REPORTDATE"][0:10],
                    #公告日期
                    "announced_data": row["NOTICE_DATE"][0:10] if row["NOTICE_DATE"] is not None else "-" ,
                    #每股收益
                    "eps": "-" if row["BASIC_EPS"] is None else row["BASIC_EPS"],

                    "code": str("sh.")+row["SECUCODE"].split(".SH")[0] if row["SECUCODE"].endswith(".SH") else str("sz.")+row["SECUCODE"].split(".SZ")[0]
                }
                lists.append(doc)
            write_list_to_json(lists,"{}.json".format(i),r"D:\dfcf")
            print(i)
    #最后合并成一个json
    outcome=megered(len(code_list))
    write_list_to_json(outcome,"outcome.json",r"D:\dfcf")
    
def write_list_to_json(list, json_file_name, json_file_save_path):
    """
    将list写入到json文件
    :param list:
    :param json_file_name: 写入的json文件名字
    :param json_file_save_path: json文件存储路径
    :return:
    """
    os.chdir(json_file_save_path)
    with open(json_file_name, 'w') as  f:
        json.dump(list, f)          

def megered(num):
    with open(r"D:\dfcf\0.json","r") as f:
        a=json.load(f)

    for i in range(1,num):
        if os.path.exists(r"D:\dfcf\{}.json".format(i)) is True:
            with open(r"D:\dfcf\{}.json".format(i),"r") as g:
                b=json.load(g)
                a+=b
        print(i)
    return a


# In[ ]:




