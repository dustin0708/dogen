#-*-coding:utf-8-*-

import sys
import time
import logging
import traceback

from Script import *

POLICY_NAME = "Policy_NewStock"

def policy_analyze_stock(basic, kdata, dparam):
    """
    """
    if (kdata.index.size <= 0):
        logging.debug("No valid trades")
        return None

    ## 初始参数
    from_trade = kdata.index[-1]
    last_trade = kdata.index[ 0]
    
    logging.debug("Trades is from " + from_trade + " to " + last_trade)
    
    ## 判断新股
    date_first = time.strftime("%Y-%m-%d", time.strptime(str(basic.loc['timeToMarket']), "%Y%m%d"))
    if kdata.index[-1] != date_first:
        logging.debug("Don't contain the whole kdata")
        return None
    
    ## 新股特殊处理
    Kdata.drop_fresh_stock_trades(basic, kdata)
    if (kdata.index.size <= 0):
        logging.debug("No valid trades")
        return None

    ## 限制样本数据
    maxi_trade = Comm.GetParamValue(dparam, Comm.PARAM_MAXI_TRADE, 30)
    if (maxi_trade is not None) and (maxi_trade < kdata.index.size):
        logging.debug("It's out of date.")
        return None
        
    return [basic.name, basic[Kdata.NAME], basic[Kdata.INDUSTRY], kdata.index[0], kdata.iloc[0][Kdata.P_CLOSE], basic['outstanding']]

def policy_execute(stocks, dparam, result):
    ## 解析截止交易日
    last_trade = Comm.GetParamValue(dparam, Comm.PARAM_LAST_TRADE)
    from_trade = Comm.GetParamValue(dparam, Comm.PARAM_FROM_TRADE)
    
    ## 加载所有股票信息
    basics = stocks.read_basics()
    if (basics is None):
        logging.error("stocks.read_basics() return none")
        return None
    else:
        Kdata.drop_st_basics(basics)
        Kdata.drop_gem_basics(basics)

    ## 初始化结果表
    result.InitTable(['code', 'name', 'industry', 'take_trade', 'price', 'outstanding'], name=POLICY_NAME)

    logging.info("Analyzing " + str(basics.index.size) + " stocks")

    count = 0
    for code in basics.index:
        
        ## 加载股票K线数据
        basic = basics.loc[code]
        kdata = stocks.read_kdata(basic.name)
        if kdata is None:
            continue
            
        try:
            ## 根据参数选取交易日数据
            if from_trade is not None:
                kdata = kdata[kdata.index >= from_trade]
            
            if last_trade is not None:
                kdata = kdata[kdata.index <= last_trade]
            
            logging.debug("Analyzing stock coded as " + basic.name)
            
            ## 分析K线
            record = policy_analyze_stock(basic, kdata, dparam)
            if record is None:
                continue
            
            ## 日志打印结果
            logging.debug(record)
            
            ## 保存策略结果
            result.AddRecord(record)
        except Exception:
            logging.fatal(traceback.format_exc())
            continue
        
        ## 结果统计
        count += 1
        
    logging.info("Analyzing result in " + str(count) + '/' + str(basics.index.size))

    ## 显示结果
    result.SortValues(['industry'])
    result.Display()

    
def event_handler(stocks, dparam, result):
    """上市新股策略入口函数
    
    stocks  数据库句柄
    dparam  策略参数, 字典形式
    result  保存结果句柄
    """
    try:
        policy_execute(stocks, dparam, result)
    except Exception:
        logging.fatal(traceback.format_exc())
    return
    
## 事件监听适配定义:类型, 回调, 参数
EVENT_TYPE    = Comm.EVT_POLICY_NEWSTOCK
EVENT_HANDLER = event_handler

if __name__ == "__main__":
	print("Welcome to " +  sys.argv[0] + " package.")