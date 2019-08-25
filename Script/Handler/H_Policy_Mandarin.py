#-*-coding:utf-8-*-

import sys
import logging
import traceback

from Script import *

POLICY_NAME = "Policy_Mandarin"


def policy_analyze_drop(basic, kdata, dparam):
    mini_heapup = Comm.GetParamValue(dparam, Comm.PARAM_MINI_HEAP_RISEUP, 5)
    mini_fallen = Comm.GetParamValue(dparam, Comm.PARAM_MINI_HEAP_FALLEN, -4)
    
    temp_kdata = kdata[kdata[Kdata.MA5] < kdata[Kdata.MA20]]
    if temp_kdata.index.size > 0:
        logging.debug("Invalid MA5 & MA20")
        return None
        
    temp__index = 0
    fall_rclose = 0
    fall_volume = 0
    fall_trades = 0
    if (0 == fall_trades) or (kdata.iloc[temp__index][Kdata.R_CLOSE] < 0):
        fall_rclose += kdata.iloc[temp__index][Kdata.R_CLOSE]
        fall_volume += kdata.iloc[temp__index][Kdata.VOLUME]
        fall_trades += 1
    temp__index += 1
    
    if (0 == fall_trades) or (kdata.iloc[temp__index][Kdata.R_CLOSE] < 0):
        fall_rclose += kdata.iloc[temp__index][Kdata.R_CLOSE]
        fall_volume += kdata.iloc[temp__index][Kdata.VOLUME]
        fall_trades += 1
    temp__index += 1
        
    rise_rclose = 0
    rise_volume = 0
    rise_trades = 0
    if kdata.iloc[temp__index][Kdata.R_CLOSE] > 0:
        rise_rclose += kdata.iloc[temp__index][Kdata.R_CLOSE]
        rise_volume += kdata.iloc[temp__index][Kdata.VOLUME]
        rise_trades += 1
    temp__index += 1
    
    if kdata.iloc[temp__index][Kdata.R_CLOSE] > 0:
        rise_rclose += kdata.iloc[temp__index][Kdata.R_CLOSE]
        rise_volume += kdata.iloc[temp__index][Kdata.VOLUME]
        rise_trades += 1
    temp__index += 1
    
    if fall_rclose > mini_fallen:
        logging.debug("Invalid fall trades")
        return None
    
    if rise_rclose < mini_heapup:
        logging.debug("Invalid rise trades")
        return None
    
    if (fall_volume/fall_trades) > (rise_volume/rise_trades):
        logging.debug("Invalid rise/fall volume")
        return None
        
    score = (int)(rise_rclose - fall_rclose)
    
    return [basic.name, basic[Kdata.NAME], basic[Kdata.INDUSTRY], kdata.index[0], kdata.iloc[0][Kdata.P_CLOSE], basic['outstanding'], score]
    
def policy_analyze_stock(basic, kdata, dparam):
    """
    """
    ## 新股特殊处理
    Kdata.drop_fresh_stock_trades(basic, kdata)

    ## 限制样本数据
    maxi_trade = Comm.GetParamValue(dparam, Comm.PARAM_MAXI_TRADE, 22)
    if maxi_trade is not None:
        kdata = kdata[0:maxi_trade]
        
    ## 初始参数
    from_trade = kdata.index[-1]
    last_trade = kdata.index[ 0]

    logging.debug("Trades is from " + from_trade + " to " + last_trade)

    ## 初选, 判断整个区间涨幅在50%以内
    temp_range = Kdata.get_last_rise_range(kdata, 50, 50)
    if temp_range is not None:
        logging.debug("Too large rise range(>50%)")
        return None
        
    ## 限制样本数据
    maxi_trade = Comm.GetParamValue(dparam, Comm.PARAM_MAXI_TRADE, 15)
    if maxi_trade is not None:
        kdata = kdata[0:maxi_trade]
        
    ## 检查数据有效性
    if kdata.index.size <= 0:
        logging.debug("No valid trades")
        return None
        
    ## 初始参数
    from_trade = kdata.index[-1]
    last_trade = kdata.index[ 0]

    logging.debug("Trades is from " + from_trade + " to " + last_trade)

    ## 筛选符合指标的股票
    return policy_analyze_drop(basic, kdata[0:4], dparam)

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
    result.InitTable(['code', 'name', 'industry', 'take_trade', 'price', 'outstanding', 'score'], name=POLICY_NAME)

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
    result.SortValues(['score'], ascending=False)
    result.Display()

    
def event_handler(stocks, dparam, result):
    """
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
EVENT_TYPE    = Comm.EVT_POLICY_MANDARIN
EVENT_HANDLER = event_handler

if __name__ == "__main__":
	print("Welcome to " +  sys.argv[0] + " package.")