#-*-coding:utf-8-*-

import sys
import logging
import traceback

from Script import *

POLICY_NAME = "Policy_Rebound"

def policy_analyze_stock2(basic, kdata, dparam):
    """快速下跌反弹类型
    """
    mini_fallen = Comm.GetParamValue(dparam, Comm.PARAM_MINI_HEAP_FALLEN, 20)
    maxi_trades = Comm.GetParamValue(dparam, Comm.PARAM_MAXI_TRADE, 15)
    
    ## 寻找合适的下降区间
    fall_range = Kdata.get_last_fall_range(kdata, mini_fallen, eIdx=maxi_trades)
    if (fall_range is None):
        logging.debug("Don't get valid fall range")
        return None
    else:
        maxi_index = fall_range[0]
        mini_index = fall_range[1]
        desc_close = fall_range[2]
    
    ## 校验最低交易日有效性
    if (mini_index > 2):
        logging.debug("Invalid mini_trade at" + kdata.index[mini_index])
        return None
    
    ## 检查合适的反弹交易日
    take_index = Kdata.get_last_column_max(kdata, Kdata.R_CLOSE, eIdx=mini_index)
    if (take_index is None) or (kdata.iloc[take_index][Kdata.R_CLOSE] < 4):
        logging.debug("Don't get valid take index since " + kdata.index[mini_index])
        return None
    
    ## 最高价交易日前最大涨幅不可超过mini_fallen
    rise_range = Kdata.get_last_rise_range(kdata, 10, max_fall=10.0, sIdx=maxi_index, eIdx=(maxi_index+maxi_trades))
    if (rise_range is not None) and (rise_range[2] >= mini_fallen):
        logging.debug("Ignore the fall range")
        return None
    
    return [basic.name, basic[Kdata.NAME], basic[Kdata.INDUSTRY], kdata.index[take_index], kdata.iloc[0][Kdata.P_CLOSE], basic['outstanding']]


def policy_analyze_pick_index(basic, kdata, dparam):
    ## 优先找合适的pick_index
    mini_close = 6
    pick_valid = 10
    
    pick_index = Kdata.get_last_column_max(kdata, Kdata.R_CLOSE, eIdx=pick_valid)
    if (pick_index is not None) and (kdata.iloc[pick_index][Kdata.R_CLOSE] < mini_close):
        return pick_index
    
    return None

def policy_analyze_stock(basic, kdata, dparam):
    """缓存下跌，大幅度反弹
    """
    ## 参数处理
    mini_fallen = Comm.GetParamValue(dparam, Comm.PARAM_MINI_HEAP_FALLEN, 40)
    maxi_riseup = Comm.GetParamValue(dparam, Comm.PARAM_MAXI_HEAP_RISEUP, 20)
    
    take_valid  = 10
    mini_valid  = 10
    
    ## 新股特殊处理
    Kdata.drop_fresh_stock_trades(basic, kdata)

    ## 检查数据有效性
    if kdata.index.size <= 0:
        logging.debug("No valid trades")
        return None
        
    ## 初始参数
    from_trade = kdata.index[-1]
    last_trade = kdata.index[ 0]

    logging.debug("Trades is from " + from_trade + " to " + last_trade)
    
    pick_index = policy_analyze_pick_index(basic, kdata, dparam)
    if (pick_index is None):
        logging.debug("Don't get valid pick_trade")
        return None
        
    ## 找上一波下跌区间
    temp_range = Kdata.get_last_fall_range(kdata, mini_fallen, max_rise=maxi_riseup)
    if (temp_range is None):
        logging.debug("Don't get valid fall range")
        return None
    else:
        maxi_index = temp_range[0]
        mini_index = temp_range[1]
        logging.debug("fall from " + kdata.index[maxi_index] + " to " + kdata.index[mini_index])
    
    ## 区间校验
    if (mini_index < pick_index) or (mini_index > mini_valid):
        logging.debug("Invalid pick_trade at " + kdata.index[pick_index])
        return None
        
    return [basic.name, basic[Kdata.NAME], basic[Kdata.INDUSTRY], kdata.index[pick_index], kdata.iloc[0][Kdata.P_CLOSE], basic['outstanding']]

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
                record = policy_analyze_stock2(basic, kdata, dparam)
                if record is None:
                    continue
                pass
            
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
EVENT_TYPE    = Comm.EVT_POLICY_REBOUND
EVENT_HANDLER = event_handler

if __name__ == "__main__":
	print("Welcome to " +  sys.argv[0] + " package.")