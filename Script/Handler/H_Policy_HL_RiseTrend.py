#-*-coding:utf-8-*-

import sys
import logging
import traceback

from Script import *

POLICY_NAME = "Policy_HL_RiseTrend"


def policy_analyze_HL_long_score(basic, kdata, pick_index, dparam):
    return 0

def policy_analyze_HL_long_takeindex(basic, kdata, pick_index, dparam):
    """ 累积涨幅大于5
    """
    ## 查找最低股价交易日
    temp_index = Kdata.get_last_column_min(kdata, Kdata.P_CLOSE, eIdx=pick_index+1)

    take_index = None
    heap_close = 0
    heap_trade = 0
    for this_index in range(temp_index+1, -1, -1):
        if kdata.iloc[this_index][Kdata.R_CLOSE] < 0:
            heap_close = 0
            heap_trade = 0
            continue
        else:
            heap_close+= kdata.iloc[this_index][Kdata.R_CLOSE]
            heap_trade+= 1

        ## 5日均线上涨
        if kdata.iloc[this_index][Kdata.MA5] < kdata.iloc[this_index+1][Kdata.MA5]:
            logging.debug("Invalid MA5")
            continue
        
        ## 必须放量
        if (kdata.iloc[this_index][Kdata.VOLUME] < (kdata.iloc[this_index+1][Kdata.VOLUME]*1.1)):
            logging.debug("Invalid volume")
            continue
        
        if heap_close >= 5:
            take_index = this_index
        pass
        
    if (take_index is not None) and (take_index > 1):
        logging.debug("Invalid take trade at " + kdata.index[take_index])
        return None
        
    return take_index
    
def policy_analyze_HL_long_drop(basic, kdata, pick_index, dparam):
    
    ## 5日均线上涨
    if kdata.iloc[0][Kdata.MA5] < kdata.iloc[1][Kdata.MA5]:
        logging.debug("Invalid MA5")
        return True
        
    ## 5日均线高于20日均线
    if kdata.iloc[0][Kdata.MA5] < kdata.iloc[0][Kdata.MA20]:
        logging.debug("Invalid MA5 and MA20")
        return True

    ## 收盘价脱离底部
    tmp1_index = Kdata.get_last_column_min(kdata, Kdata.P_CLOSE, eIdx=pick_index+1)
    tmp2_index = Kdata.get_last_column_max(kdata, Kdata.P_CLOSE, eIdx=tmp1_index+1)
    if (kdata.iloc[tmp2_index][Kdata.P_CLOSE]*2) < (kdata.iloc[tmp1_index][Kdata.P_CLOSE]+kdata.iloc[pick_index][Kdata.P_CLOSE]):
        logging.debug("Invalid close price")
        return True
    
    ## 相对涨停价最高涨幅限制
    maxi_close = Comm.GetParamValue(dparam, Comm.PARAM_MAXI_HEAP_RISEUP, 18)
    temp_close = Kdata.caculate_incr_percentage(kdata.iloc[tmp2_index][Kdata.P_CLOSE], kdata.iloc[pick_index][Kdata.P_CLOSE])
    if temp_close > maxi_close:
        logging.debug("Too large close at " + kdata.index[tmp2_index])
        return True
    
    ## 相对涨停价最低跌幅限制
    maxi_close = Comm.GetParamValue(dparam, Comm.PARAM_MAXI_HEAP_FALLEN, kdata.iloc[pick_index][Kdata.R_CLOSE])
    temp_close = Kdata.caculate_incr_percentage(kdata.iloc[pick_index][Kdata.P_CLOSE], kdata.iloc[tmp1_index][Kdata.P_CLOSE])
    if temp_close > maxi_close:
        logging.debug("Too small close at " + kdata.index[tmp1_index])
        return True

    ## 最近三个交易日最大振幅必须大于5%, 涨幅绝对值必须大于3
    temp_trade = kdata[0:3].idxmax()[Kdata.R_CLOSE]
    if kdata.loc[temp_trade][Kdata.R_CLOSE] < 3:
        logging.debug("Invalid maximun trade")
        return True
        
    return False
    
def policy_analyze_HL_long(basic, kdata, pick_index, dparam):
    """5天以前涨停判断
    """
    
    if policy_analyze_HL_long_drop(basic, kdata, pick_index, dparam):
        return None
    
    take_index = policy_analyze_HL_long_takeindex(basic, kdata, pick_index, dparam)
    if take_index is None:
        return None
    
    temp_score = policy_analyze_HL_long_score(basic, kdata, pick_index, dparam)
    
    return [basic.name, basic[Kdata.NAME], basic[Kdata.INDUSTRY], kdata.index[take_index], kdata.iloc[0][Kdata.P_CLOSE], basic['outstanding'], (int)(temp_score)]

def policy_analyze_drop(basic, kdata, dparam):

    ## 最多两个涨停交易日
    hl_array = Kdata.get_highlimit_trades(kdata)
    if (hl_array.size > 2) or (hl_array.size <= 0):
        logging.debug("Too many highlimit trades")
        return True
    
    return False
        
def policy_analyze_stock(basic, kdata, dparam):
    """
    """
    ## 新股特殊处理
    Kdata.drop_fresh_stock_trades(basic, kdata)
    
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
    if policy_analyze_drop(basic, kdata, dparam):
        return None
        
    ## 检查涨停交易日
    hl_trade = Kdata.get_highlimit_trades(kdata)[0]
    pick_index = kdata.index.get_loc(hl_trade)
    if pick_index <= 5:
        logging.debug("Invalid hl trade at " + kdata.index[pick_index])
        return None
    
    return policy_analyze_HL_long(basic, kdata, pick_index, dparam)

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
EVENT_TYPE    = Comm.EVT_POLICY_HL_RISETREND
EVENT_HANDLER = event_handler

if __name__ == "__main__":
	print("Welcome to " +  sys.argv[0] + " package.")