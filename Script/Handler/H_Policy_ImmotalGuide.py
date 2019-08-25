#-*-coding:utf-8-*-

import sys
import logging
import traceback

from Script import *

POLICY_NAME = "Policy_ImmotalGuide"

def Immotal_guide(basic, kdata, pick_index, dparam):
    """上涨趋势中, 仙人指路交易日放量
    """
    ## 有效性检查
    if (kdata.index.size <= 0):
        logging.debug("Invalid trades")
        return None

    ## 均价判断上涨趋势
    if (kdata.iloc[0][Kdata.MA5] <= kdata.iloc[0][Kdata.MA20]):
        logging.debug("Invalid trend")
        return None
    
    ## 要求三日内有效
    if (pick_index > 2):
        return None
    
    ## 最后一天必须上涨
    if (kdata.iloc[0][Kdata.R_CLOSE] <= 0):
        logging.debug("Invalid R_CLOSE")
        return None
    
    ## 最后一天必须放量
    if (kdata.iloc[0][Kdata.VOLUME] <= kdata.iloc[1][Kdata.VOLUME]):
        logging.debug("Invalid voume")
        return None

    return [basic.name, basic[Kdata.NAME], basic[Kdata.INDUSTRY], kdata.index[0], kdata.iloc[0][Kdata.P_CLOSE], basic['outstanding']]

def get_immotal_guide_trade(kdata):
    """找最后三个交易日仙人指路特征
    """
    eIdx = 3
    if (kdata.index.size <= eIdx):
        eIdx = kdata.index.size - 1
    
    pick_index = None
    for i in range(0, eIdx):
        ## 要求最小涨幅
        if (kdata.iloc[i][Kdata.R_CLOSE] <= 2):
            continue
        
        ## 必须阳线收盘
        if (kdata.iloc[i][Kdata.P_CLOSE] <= kdata.iloc[i][Kdata.P_OPEN]):
            continue
        
        ## 最高价涨幅大于收盘价涨幅两倍, 且涨幅最少7个点
        incr_rhigh = Kdata.caculate_incr_percentage(kdata.iloc[i][Kdata.P_HIGH], kdata.iloc[i+1][Kdata.P_CLOSE])
        if (incr_rhigh > 7) and (incr_rhigh >= kdata.iloc[i][Kdata.R_CLOSE]*1.5):
            pick_index = i
        pass
    
    return pick_index
        
    
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
    
    ## 查找仙人指路交易日
    pick_index = get_immotal_guide_trade(kdata)
    if (pick_index is None):
        logging.debug("Don't get immotal guide trade")
        return None
    
    ## 涨停震荡洗盘
    return Immotal_guide(basic, kdata, pick_index, dparam)

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
EVENT_TYPE    = Comm.EVT_POLICY_IMMOTALGUIDE
EVENT_HANDLER = event_handler

if __name__ == "__main__":
	print("Welcome to " +  sys.argv[0] + " package.")