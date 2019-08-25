#-*-coding:utf-8-*-

import sys
import logging
import traceback

from Script import *

POLICY_NAME = "Policy_SerialRise"

   
def policy_analyze_score(basic, kdata, dparam):
    """ 为股票评分, 评分高表示股票更优质; 考虑股价上涨获利的原则, 
    主要以股价评分为核心, 辅以成交量、市值、股价等手段加以优化排序.
    """
    temp_score = 0
    
    ## 连阳计分（累积上涨交易日数+累积涨幅）
    temp_range = Kdata.get_maxi_serial_range(kdata, 3)
    if temp_range is not None:
        temp_score+=(temp_range[0] - temp_range[1] + 1) + temp_range[2]
    
    ## 上涨大于3%计分（交易日数+平均振幅+平均收盘增幅）
    temp_kdata = kdata[kdata[Kdata.R_CLOSE] >= 3]
    if temp_kdata.index.size > 0:
        temp_score += temp_kdata.index.size
        temp_score += temp_kdata[Kdata.R_AMP].sum()/temp_kdata.index.size
        temp_score += abs(temp_kdata[Kdata.R_CLOSE]).sum()/temp_kdata.index.size
    
    ## 下跌3%以上计分（平均收盘增幅绝对值）
    temp_kdata = kdata[kdata[Kdata.R_CLOSE] <= -3]
    if temp_kdata.index.size > 0:
        temp_score += temp_kdata.index.size
        temp_score += temp_kdata[Kdata.R_AMP].sum()/temp_kdata.index.size
        temp_score += abs(temp_kdata[Kdata.R_CLOSE]).sum()/temp_kdata.index.size
        
    ## 放量、MA5均价计分（累积交易日数）
    for temp_index in range(0, kdata.index.size-2):
        if kdata.iloc[temp_index+0][Kdata.VOLUME] > kdata.iloc[temp_index+1][Kdata.VOLUME]:
            temp_score += 1
        if kdata.iloc[temp_index+0][Kdata.MA5] > kdata.iloc[temp_index+1][Kdata.MA5]:
            temp_score += 1
        pass
    
    ## 涨幅扩大计分（累积交易日数）
    for temp_index in range(0, kdata.index.size-2):
        this_close = kdata.iloc[temp_index+0][Kdata.R_CLOSE]
        last_close = kdata.iloc[temp_index+1][Kdata.R_CLOSE]
        if (this_close < 0) or (last_close < 0):
            continue
        if (this_close > last_close):
            temp_score += (this_close - last_close)
        pass
        
    return (int)(temp_score)
    
def policy_analyze_takeindex(basic, kdata, dparam):
    """ 获取合适的买入点
        * 累积涨幅超过5%
        * 缩量下跌超过5%
    """
    take_index = None
    rise_close = 0
    fall_close = 0
    for this_index in range(kdata.index.size-1, -1, -1):
        this_close = kdata.iloc[this_index][Kdata.R_CLOSE]
        
        if this_close > 0:
            fall_close = 0
            rise_close+= this_close
        elif this_close < 0:
            rise_close = 0
            fall_close+= this_close
        else:
            continue
        
        if (abs(rise_close) < 5) and (abs(fall_close) < 5):
            continue
            
        take_index = this_index
            
    return take_index
    
def policy_analyze_drop(basic, kdata, dparam):
    """ 根据股票K线特征是否符合策略
    """        
    ## MA5一直在MA20之上
    temp_trade = kdata[kdata[Kdata.MA5] < kdata[Kdata.MA20]]
    if temp_trade.index.size > 0:
        logging.debug("Invalid ma5 && ma20")
        return True

    ## MA20必须一直上涨
    for this_index in range(0, kdata.index.size-2):
        if kdata.iloc[this_index][Kdata.MA20] < kdata.iloc[this_index+1][Kdata.MA20]:
            logging.debug("Invalid MA20")
            return  True
        pass
    
    ## 最后5个交易日, MA5不能一直下跌
    temp_value = True
    for this_index in range(0, 5):
        if kdata.iloc[this_index][Kdata.MA5] > kdata.iloc[this_index+1][Kdata.MA5]:
            temp_value = False
            break
        pass
    if temp_value:
        logging.debug("Invalid MA5 in last trades")
        return True
        
    ## 收盘价不能长期低于MA20
    temp_trade = kdata[kdata[Kdata.P_CLOSE] < kdata[Kdata.MA20]]
    if temp_trade.index.size > 3:
        logging.debug("Invalid trade at " + temp_trade.index[0])
        return True
        
    ## 上涨交易日更多, 量更大
    rise_trade = kdata[kdata[Kdata.R_CLOSE] > 0]
    fall_trade = kdata[kdata[Kdata.R_CLOSE] < 0]
    if (rise_trade.index.size < fall_trade.index.size) or (rise_trade[Kdata.VOLUME].sum() < fall_trade[Kdata.VOLUME].sum()):
        logging.debug("Too many fall trades")
        return True
    
    ## 无放量下跌交易日(相对于上涨交易日, 跌幅至少大于3%)
    for this_index in range(0, kdata.index.size-2):
        if kdata.iloc[this_index][Kdata.R_CLOSE] >= 0:
            continue
        
        ## 忽略假阴
        if kdata.iloc[this_index][Kdata.P_CLOSE] >= kdata.iloc[this_index][Kdata.P_OPEN]:
            continue
        
        ## 预留20%的误差区间
        last_volume = kdata.iloc[this_index+1][Kdata.VOLUME] * 1.2
        this_voulme = kdata.iloc[this_index  ][Kdata.VOLUME]
        if  this_voulme > last_volume:
            logging.debug("Fall with larger volume at " + kdata.index[this_index])
            return True
        pass

    ## 最近三个交易日最大振幅必须大于5%, 涨幅绝对值必须大于3
    temp_index = kdata[0:3].idxmax()[Kdata.R_AMP]
    if (abs(kdata.loc[temp_index][Kdata.R_CLOSE]) < 3) or (kdata.loc[temp_index][Kdata.R_AMP] < 5):
        logging.debug("Invalid maximun trade")
        return True
        
    ## 区间最大上涨幅度不超过25个点
    temp_range = Kdata.get_last_rise_range(kdata, 25, 25)
    if temp_range is not None:
        logging.debug("Too large rise range")
        return True
    
    ## 连续涨幅不超过18个点
    temp_range = Kdata.get_maxi_serial_range(kdata, 1)
    if temp_range[2] >= 18:
        logging.debug("Too large serial rise range")
        return True
    
    return False

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
    maxi_trade = Comm.GetParamValue(dparam, Comm.PARAM_MAXI_TRADE, 10)
    if maxi_trade is not None:
        kdata = kdata[0:maxi_trade]

    ## 检查数据有效性
    if kdata.index.size < maxi_trade:
        logging.debug("No enough valid trades")
        return True
        
    ## 筛选符合指标的股票
    if policy_analyze_drop(basic, kdata, dparam):
        return None
    
    ## 获取合适的买入点
    take_index = policy_analyze_takeindex(basic, kdata, dparam)
    if (take_index is None):
        logging.debug("Don't get take trade")
    elif (take_index >= 3):
        logging.debug("Get invalid take trade at " + kdata.index[take_index])
        return None
        
    ## 为股票评分
    score = policy_analyze_score(basic, kdata[0:5], dparam)
    if score < 0:
        logging.debug("Invalid scored")
        return None
    
    return [basic.name, basic[Kdata.NAME], basic[Kdata.INDUSTRY], kdata.index[take_index], kdata.iloc[0][Kdata.P_CLOSE], basic['outstanding'], score]

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
EVENT_TYPE    = Comm.EVT_POLICY_SERIALRISE
EVENT_HANDLER = event_handler

if __name__ == "__main__":
	print("Welcome to " +  sys.argv[0] + " package.")