#-*-coding:utf-8-*-

import sys
import time
import math
import copy


from dogen.stock.constant import *

    
def drop_basics(basics, inplace=True):
    """过滤ST/科创板股票
    """
    if not inplace:
        basics = basics.copy()
    
    for i in range(basics.index.size - 1, -1, -1):
        if basics.iloc[i].loc['name'].startswith("*", 0) or basics.iloc[i].loc['name'].startswith("S", 0) or basics.index[i].startswith("68", 0):
            basics.drop(basics.index[i], inplace=True)
    return basics

def drop_codes(codes, inplace=True):
    """ 通过股票代码过滤科创板股票
    """
    if not inplace:
        pool = copy.deepcopy(codes)
    else:
        pool = codes

    for code in pool:
        if code.startswith("68", 0):
            pool.remove(code)
    return pool

def drop_stock_check(code, basic):
    if code is not None and code.startswith("68", 0):
        return True
    if basic is not None:
        if basic[NAME].startswith("*", 0) or basic[NAME].startswith("S", 0):
            return True
        pass
    return False

def drop_fresh_stock_trades(basic, kdata, inplace=True):
    """丢掉新股涨停板, kdata索引按降序排列
    """
    if not inplace:
        kdata = kdata.copy()

    if kdata.index.size <= 0:
        return kdata
        
    ## 校验合法性
    date_first = time.strftime("%Y-%m-%d", time.strptime(str(basic.loc['timeToMarket']), "%Y%m%d"))
    if kdata.index[-1] != date_first:
        return kdata
        
    ## 第一个交易日直接丢弃
    kdata.drop(kdata.index[-1], inplace=True)
    
    ## 再丢掉连板交易日
    for i in range(0, kdata.index.size):
        if (kdata.iloc[-1][L_HIGH] <= kdata.iloc[-1][P_CLOSE]) and (kdata.iloc[-1][P_LOW] == kdata.iloc[-1][P_HIGH]):
            kdata.drop(kdata.index[-1], inplace=True)
        pass
    return kdata

def forecast_macd(kdata):
    this_macd = kdata.iloc[0][MACD]
    prev_macd = kdata.iloc[1][MACD]
    next_macd = this_macd*2 - prev_macd
    return [0, 0, next_macd]

def score_by_pclose(total_score, p_close, l_close):
    if (p_close <= l_close):
        return (int)(total_score * (1 - (math.floor(p_close/l_close))))
    return 0

def score_by_outstanding(total_score, c_out, l_out):
    if (c_out <= l_out):
        return (int)(total_score * (1 - (math.floor(c_out/l_out))))
    return 0

def caculate_l_high(price, limit=10):
    return round(price*(1+limit/100.0), 2)

def caculate_l_low(price, limit=10):
    return round(price*(1-limit/100.0), 2)

def caculate_percentage(num, den, fraction=2):
    """计算百分比
    """
    return round((num*100.0)/den, fraction)
    
def caculate_incr_percentage(nd, od, fraction=2):
    """计算上涨百分比
        nd: new data
        od: old data
    """
    return caculate_percentage(nd-od, od, fraction=fraction)
    
def get_last_column_min(kdata, column, sIdx=0, eIdx=None):
    """获取K线column值最小交易日
        column: K线列参数
        sIdx: 起始索引
        eIdx: 截止索引
    """
    if (eIdx is None) or (eIdx > kdata.index.size):
        eIdx = kdata.index.size
    try:
        date = kdata[sIdx:eIdx].idxmin()[column]
        mIdx = kdata.index.get_loc(date)
    except Exception:
        mIdx = None
    return mIdx

def get_last_column_max(kdata, column, sIdx=0, eIdx=None):
    """查找参数column最大交易日
        column: K线列参数
        sIdx: 起始索引
        eIdx: 截止索引
    """
    if (eIdx is None) or (eIdx > kdata.index.size):
        eIdx = kdata.index.size
    try:
        date = kdata[sIdx:eIdx].idxmax()[column]
        mIdx = kdata.index.get_loc(date)
    except Exception:
        mIdx = None
    return mIdx

def get_last_rise_range(kdata, min_rise, max_fall=10.0, sIdx=0, eIdx=None):
    """查找上一个上升区间
        min_rise: 目标最小涨幅
        max_fall: 上涨过程允许最大跌幅
        sIdx: 起始索引
        eIdx: 截止索引
    
        返回结果：
            [min_index, max_index, inc_close, get_lhigh, $next_index]
    """        
    if (eIdx is None) or (eIdx > kdata.index.size):
        eIdx = kdata.index.size

    the_range = None
    max_index = None
    min_index = None
    tmp_index = None
    get_lhigh = 0
    for i in range(sIdx, eIdx):
        ## 初始化索引
        if (max_index is None):
            max_index = i
        if (min_index is None):
            min_index = i
        
        if (kdata.iloc[i].loc[P_CLOSE] >= kdata.iloc[i].loc[L_HIGH]):
            get_lhigh += 1
        
        ## 更新最小值索引
        if (kdata.iloc[i].loc[P_CLOSE] <  kdata.iloc[min_index].loc[P_CLOSE]):
            min_index = i
        
        ## 最大值变更, 已找到则退出; 否则更新索引, 继续查找
        if (kdata.iloc[i].loc[P_CLOSE] >= kdata.iloc[max_index].loc[P_CLOSE]):
            if (the_range is not None):
                tmp_index = i
                break
            get_lhigh = 0
            ### 当日涨停也要计算
            if (kdata.iloc[i].loc[P_CLOSE] >= kdata.iloc[i].loc[L_HIGH]):
                get_lhigh += 1
            max_index = i
            min_index = i

        if (min_index == max_index):
            continue
            
        max_close = kdata.iloc[max_index].loc[P_CLOSE]
        min_close = kdata.iloc[min_index].loc[P_CLOSE]

        ## 涨幅满足条件, 保存区间信息
        inc_close = caculate_incr_percentage(max_close, min_close)
        if (inc_close >= min_rise):
            the_range = [min_index, max_index, inc_close, get_lhigh]
        
        ## 检查下降幅度, 超出限制，且已找到区间立即退出; 否则继续查找
        tmp_close = kdata.iloc[i].loc[P_CLOSE]
        dec_close = caculate_incr_percentage(min_close, tmp_close)
        if (abs(dec_close) >= max_fall) and (the_range is not None):
            tmp_index = i
            break
        pass

    ## 将最新遍历索引塞进结果, 方便循环遍历
    if the_range is not None:
        the_range.append(i)

    return the_range

def get_last_fall_range(kdata, min_fall, max_rise=10.0, sIdx=0, eIdx=None):
    """查找上一个下降波段, 直至超过最大涨幅
        min_fall: 目标最小跌幅
        max_rise: 上涨过程允许最大涨幅
        sIdx: 起始索引
        eIdx: 截止索引
    """        
    if (eIdx is None) or (eIdx > kdata.index.size):
        eIdx = kdata.index.size

    the_range = None
    max_index = None
    min_index = None
    tmp_index = None
    get_llow  = 0
    for i in range(sIdx, eIdx):
        ## 初始化索引
        if (max_index is None):
            max_index = i
        if (min_index is None):
            min_index = i

        if (kdata.iloc[i].loc[P_CLOSE] <= kdata.iloc[i].loc[L_LOW]):
            get_llow += 1
            
        ## 更新最大值索引
        if (kdata.iloc[i].loc[P_CLOSE] >  kdata.iloc[max_index].loc[P_CLOSE]):
            max_index = i

        ## 最小值索引变更，找到就退出
        if (kdata.iloc[i].loc[P_CLOSE] <= kdata.iloc[min_index].loc[P_CLOSE]):
            if (the_range is not None):
                tmp_index = i
                break
            get_llow  = 0
            if (kdata.iloc[i].loc[P_CLOSE] <= kdata.iloc[i].loc[L_LOW]):
                get_llow += 1
            max_index = i
            min_index = i
        
        if (min_index == max_index):
            continue
        
        max_close = kdata.iloc[max_index].loc[P_CLOSE]
        min_close = kdata.iloc[min_index].loc[P_CLOSE]
        
        ## 跌幅满足条件, 保存区间信息
        dec_close = caculate_incr_percentage(min_close, max_close)
        if (abs(dec_close) >= min_fall):
            the_range = [max_index, min_index, dec_close, get_llow]
        
        tmp_close = kdata.iloc[i].loc[P_CLOSE]
        inc_close = caculate_incr_percentage(max_close, tmp_close)
        if (inc_close >= max_rise) and (the_range is not None):
            tmp_index = i
            break
        pass

    ## 将最新遍历索引塞进结果, 方便循环遍历
    if the_range is not None:
        the_range.append(i)

    return the_range

def get_maxi_serial_range(kdata, min_serial, ascending=True, sIdx=0, eIdx=None):
    """获取最大连阳/阴交易区间
        ascending: True表示取上涨区间, False取下跌区间
    """
    heap_range = None
    if (eIdx is None) or (eIdx > kdata.index.size):
        eIdx = kdata.index.size

    last_index = sIdx
    from_index = sIdx
    heap_trade = 0
    heap_close = 0
    for i in range(sIdx, eIdx):
        if (ascending is True):
            if kdata.iloc[i][R_CLOSE] < 0:
                heap_trade = 0
                heap_close = 0
                last_index = i+1
            else:
                heap_trade+= 1
                heap_close+= kdata.iloc[i][R_CLOSE]
                from_index = i
            pass
        else:
            if kdata.iloc[i][R_CLOSE] > 0:
                heap_trade = 0
                heap_close = 0
                last_index = i+1
            else:
                heap_trade+= 1
                heap_close+= kdata.iloc[i][R_CLOSE]
                from_index = i
            pass
            
        if heap_trade >= min_serial:
            min_serial = heap_trade
            heap_range = [from_index, last_index, heap_close]
        pass
        
    return heap_range
    
def get_highlimit_trades(kdata, sIdx=0, eIdx=None):
    """获取区间内涨停板交易日
    """
    if (eIdx is None) or (eIdx > kdata.index.size):
        eIdx = kdata.index.size
    ktemp = kdata[sIdx:eIdx]
    ktemp = ktemp[ktemp[P_CLOSE] >= ktemp[L_HIGH]]
    return ktemp.index
        