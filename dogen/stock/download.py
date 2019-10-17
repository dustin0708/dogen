#-*-coding:utf-8-*-

import sys
import talib
import dogen
import pandas
import tushare

from dogen.stock.constant import *

def __process_kdata(basic, kdata):
    """ 加工交易数据, 计算涨停价、跌停价、涨幅、振幅、MA5、MA10、MA20等参数

        参数说明：
            basic - 股票基本信息数据，Series类型
            kdata - 股票交易日线数据

        返回结果：
            股票交易日线数据（拓展了列参数）
    """
    colum = []
    colum.append(DATE)
    colum.append(L_HIGH)
    colum.append(L_LOW)
    
    colum.append(P_OPEN)
    colum.append(P_LOW)
    colum.append(P_HIGH)
    colum.append(P_CLOSE)
    
    colum.append(R_CLOSE)
    colum.append(R_AMP)

    colum.append(VOLUME)    
    colum.append(MA5)
    colum.append(MA10)
    colum.append(MA20)
    
    colum.append(DIF)
    colum.append(DEA)
    colum.append(MACD)

    # 样本总数
    samples = kdata.index.size

    # 计算macd
    [dif, dea, macd] = talib.MACD(kdata[P_CLOSE])
    dif.fillna(0, inplace=True)
    dea.fillna(0, inplace=True)
    macd.fillna(0, inplace=True)

    # 初始化目标数据 
    ndata = pandas.DataFrame(columns=colum)

    # 追加最后一行, 避免越界
    kdata.sort_index(ascending=False, inplace=True)
    kdata = kdata.append(kdata.iloc[-1])
    for i in range(0, samples):
        date  = kdata.index[i]
        open  = kdata.iloc[ i ].loc[P_OPEN]
        close = kdata.iloc[ i ].loc[P_CLOSE]
        high  = kdata.iloc[ i ].loc[P_HIGH]
        low   = kdata.iloc[ i ].loc[P_LOW]
        volume= kdata.iloc[ i ].loc[VOLUME]
        
        last_close = kdata.iloc[i+1].loc[P_CLOSE]
        if basic.name.startswith('68',0):
            L_high = dogen.caculate_l_high(last_close, limit=20)
            L_low  = dogen.caculate_l_low(last_close, limit=20)
        else:
            L_high = dogen.caculate_l_high(last_close)
            L_low  = dogen.caculate_l_low(last_close)
        
        diff_close = (close - last_close)/last_close
        R_close    = round(100 * diff_close, 2)
        
        amplitude  = (high - low)/last_close
        R_amp      = round(100 * amplitude, 2)
        
        ma5  = round(kdata[i:(i+5 )][P_CLOSE].mean(), 2)
        ma10 = round(kdata[i:(i+10)][P_CLOSE].mean(), 2)
        ma20 = round(kdata[i:(i+20)][P_CLOSE].mean(), 2)

        value = []
        value.append(date)
        value.append(L_high)
        value.append(L_low)
        value.append(open)
        value.append(low)
        value.append(high)
        value.append(close)
        value.append(R_close)
        value.append(R_amp)
        value.append(volume)
        value.append(ma5)
        value.append(ma10)
        value.append(ma20)
        value.append(dif[samples-1-i])
        value.append(dea[samples-1-i])
        value.append(macd[samples-1-i]*2)
        tdata = pandas.DataFrame([value], columns=colum)
        ndata = ndata.append(tdata)
        pass

    ndata.set_index('date', inplace=True)
    return ndata

def download_basics():
    """ 下载A股股票基本信息
        
        返回结果：
            源自tushare，只是针对部分数据类型做改动，未增减字段；失败返回None
    """
    try:
        basics = tushare.get_stock_basics()
        return basics.astype({'timeToMarket': 'str'})
    except Exception:
        basics = None
    return basics

def download_kdata(basic, start='', end=''):
    """ 下载指定股票交易日线数据

        参数说明：
            basic - 指定股票基本信息
            start - 起始交易日
            end - 截止交易日

        返回结果：
            加工后日线交易数据，失败返回None
    """
    try:
        kdata = tushare.get_k_data(basic.name, start, end)
        if (kdata is None) or (kdata.index.size <= 0):
            return None
    except Exception:
        return None

    kdata.set_index('date', inplace=True)
    return __process_kdata(basic, kdata)
