#-*-coding:utf-8-*-

import sys
import pandas
import tushare

from dogen.stock.constant import *

def __process_mean(kdata, column):
    """计算平均值
    """
    sum = 0
    if (kdata.index.size <= 0):
        return 0
        
    for i in range(0, kdata.index.size):
        sum += kdata.iloc[i].loc[column]
    return round(sum/kdata.index.size, 2)

def __process_kdata(basic, kdata):
    """加工交易数据
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
    
    ndata = pandas.DataFrame(columns=colum)
    kdata.sort_index(ascending=False, inplace=True)
    
    # 追加最后一行, 避免越界
    kdata = kdata.append(kdata.iloc[-1])
    for i in range(0, kdata.index.size - 1):
        date  = kdata.index[i]
        open  = kdata.iloc[ i ].loc[P_OPEN]
        close = kdata.iloc[ i ].loc[P_CLOSE]
        high  = kdata.iloc[ i ].loc[P_HIGH]
        low   = kdata.iloc[ i ].loc[P_LOW]
        volume= kdata.iloc[ i ].loc[VOLUME]
        
        last_close = kdata.iloc[i+1].loc[P_CLOSE]
        L_high     = round(1.1 * last_close, 2)
        L_low      = round(0.9 * last_close, 2)
        
        diff_close = (close - last_close)/last_close
        R_close    = round(100 * diff_close, 2)
        
        amplitude  = (high - low)/last_close
        R_amp      = round(100 * amplitude, 2)
        
        ma5  = __process_mean(kdata[i:(i+5 )], P_CLOSE)
        ma10 = __process_mean(kdata[i:(i+10)], P_CLOSE)
        ma20 = __process_mean(kdata[i:(i+20)], P_CLOSE)

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
        tdata = pandas.DataFrame([value], columns=colum)
        ndata = ndata.append(tdata)
        pass
    ndata.set_index('date', inplace=True)
    return ndata

def download_basics():
    """下载A股股票基本信息
    """
    try:
        basics = tushare.get_stock_basics()
        return basics.astype({'timeToMarket': 'str'})
    except Exception:
        basics = None
    return basics

def download_kdata(basic, start='', end=''):
    """下载K线数据
    """
    try:
        kdata = tushare.get_k_data(basic.name, start, end)
        if (kdata is None) or (kdata.index.size <= 0):
            return None
    except Exception:
        return None

    kdata.set_index('date', inplace=True)
    return __process_kdata(basic, kdata)

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.")