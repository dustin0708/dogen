#-*-coding:utf-8-*-

import sys
import abc
import time
import pandas
import tushare
import logging
import traceback
import threading

NAME        = 'name'
INDUSTRY    = 'industry'

DATE        = 'date'
L_HIGH      = 'L-high'
L_LOW       = 'L-low'

P_OPEN      = 'open'
P_LOW       = 'low'
P_HIGH      = 'high'
P_CLOSE     = 'close'

R_CLOSE     = 'R-close'
R_AMP       = 'R-amp'

VOLUME      = 'volume'
MA5         = 'ma5'
MA10        = 'ma10'
MA20        = 'ma20'

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

    if sys.version_info.major == 2:
        kdata = kdata.astype({'date':'str'})
    kdata.set_index('date', inplace=True)
    return __process_kdata(basic, kdata)

def drop_gem_basics(basics, inplace=True):
    """过滤创业板股票
    """
    if not inplace:
        basics = basics.copy()

    for i in range(basics.index.size - 1, -1, -1):
        if basics.index[i].startswith("3", 0):
            pass # basics.drop(basics.index[i], inplace=True)
    return basics
    
def drop_st_basics(basics, inplace=True):
    """丢弃ST股票
    """
    if not inplace:
        basics = basics.copy()
    
    for i in range(basics.index.size - 1, -1, -1):
        if basics.iloc[i].loc['name'].startswith("*", 0) or basics.iloc[i].loc['name'].startswith("S", 0):
            basics.drop(basics.index[i], inplace=True)
    return basics

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
            
        ## 更新最小值索引
        if (kdata.iloc[i].loc[P_CLOSE] >  kdata.iloc[max_index].loc[P_CLOSE]):
            max_index = i
        if (kdata.iloc[i].loc[P_CLOSE] <= kdata.iloc[min_index].loc[P_CLOSE]):
            get_llow  = 0
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
        
class Database(metaclass=abc.ABCMeta):
    """数据库抽象类
    """
    key_basics = 'basics'
    postfix_kdata = '_kdata'
    
    @abc.abstractmethod
    def read_basics(self):
        pass
                
    @abc.abstractmethod
    def write_basics(self):
        pass
    
    @abc.abstractmethod
    def read_kdata(self):
        pass
        
    @abc.abstractmethod
    def write_kdata(self):
        pass
    
    
class DbH5file(Database):
    """股票数据写hdf5文件(若文件过大使用linux ptrepack工具压缩)
    """
    def __init__(self, filepath):
        self.filepath = filepath
        self.lock = threading.RLock()
        pass
    
    def read_basics(self):
        """从文件读取股票基本信息
        """
        if not self.lock.acquire():
            return None
            
        try:
            basics = pandas.read_hdf(self.filepath, self.key_basics)
            if (basics.index.size <= 0):
                basics = None
        except Exception:
            basics = None
        finally:
            self.lock.release()
        
        if (basics is not None):
            basics.sort_index(inplace=True)
        
        return basics
                
    def write_basics(self, basics):
        """保存股票基本信息到文件
        """
        if not self.lock.acquire():
            return None
            
        try:
            basics = basics.sort_index().copy()
            basics.to_hdf(self.filepath, self.key_basics, format="table", complib="zlib", complevel=9)
        except Exception:
            traceback.print_exc()
        finally:
            self.lock.release()
        pass
    
    def __get_key_of_kdata(self, code):
        """生成股票K线数据键值
        """
        return '_' + code + self.postfix_kdata
    
    def read_kdata(self, code):
        """读取K线数据
        """
        if not self.lock.acquire():
            return None
            
        try:
            kdata = pandas.read_hdf(self.filepath, self.__get_key_of_kdata(code))
            if (kdata.index.size <= 0):
                kdata = None
        except Exception:
            kdata = None
        finally:
            self.lock.release()
        
        if (kdata is not None):
            kdata.sort_index(ascending=False, inplace=True)
        
        return kdata
        
    def write_kdata(self, code, kdata):
        """保存K线数据
        """
        if not self.lock.acquire():
            return None
            
        try:
            edata = pandas.read_hdf(self.filepath, self.__get_key_of_kdata(code))
        except Exception:
            edata = None
            
        try:
            count = 0
            if (edata is not None):
                inter = kdata.index.intersection(edata.index)
                kdata = kdata.drop(inter)
            if (kdata is not None) and (kdata.index.size > 0):
                kdata.to_hdf(self.filepath, self.__get_key_of_kdata(code), format="table", complib="zlib", complevel=9, append=True)
                count = kdata.index.size
        except Exception:
            traceback.print_exc()
        finally:
            self.lock.release()
        return count
    
    
class DbMemory(Database):
    def __init__(self):
        self.dict = {}
        self.lock = threading.RLock()
        pass
    
    def read_basics(self):
        """从内存读取股票基本信息
        """
        if not self.lock.acquire():
            return None
            
        try:
            basics = self.dict[self.key_basics].copy()
            if (basics.index.size <= 0):
                basics = None
        except Exception:
            basics = None
        finally:
            self.lock.release()
        
        if (basics is not None):
            basics.sort_index(inplace=True)
        
        return basics
        
    def write_basics(self, basics=None):
        """保存股票基本信息到内存
        """
        if not self.lock.acquire():
            return None
            
        try:
            if (basics is not None):
                basics = basics.sort_index().copy()
            self.dict[self.key_basics] = basics
        except Exception:
            traceback.print_exc()
        finally:
            self.lock.release()
        pass
    
    def __get_key_of_kdata(self, code):
        """生成股票K线数据键值
        """
        return '_' + code + self.postfix_kdata
        
    def read_kdata(self, code):
        """从内存读取K线数据
        """
        if not self.lock.acquire():
            return None
            
        try:
            kdata = self.dict[self.__get_key_of_kdata(code)].copy()
            if (kdata.index.size <= 0):
                kdata = None
        except Exception:
            kdata = None
        finally:
            self.lock.release()
        
        if (kdata is not None):
            kdata.sort_index(ascending=False, inplace=True)
        
        return kdata
        
    def write_kdata(self, code, kdata=None):
        """保存K线数据到内存
        """
        if not self.lock.acquire():
            return None
            
        try:
            edata = self.dict[self.__get_key_of_kdata(code)]
        except Exception:
            edata = None
            
        try:
            if (kdata is None):
                count = 0
            else:
                count = kdata.index.size
            
            if (edata is not None) and (kdata is not None):
                inter = kdata.index.intersection(edata.index)
                kdata = kdata.drop(inter)
                count = kdata.index.size
                kdata = edata.append(kdata)
            self.dict[self.__get_key_of_kdata(code)] = kdata
        except Exception:
            traceback.print_exc()
        finally:
            self.lock.release()
        return count
        
def copy_db(s_db, d_db):
    """数据库拷贝
    """
    basics = s_db.read_basics()
    if (basics is None):
        logging.error("No data in s_db")
        return None
    else:
        d_db.write_basics(basics)
    
    logging.info("Start copying kdata about " + str(basics.index.size))
    
    count = 0
    for code in basics.index:
        kdata = s_db.read_kdata(code)
        if kdata is None:
            continue
        d_db.write_kdata(code, kdata)
        count += 1
        
    logging.info("Finish copying kdata " + str(count) + '/' + str(basics.index.size))
    return None


    
if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.")