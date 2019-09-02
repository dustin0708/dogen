#-*-coding:utf-8-*-

import sys
import dogen
import pymongo
import logging
import traceback

from celery import Celery

app = Celery(__name__, broker='pyamqp://127.0.0.1')

def policy_analyze(basic, kdata, maxi_trade=5, mini_scale=1.2, mini_falls=4):
    """ 涨停回调策略, 有如下特征：
            * 涨停在五日之内;
            * 涨停后紧接着最多上涨一天, 若上涨必须放量20%;
            * 累积下跌4个点以上;
            * 最后一日MA5上涨;
    """
    ### 特征一校验
    index = dogen.get_highlimit_trades(kdata, eIdx=maxi_trade)
    if index.size != 1:
        logging.debug("Don't match highlimit trades")
        return None
    else:
        pick_trade = index[0]        
        ### 若最后一天涨停忽略
        pick_index = kdata.index.get_loc(pick_trade)
        if pick_index == 0:
            logging.debug("Fallback didn't occur")
            return None
        pass
    
    
    ### 特征二校验
    if kdata.iloc[pick_index-1][dogen.R_CLOSE] > 0:
        if (kdata.iloc[pick_index][dogen.VOLUME] * mini_scale) > kdata.iloc[pick_index-1][dogen.VOLUME]:
            logging.debug("Too small volume at " + kdata.index[pick_index-1])
            return None
        ### 更正pick_index
        pick_index = pick_index-1
        if pick_index == 0:
            logging.debug("Fallback didn't occur")
            return None
        pass
    
    ### 特征三校验
    heap_falls = 0
    take_index = None
    for this_index in range(pick_index-1, -1, -1):        
        this_close = kdata.iloc[this_index][dogen.R_CLOSE]        
        ### 若上涨停止
        if  this_close > 0:
            ### 更新take_index
            if take_index is not None:
                take_index = this_index
            break
        ### 达到回调要求, 命中
        heap_falls += abs(this_close)
        if heap_falls >= mini_falls:
            take_index = this_index
        pass
    if take_index is None:
        logging.debug("Don't match valid fallback trade")
        return None
    
    ### 特征四校验
    if kdata.iloc[take_index+1][dogen.MA5] >= kdata.iloc[take_index][dogen.MA5]:
        logging.debug("Don't match valid MA5 at " + kdata.index[take_index])
        return None
    
    ### 结果最后排它校验
        
    return [basic.name, kdata.index[take_index]]

@app.task
def execute(codes, start=None, end=None, save_into_db=True):
    
    try:
        db = dogen.DbMongo()
    except Exception:
        traceback.print_exc()
        return traceback.format_exc()
    
    ### 依次策略检查
    success_list = []
    for code in codes:
        try:
            ### 从数据库读取basic数据
            basic = db.lookup_stock_basic(code)
        
            ### 从数据库读取日线数据，必须按索引（日期）降序排列
            kdata = db.lookup_stock_kdata(code, start=start, end=end)
            kdata.sort_index(ascending=False, inplace=True)
            
            ### 策略分析
            match = policy_analyze(basic, kdata)
            if match is None:
                continue
            
            ### 输出结果
            if save_into_db:
                success_list.append(match)
            else:
                success_list.append(match)
        except Exception:
            continue
        pass            
        
    return success_list

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.") 
    