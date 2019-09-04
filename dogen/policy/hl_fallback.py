#-*-coding:utf-8-*-

import sys
import dogen
import pymongo
import traceback

from celery import Celery
from celery.utils.log import get_task_logger

### 从外部程序导入
from celery_dogen import app

### 日志句柄
logger = get_task_logger(__name__)

def policy_analyze(basic, kdata, take_valid, maxi_trade, mini_scale, mini_falls):
    ### 特征一校验
    index = dogen.get_highlimit_trades(kdata, eIdx=maxi_trade)
    if index.size != 1:
        logger.debug("Don't match highlimit trades")
        return None
    else:
        pick_trade = index[0]        
        ### 若最后一天涨停忽略
        pick_index = kdata.index.get_loc(pick_trade)
        if pick_index == 0:
            logger.debug("Fallback didn't occur")
            return None
        pass
    
    ### 特征二校验
    if kdata.iloc[pick_index-1][dogen.R_CLOSE] > 0:
        if (kdata.iloc[pick_index][dogen.VOLUME] * mini_scale) > kdata.iloc[pick_index-1][dogen.VOLUME]:
            logger.debug("Too small volume at " + kdata.index[pick_index-1])
            return None
        ### 更正pick_index
        pick_index = pick_index-1
        if pick_index == 0:
            logger.debug("Fallback didn't occur")
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
    if take_index is None or take_index > take_valid:
        logger.debug("Don't match valid fallback trade")
        return None
    
    ### 特征四校验
    if kdata.iloc[take_index+1][dogen.MA5] >= kdata.iloc[take_index][dogen.MA5]:
        logger.debug("Don't match valid MA5 at " + kdata.index[take_index])
        return None
    
    ### 结果最后排它校验
        
    return [basic.name, kdata.index[take_index]]

@app.task
def execute(codes, start=None, end=None, max_days=60, save_result=True, take_valid=0, maxi_trade=5, mini_scale=1.2, mini_falls=4):
    """ 涨停回调策略, 有如下特征：
            * 涨停在$maxi_trade个交易日之内;
            * 涨停后紧接着最多上涨一天, 若上涨必须放量$mini_scale倍;
            * 累积下跌等于或大于$mini_falls;
            * 最后一日MA5上涨;

        参数说明：
            start - 样本起始交易日(数据库样本可能晚于该日期, 如更新不全)；若未指定默认取end-$max_days做起始日
            end - 样本截止交易日(数据库样本可能早于该日期, 如停牌)
            max_days - 自然日数（交易日和非交易日），若start取有效值，该字段无效
            save_result - 保存命中结果
            take_valid - 命中交易日有效期, 0表示最后一天命中有效
            maxi_trade - 最后一个涨停有效交易日数
            mini_scale - 涨停后一交易日上涨时，放量最小倍数
            mini_falls - 回调最小幅度，单位1%
        
        返回结果：
            列表数据如[[item-1], [item-2], ..., [item-n]]，根据股票的流通市值、收盘价、成交量、涨跌幅等数据决策。
    """
    try:
        db = dogen.DbMongo()
    except Exception:
        logger.error(traceback.format_exc())
        return None
    
    ### 依次策略检查
    success_list = []
    for code in codes:
        try:
            ### 从数据库读取basic数据
            basic = db.lookup_stock_basic(code)

            ### 从数据库读取日线数据，必须按索引（日期）降序排列
            if end is None:
                end = dogen.date_today()
            if start is None:
                start = dogen.date_delta(end, -max_days)
            kdata = db.lookup_stock_kdata(code, start=start, end=end)
            kdata.sort_index(ascending=False, inplace=True)
            
            ### 策略分析
            match = policy_analyze(basic, kdata, take_valid, maxi_trade, mini_scale, mini_falls)
            if match is None:
                continue
            
            ### 输出结果
            success_list.append(match)
            if save_result:
                pass
        except Exception:
            continue
        pass
        
    return success_list

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.") 
    