#-*-coding:utf-8-*-

import sys
import math
import dogen
import celery_dogen

### 导入当前模块变量
from . import app
from . import logger

@app.task
def hl_fallback_match(codes, start=None, end=None, max_days=60, save_result=False, take_valid=0, maxi_trade=5, mini_scale=1.2, mini_falls=4):
    """ 调用hl_fallback策略
    """
    logger.info("%s called with arguments: len(codes)=%d, start=%s, end=%s, max_days=%d, save_result=%s" % ('hl_fallback_match', len(codes), start, end, max_days, save_result))
    return dogen.hl_fallback.match(codes, start=start, end=end, max_days=max_days, save_result=save_result, take_valid=take_valid, maxi_trade=maxi_trade, mini_scale=mini_scale, mini_falls=mini_falls)

@app.task
def dispatcher_of_hl_fallback_match(codes=None, start=None, end=None, save_result=False, slice=1000):
    """ hl_fallback策略任务拆分

        参数说明：
            start - 样本起始交易日(数据库样本可能晚于该日期, 如更新不全)；若未指定默认取end-$max_days做起始日
            end - 样本截止交易日(数据库样本可能早于该日期, 如停牌)
            save_result - 保存命中结果
        
        返回结果：
            列表数据如[{item-1}, {item-2}, ..., {item-n}]，根据股票的流通市值、收盘价、成交量、涨跌幅等数据决策。
    """
    ### 初始化本地数据库连接
    try:
        db = dogen.DbMongo()
    except Exception:
        traceback.print_exc()
        return None
    
    ### 获取代码列表
    if codes is None:
        codes = db.lookup_stock_codes()

    ### 拆分任务
    tasks = (int)(math.ceil(len(codes)/slice))
    logger.info('%s called to dispatch %d codes into %d sub-task' % ('dispatcher_of_hl_fallback_match', len(codes), tasks))
    
    reply = []
    for i in range(0, tasks):
        reply.append(celery_dogen.hl_fallback_match.delay(codes[i*slice:(i+1)*slice], start=start, end=end, save_result=save_result))

    ### 聚合子任务结果
    result = []
    for i in range(0, len(reply)):
        while not reply[i].ready():
            continue
        result.extend(reply[i].result)

    return result
