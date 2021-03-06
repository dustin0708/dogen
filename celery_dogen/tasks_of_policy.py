#-*-coding:utf-8-*-

import sys
import time
import dogen
import pandas
import traceback
import celery_dogen

### 导入当前模块变量
from . import app
from . import logger, mongo_server, mongo_database

def dispatcher_poll_result(reply):
    result = []
    for i in range(0, len(reply)):
        while not reply[i].ready():
            time.sleep(0.05)
            continue
        result.extend(reply[i].result)
    data = None
    if len(result) > 0:
        data = pandas.DataFrame.from_dict(result, orient='columns')
        data.sort_values(by=dogen.RST_COL_SCORE, ascending=False, inplace=True)
    return data

@app.task
def hl_fallheap_match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 调用hl_fallback策略
    """
    logger.info("%s called with arguments: len(codes)=%d, start=%s, end=%s, save_result=%s, policy_args=%s" % ('hl_fallheap_match', len(codes), start, end, save_result, policy_args))
    return dogen.hl_fallheap.match(codes, start=start, end=end, save_result=save_result, policy_args=policy_args)

@app.task
def dispatcher_of_hl_fallheap_match(codes=None, start=None, end=None, save_result=False, policy_args=None, slice=1000):
    """ hl_fallback策略任务拆分

        参数说明：
            start - 样本起始交易日(数据库样本可能晚于该日期, 如更新不全)；若未指定默认取end-$max_days做起始日
            end - 样本截止交易日(数据库样本可能早于该日期, 如停牌)
            save_result - 保存命中结果
        
        返回结果：
            列表数据如[{item-1}, {item-2}, ..., {item-n}]，根据股票的流通市值、收盘价、成交量、涨跌幅等数据决策。
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None
    
    ### 获取代码列表
    if codes is None:
        codes = db.lookup_stock_codes()
        codes.sort()

    ### 拆分任务, 聚合结果
    logger.info('%s called to dispatch %d codes into sub-task with slice=%d' % ('dispatcher_of_hl_fallheap_match', len(codes), slice))
    reply = []
    while True:
        tasks = len(reply)
        if (tasks * slice) >= len(codes):
            break
        reply.append(celery_dogen.hl_fallheap_match.delay(codes[tasks*slice:(tasks+1)*slice], start=start, end=end, save_result=save_result, policy_args=policy_args))

    ### 聚合子任务结果
    return dispatcher_poll_result(reply)

@app.task
def rb_risekeep_match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 调用rebound策略
    """
    logger.info("%s called with arguments: len(codes)=%d, start=%s, end=%s, save_result=%s, policy_args=%s" % ('rb_risekeep_match', len(codes), start, end, save_result, policy_args))
    return dogen.rb_risekeep.match(codes, start=start, end=end, save_result=save_result, policy_args=policy_args)

@app.task
def dispatcher_of_rb_risekeep_match(codes=None, start=None, end=None, save_result=False, policy_args=None, slice=1000):
    """ rebound策略任务拆分

        参数说明：
            start - 样本起始交易日(数据库样本可能晚于该日期, 如更新不全)；若未指定默认取end-$max_days做起始日
            end - 样本截止交易日(数据库样本可能早于该日期, 如停牌)
            save_result - 保存命中结果
        
        返回结果：
            列表数据如[{item-1}, {item-2}, ..., {item-n}]，根据股票的流通市值、收盘价、成交量、涨跌幅等数据决策。
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None
    
    ### 获取代码列表
    if codes is None:
        codes = db.lookup_stock_codes()
        codes.sort()

    ### 拆分任务, 聚合结果
    logger.info('%s called to dispatch %d codes into sub-task with slice=%d' % ('dispatcher_of_rb_risekeep_match', len(codes), slice))
    reply = []
    while True:
        tasks = len(reply)
        if (tasks * slice) >= len(codes):
            break
        reply.append(celery_dogen.rb_risekeep_match.delay(codes[tasks*slice:(tasks+1)*slice], start=start, end=end, save_result=save_result, policy_args=policy_args))

    ### 聚合子任务结果
    return dispatcher_poll_result(reply)


@app.task
def hl_risekeep_match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 调用rebound策略
    """
    logger.info("%s called with arguments: len(codes)=%d, start=%s, end=%s, save_result=%s, policy_args=%s" % ('hl_risekeep_match', len(codes), start, end, save_result, policy_args))
    return dogen.hl_risekeep.match(codes, start=start, end=end, save_result=save_result, policy_args=policy_args)

@app.task
def dispatcher_of_hl_risekeep_match(codes=None, start=None, end=None, save_result=False, policy_args=None, slice=1000):
    """ rebound策略任务拆分

        参数说明：
            start - 样本起始交易日(数据库样本可能晚于该日期, 如更新不全)；若未指定默认取end-$max_days做起始日
            end - 样本截止交易日(数据库样本可能早于该日期, 如停牌)
            save_result - 保存命中结果
        
        返回结果：
            列表数据如[{item-1}, {item-2}, ..., {item-n}]，根据股票的流通市值、收盘价、成交量、涨跌幅等数据决策。
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None
    
    ### 获取代码列表
    if codes is None:
        codes = db.lookup_stock_codes()
        codes.sort()

    ### 拆分任务, 聚合结果
    logger.info('%s called to dispatch %d codes into sub-task with slice=%d' % ('dispatcher_of_hl_risekeep_match', len(codes), slice))
    reply = []
    while True:
        tasks = len(reply)
        if (tasks * slice) >= len(codes):
            break
        reply.append(celery_dogen.hl_risekeep_match.delay(codes[tasks*slice:(tasks+1)*slice], start=start, end=end, save_result=save_result, policy_args=policy_args))

    ### 聚合子任务结果
    return dispatcher_poll_result(reply)


@app.task
def new_ipo_match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 调用rebound策略
    """
    logger.info("%s called with arguments: len(codes)=%d, start=%s, end=%s, save_result=%s, policy_args=%s" % ('new_ipo_match', len(codes), start, end, save_result, policy_args))
    return dogen.new_ipo.match(codes, start=start, end=end, save_result=save_result, policy_args=policy_args)

@app.task
def dispatcher_of_new_ipo_match(codes=None, start=None, end=None, save_result=False, policy_args=None, slice=1000):
    """ rebound策略任务拆分

        参数说明：
            start - 样本起始交易日(数据库样本可能晚于该日期, 如更新不全)；若未指定默认取end-$max_days做起始日
            end - 样本截止交易日(数据库样本可能早于该日期, 如停牌)
            save_result - 保存命中结果
        
        返回结果：
            列表数据如[{item-1}, {item-2}, ..., {item-n}]，根据股票的流通市值、收盘价、成交量、涨跌幅等数据决策。
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None
    
    ### 获取代码列表
    if codes is None:
        codes = db.lookup_stock_codes()
        codes.sort()

    ### 拆分任务, 聚合结果
    logger.info('%s called to dispatch %d codes into sub-task with slice=%d' % ('dispatcher_of_new_ipo_match', len(codes), slice))
    reply = []
    while True:
        tasks = len(reply)
        if (tasks * slice) >= len(codes):
            break
        reply.append(celery_dogen.new_ipo_match.delay(codes[tasks*slice:(tasks+1)*slice], start=start, end=end, save_result=save_result, policy_args=policy_args))

    ### 聚合子任务结果
    return dispatcher_poll_result(reply)

@app.task
def up_fallback_match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 调用rebound策略
    """
    logger.info("%s called with arguments: len(codes)=%d, start=%s, end=%s, save_result=%s, policy_args=%s" % ('up_fallback_match', len(codes), start, end, save_result, policy_args))
    return dogen.up_fallback.match(codes, start=start, end=end, save_result=save_result, policy_args=policy_args)

@app.task
def dispatcher_of_up_fallback_match(codes=None, start=None, end=None, save_result=False, policy_args=None, slice=1000):
    """ rs_fallback策略任务拆分

        参数说明：
            start - 样本起始交易日(数据库样本可能晚于该日期, 如更新不全)；若未指定默认取end-$max_days做起始日
            end - 样本截止交易日(数据库样本可能早于该日期, 如停牌)
            save_result - 保存命中结果
        
        返回结果：
            列表数据如[{item-1}, {item-2}, ..., {item-n}]，根据股票的流通市值、收盘价、成交量、涨跌幅等数据决策。
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None
    
    ### 获取代码列表
    if codes is None:
        codes = db.lookup_stock_codes()
        codes.sort()

    ### 拆分任务, 聚合结果
    logger.info('%s called to dispatch %d codes into sub-task with slice=%d' % ('dispatcher_of_up_fallback_match', len(codes), slice))
    reply = []
    while True:
        tasks = len(reply)
        if (tasks * slice) >= len(codes):
            break
        reply.append(celery_dogen.up_fallback_match.delay(codes[tasks*slice:(tasks+1)*slice], start=start, end=end, save_result=save_result, policy_args=policy_args))

    ### 聚合子任务结果
    return dispatcher_poll_result(reply)


@app.task
def up_riseheap_match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 调用rebound策略
    """
    logger.info("%s called with arguments: len(codes)=%d, start=%s, end=%s, save_result=%s, policy_args=%s" % ('up_riseheap_match', len(codes), start, end, save_result, policy_args))
    return dogen.up_riseheap.match(codes, start=start, end=end, save_result=save_result, policy_args=policy_args)

@app.task
def dispatcher_of_up_riseheap_match(codes=None, start=None, end=None, save_result=False, policy_args=None, slice=1000):
    """ rebound策略任务拆分

        参数说明：
            start - 样本起始交易日(数据库样本可能晚于该日期, 如更新不全)；若未指定默认取end-$max_days做起始日
            end - 样本截止交易日(数据库样本可能早于该日期, 如停牌)
            save_result - 保存命中结果
        
        返回结果：
            列表数据如[{item-1}, {item-2}, ..., {item-n}]，根据股票的流通市值、收盘价、成交量、涨跌幅等数据决策。
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None
    
    ### 获取代码列表
    if codes is None:
        codes = db.lookup_stock_codes()
        codes.sort()

    ### 拆分任务, 聚合结果
    logger.info('%s called to dispatch %d codes into sub-task with slice=%d' % ('dispatcher_of_up_riseheap_match', len(codes), slice))
    reply = []
    while True:
        tasks = len(reply)
        if (tasks * slice) >= len(codes):
            break
        reply.append(celery_dogen.up_riseheap_match.delay(codes[tasks*slice:(tasks+1)*slice], start=start, end=end, save_result=save_result, policy_args=policy_args))

    ### 聚合子任务结果
    return dispatcher_poll_result(reply)

@app.task
def hl_twoserial_match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 调用hl_twoserial策略
    """
    logger.info("%s called with arguments: len(codes)=%d, start=%s, end=%s, save_result=%s, policy_args=%s" % ('hl_twoserial_match', len(codes), start, end, save_result, policy_args))
    return dogen.hl_twoserial.match(codes, start=start, end=end, save_result=save_result, policy_args=policy_args)

@app.task
def dispatcher_of_hl_twoserial_match(codes=None, start=None, end=None, save_result=False, policy_args=None, slice=1000):
    """ rebound策略任务拆分

        参数说明：
            start - 样本起始交易日(数据库样本可能晚于该日期, 如更新不全)；若未指定默认取end-$max_days做起始日
            end - 样本截止交易日(数据库样本可能早于该日期, 如停牌)
            save_result - 保存命中结果
        
        返回结果：
            列表数据如[{item-1}, {item-2}, ..., {item-n}]，根据股票的流通市值、收盘价、成交量、涨跌幅等数据决策。
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None
    
    ### 获取代码列表
    if codes is None:
        codes = db.lookup_stock_codes()
        codes.sort()

    ### 拆分任务, 聚合结果
    logger.info('%s called to dispatch %d codes into sub-task with slice=%d' % ('dispatcher_of_hl_twoserial_match', len(codes), slice))
    reply = []
    while True:
        tasks = len(reply)
        if (tasks * slice) >= len(codes):
            break
        reply.append(celery_dogen.hl_twoserial_match.delay(codes[tasks*slice:(tasks+1)*slice], start=start, end=end, save_result=save_result, policy_args=policy_args))

    ### 聚合子任务结果
    return dispatcher_poll_result(reply)

@app.task
def up_ma20touch_match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 调用up_ma20touch策略
    """
    logger.info("%s called with arguments: len(codes)=%d, start=%s, end=%s, save_result=%s, policy_args=%s" % ('up_ma20touch_match', len(codes), start, end, save_result, policy_args))
    return dogen.up_ma20touch.match(codes, start=start, end=end, save_result=save_result, policy_args=policy_args)

@app.task
def dispatcher_of_up_ma20touch_match(codes=None, start=None, end=None, save_result=False, policy_args=None, slice=1000):
    """ up_ma20touch策略任务拆分

        参数说明：
            start - 样本起始交易日(数据库样本可能晚于该日期, 如更新不全)；若未指定默认取end-$max_days做起始日
            end - 样本截止交易日(数据库样本可能早于该日期, 如停牌)
            save_result - 保存命中结果
        
        返回结果：
            列表数据如[{item-1}, {item-2}, ..., {item-n}]，根据股票的流通市值、收盘价、成交量、涨跌幅等数据决策。
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None
    
    ### 获取代码列表
    if codes is None:
        codes = db.lookup_stock_codes()
        codes.sort()

    ### 拆分任务, 聚合结果
    logger.info('%s called to dispatch %d codes into sub-task with slice=%d' % ('dispatcher_of_up_ma20touch_match', len(codes), slice))
    reply = []
    while True:
        tasks = len(reply)
        if (tasks * slice) >= len(codes):
            break
        reply.append(celery_dogen.up_ma20touch_match.delay(codes[tasks*slice:(tasks+1)*slice], start=start, end=end, save_result=save_result, policy_args=policy_args))

    ### 聚合子任务结果
    return dispatcher_poll_result(reply)