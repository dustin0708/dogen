#-*-coding:utf-8-*-

import sys
import time
import dogen
import traceback
import celery_dogen

### 导入当前模块变量
from . import app
from . import logger, mongo_server, mongo_database

@app.task
def daily_statistics_find_largerise_range(codes, start=None, end=None, save_result=False, args=[20, 2]):
    """ 执行统计股票上涨区间任务
    """
    logger.info("%s called with arguments: len(codes)=%d, start=%s, end=%s, save_result=%s" % ('daily_statistics_find_largerise_range', len(codes), start, end, save_result))
    return dogen.daily_statistics.find_largerise_range(codes, start=start, end=end,  save_result=save_result, args=args)

@app.task
def dispatcher_of_daily_statistics_find_largerise_range(codes=None, start=None, end=None, save_result=False, slice=1000):
    """ 拆分统计股票上涨区间任务
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

    ### 分配任务, 聚合结果
    logger.info('%s called to dispatch %d codes into sub-task with slice=%d' % ('dispatcher_of_daily_statistics_find_largerise_range', len(codes), slice))
    reply = []
    while True:
        tasks = len(reply)
        if (tasks * slice) >= len(codes):
            break
        reply.append(celery_dogen.daily_statistics_find_largerise_range.delay(codes[tasks*slice:(tasks+1)*slice], start=start, end=end, save_result=save_result))

    result = []
    for i in range(0, len(reply)):
        while not reply[i].ready():
            time.sleep(0.05)
            continue
        result.extend(reply[i].result)
    
    return result