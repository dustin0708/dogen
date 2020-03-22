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
def daily_pull_update_kdata(codes, full=False, start=None, end=None):
    """ 执行股票数据更新
    """
    logger.info("%s called with arguments: len(codes)=%d, full=%s, start=%s, end=%s" % ('daily_pull_update_kdata', len(codes), full, start, end))
    return dogen.daily_pull.update_kdata(codes, full=full, start=start, end=end)

@app.task
def dispatcher_of_daily_pull_update_kdata(codes=None, full=False, start=None, end=None, slice=1000):
    """ 拆分股票更新任务
    """
    try:
        if codes is None:
            ### 下载股票代码数据
            basics = dogen.download_basics()
            codes = basics.index.tolist()
            codes.sort()

            ### 添加指数
            codes.append(dogen.get_index_of_sh())
            codes.append(dogen.get_index_of_sz())
    except Exception:
        traceback.print_exc()
        return None

    ### 分配任务, 聚合结果
    logger.info('%s called to dispatch %d codes into sub-task with slice=%d' % ('dispatcher_of_daily_pull_update_kdata', len(codes), slice))
    reply = []
    while True:
        tasks = len(reply)
        if (tasks * slice) >= len(codes):
            break
        reply.append(celery_dogen.daily_pull_update_kdata.delay(codes[tasks*slice:(tasks+1)*slice], full=full, start=start, end=end))

    result = []
    for i in range(0, len(reply)):
        while not reply[i].ready():
            time.sleep(0.05)
            continue
        result.extend(reply[i].result)
        
    return result


@app.task
def daily_update_hot_concept(end=None, num=1, save_result=False):
    return dogen.daily_pull.update_hot_concept(end=end, num=num, save_result=save_result)