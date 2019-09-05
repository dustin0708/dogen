#-*-coding:utf-8-*-

import sys
import math
import dogen
import celery_dogen

### 导入当前模块app
from . import app

@app.task
def daily_pull_update_kdata(codes, full=False, start=None, end=None):
    """ 执行股票数据更新
    """
    return dogen.daily_pull.update_kdata(codes, full=full, start=start, end=end)

@app.task
def dispatcher_of_daily_pull_update_kdata(full=False, start=None, end=None, slice=1000):
    """ 拆分股票更新任务
    """
    try:
        ### 下载股票代码数据
        basics = dogen.download_basics()
        codes = basics.index.tolist()
    except Exception:
        traceback.print_exc()
        return None

    ### 分配任务, 聚合结果
    tasks = (int)(math.ceil(len(codes)/slice))
    reply = []
    for i in range(0, tasks):
        reply.append(celery_dogen.daily_pull_update_kdata.delay(codes[i*slice:(i+1)*slice], full=full, start=start, end=end))

    result = []
    for i in range(0, len(reply)):
        while not reply[i].ready():
            continue
        codes.extend(reply[i].result)
        
    return "Success update %d/%d" % (len(result), len(codes))
