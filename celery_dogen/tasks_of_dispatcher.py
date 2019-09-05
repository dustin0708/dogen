#-*-coding:utf-8-*-

import sys
import math
import dogen
import celery_dogen

### 导入当前模块app
from . import app

@app.task
def dispatch_tasks_of_hl_fallback(slice=1000):
    """ 运行涨停回调策略
    """
    ### 任务启动打印
    print("Start to dispatch policy-hl_fallback tasks.")

    try:
        db = dogen.DbMongo()
    except Exception:
        traceback.print_exc()
        return None
    
    ### 下载股票代码数据
    basics = dogen.download_basics()
    code_all = basics.index.tolist()

    ### 分配任务, 聚合结果
    tasks = (int)(math.ceil(len(code_all)/slice))
    reply = []
    for i in range(0, tasks):
        reply.append(celery_dogen.hl_fallback_match.delay(code_all[i*slice:(i+1)*slice]))

    result = []
    for i in range(0, len(reply)):
        while not reply[i].ready():
            continue
        result.extend(reply[i].result)

    ### 任务结束打印结果
    print("Success in policy-hl_fallback")

    return result

@app.task
def dispatch_tasks_of_daily_pull(slice=1000):
    """ 周期性执行从网络侧更新股票数据的任务
    """
    ### 任务启动打印
    print("Start to update stocks' kdata.")

    try:
        db = dogen.DbMongo()
    except Exception:
        traceback.print_exc()
        return None
    
    ### 下载股票代码数据
    basics = dogen.download_basics()
    code_all = basics.index.tolist()

    ### 分配任务, 聚合结果
    tasks = (int)(math.ceil(len(code_all)/slice))
    reply = []
    for i in range(0, tasks):
        reply.append(celery_dogen.update_stock_kdata_from_network.delay(code_all[i*slice:(i+1)*slice]))

    code_rst = []
    for i in range(0, len(reply)):
        while not reply[i].ready():
            continue
        code_rst.extend(reply[i].result)

    ### 任务结束打印结果
    print("Success in updating %d/%d stocks' kdata." % (len(code_rst), len(code_all)))

    return code_rst
