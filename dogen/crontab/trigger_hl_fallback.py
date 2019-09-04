#-*-coding:utf-8-*-

import os
import sys
import math
import dogen
import pymongo
import traceback


from celery import Celery
from celery.utils.log import get_task_logger

### 从外部程序导入
from celery_dogen import app

### 日志句柄
logger = get_task_logger(__name__)

def dispatch_tasks_of_hl_fallback(slice=1000):
    """ 运行涨停回调策略
    """
    ### 任务启动打印
    logger.info("Start to dispatch policy-hl_fallback tasks.")

    try:
        db = dogen.DbMongo()
    except Exception:
        traceback.print_exc()
        return None
    
    ### 下载股票代码数据
    basics = dogen.download_basics()
    code_all = basics.index.tolist()

    ### 分配任务, 聚合结果
    tasks = math.ceil(len(code_all)/slice)
    reply = []
    for i in range(0, tasks):
        reply.append(dogen.policy.hl_fallback.match_decorator.delay(code_all[i*slice:(i+1)*slice]))

    result = []
    for i in range(0, len(reply)):
        while not reply[i].ready():
            continue
        result.append(reply[i].get())

    ### 任务结束打印结果
    logger.info("Success in policy-hl_fallback")

    return result

@app.task
def dispatch_tasks_of_hl_fallback_decorator(slice=1000):
    return dispatch_tasks_of_hl_fallback(slice=slice)

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.") 
    
