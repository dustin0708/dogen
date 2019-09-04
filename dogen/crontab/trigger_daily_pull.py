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

@app.task
def dispatch_tasks_of_daily_pull(slice=1000):
    """ 周期性执行从网络侧更新股票数据的任务
    """
    ### 任务启动打印
    logger.info("Start to update stocks' kdata.")

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
        reply.append(dogen.kdata.daily_pull.update_stock_kdata_from_network_decorator.delay(code_all[i*slice:(i+1)*slice]))

    code_rst = []
    for i in range(0, len(reply)):
        while not reply[i].ready():
            continue
        code_rst.append(reply[i].get())

    ### 任务结束打印结果
    logger.info("Success in updating %d/%d stocks' kdata." % (len(code_rst), len(code_all)))

    return None

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.") 
    
