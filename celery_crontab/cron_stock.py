#-*-coding:utf-8-*-

import sys
import math
import pymongo
import traceback


from celery import Celery
from celery.task.schedules import crontab 
from celery.utils.log import get_task_logger

import dogen
from celery_workers import stock

### 日志句柄
logger = get_task_logger(__name__)

app = Celery(__name__, broker='pyamqp://127.0.0.1')


@app.on_after_configure.connect
def startup_stock_daily_update():
    """ 启动周期任务
    """
    sender.add_periodic_task(crontab(day_of_week='1-6', hour='5', minute='0'), stock_daily_update(), )
    return None

@app.task
def stock_daily_update(slice=1000):
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
    code_rst = []
    for i in range(0, tasks):
        code_rst.append(stock.download_kdata(code_all[i*slice:(i+1)*slice]))
    
    ### 任务结束打印结果
    logger.info("Success in updating %d/%d stocks' kdata." % (len(code_rst), len(code_all)))

    return None

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.") 
    
