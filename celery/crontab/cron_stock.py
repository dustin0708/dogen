#-*-coding:utf-8-*-

import sys
import dogen
import pymongo
import traceback

from celery import Celery
from celery.task.schedules import crontab 
from celery.decorators import periodic_task


app = Celery(__name__)

@periodic_task(run_every=crontab(day_of_week='1-6', hour='01'))
def stock_daily_update():
    """ 周期性执行从网络侧更新股票数据的任务
    """
    try:
        db = dogen.DbMongo()
    except Exception:
        traceback.print_exc()
        return None
        
    print("periodic taks")

    return None

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.") 
    
