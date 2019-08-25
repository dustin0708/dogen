#-*-coding:utf-8-*-

import sys
import threading
import threadpool

from apscheduler.schedulers.blocking    import BlockingScheduler
from apscheduler.schedulers.background  import BackgroundScheduler

class Manager(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        
        self.scheduler =  BackgroundScheduler()
        self.jobs = {}
        self.lock = threading.RLock()
    
    def __add_job(self, cb_task, handler, id):
        """记录定时任务
        """
        self.lock.acquire()
        try:
            lst = self.jobs[cb_task]
        except Exception:
            lst = []            
        job = (handler, id)
        lst.append(job)
        self.jobs[cb_task] = lst
        self.lock.release()

    def __del_job(self, cb_task, id):
        """停止定时任务
        """
        self.lock.acquire()
        try:
            lst = self.jobs[cb_task]
        except Exception:
            lst = []
        
        rmv = []
        for job in lst:
            if ((id is not None) and (job[1] == id)) or (id is None):
                rmv.append(job)
            pass
        
        for j in rmv:
            try:
                j[0].remove()
            except Exception:
                pass
            lst.remove(j)
        
        self.lock.release()
        
    def AddCronweekTask(self, cb_task, args=None, id=None, day_of_week='0,1,2,3,4,5,6', hour='0', minute='0', second='0', start_date=None):
        """创建定时任务
        
        day_of_week: '0-4' or '0,1,2,3,4'
        hour: '0-23' or '0,1,2'
        minute/sesond: '0-59' or '0,1,2'
        """
        if (args is not None) and (not isinstance(args, list)):
            args = [args]
        handler = self.scheduler.add_job(cb_task, 'cron', args=args, id=id, day_of_week=day_of_week, hour=hour, minute=minute, second=second, start_date=start_date)
        self.__add_job(cb_task, handler, id)
        pass
    
    def AddIntervalTask(self, cb_task, args=None, id=None, weeks=0, days=0, hours=0, minutes=0, seconds=0, start_date=None):
        """创建周期任务
        """
        if (args is not None) and (not isinstance(args, list)):
            args = [args]
        handler = self.scheduler.add_job(cb_task, 'interval', args=args, id=id, weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds, start_date=start_date)
        self.__add_job(cb_task, handler, id)
        
    def AddDatetimeTask(self, cb_task, args=None, id=None, date=None, time=None):
        """创建一次性任务
        """
        datetime = None
        if date is not None:
            datetime = ""
            datetime += date
            if time is not None:
                datetime += " "
                datetime += time
            pass
            
        if (args is not None) and (not isinstance(args, list)):
            args = [args]
            
        if datetime is not None:
            handler = self.scheduler.add_job(cb_task, 'date', args=args, id=id, run_date=datetime)
        else:
            handler = self.scheduler.add_job(cb_task, 'date', args=args, id=id)
            
        self.__add_job(cb_task, handler, id)
    
    def DelTask(self, cb_task, id=None):
        """删除任务
        """
        self.__del_job(cb_task, id)
    
    def GetTask(self):
        """获取任务信息
        """
        pass
    
    def run(self):
        """线程执行入口
        """
        self.scheduler.start()
        
    def Start(self):
        """启动任务调度, 立即返回
        """
        self.start()
        
    def Stop(self):
        """停止任务调度
        """
        self.scheduler.shutdown()
        self.join()

        
if __name__ == "__main__":
	print("Welcome to " +  sys.argv[0] + " package.")
