#-*-coding:utf-8-*-

import sys
import time
import logging
import pkgutil
import importlib
import traceback

from Script import *

class Env():
    def __init__(self):
        
        self.event_mgr = Event.Manager()
        self.sched_mgr = Sched.Manager()
        
        self.kdata_dbmemory = Kdata.DbMemory()
        self.kdata_dbfile = None
        
        self.active = False
        pass
        
    def AddKdataDbFile(self, filepath):
        """增加k线文件数据库, 并同步拷贝到内存数据库
        """
        self.kdata_dbfile = Kdata.DbH5file(filepath)
        Kdata.copy_db(self.kdata_dbfile, self.kdata_dbmemory)
       
    def GetKdataDbs(self):
        """获取k线数据库集
        """
        dbs = [self.kdata_dbmemory]
        if self.kdata_dbfile is not None:
            dbs.append(self.kdata_dbfile)
        return dbs
    
    def GetKdataDbMemory(self):
        """获取k线数据内存数据库
        """
        return self.kdata_dbmemory
        
    def GetEventManager(self):
        """获取事件管理器
        """
        return self.event_mgr
        
    def GetSchedManager(self):
        """获取任务调度管理器
        """
        return self.sched_mgr
    
    def GetHandlerPath(self):
        """获取默认事件处理模块所属目录
        """
        return "Script/Handler"

    def GetTaskPath(self):
        """获取默认定时任务模块所属目录
        """
        return "Script/Task"
    
    def RegHandlerModule(self, module_path):
        """注册事件监听处理模块
            module_path: 模块文件相对路径(不带.py后缀)
        """
        try:
            module = importlib.import_module(module_path.replace('/','.'))
            self.GetEventManager().AddHandler(module.EVENT_TYPE, module.EVENT_HANDLER)
            logging.info("Register handler defined in " + str(module))
            return True
        except Exception:
            traceback.print_exc()
            logging.error("Fail in registering module: " + module_path)
        return False
        
    def __RegHandlers(self):
        """注册事件监听器
        """
        hpath = self.GetHandlerPath()
        
        for (loader, name, ispkg) in pkgutil.iter_modules(path=[hpath]):
            module_path = loader.path + '/' + name
            self.RegHandlerModule(module_path)
        return None

    def RegTimerTaskModule(self, module_path):
        """注册定时任务模块
            module_path: 模块文件相对路径(不带.py后缀)
        """
        try:
            module = importlib.import_module(module_path.replace('/','.'))
            module.TASK_REGISTER(self)
            logging.info("Register task defined in " + str(module))
            return True
        except Exception:
            traceback.print_exc()
            logging.error("Fail in registering module: " + module_path)
        return False
        
    def __RegTasks(self):
        """注册定时任务
        """
        tpath = self.GetTaskPath()
            
        for (loader, name, ispkg) in pkgutil.iter_modules(path=[tpath]):
            module_path = loader.path + '/' + name
            self.RegTimerTaskModule(module_path)
        return None
        
    def LoopExecute(self):
        self.active = True
        
        self.__RegHandlers()
        self.GetEventManager().start()
        
        self.__RegTasks()
        self.GetSchedManager().Start()
        
        try:
            while self.active:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Exitting...")
            
            self.GetEventManager().Stop()
            self.GetSchedManager().Stop()

        pass

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.")