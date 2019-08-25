#-*-coding:utf-8-*-

import sys
import logging
import traceback
import threading

from concurrent.futures import ThreadPoolExecutor

if sys.version_info.major == 2:
    import Queue as queue
else:
    import queue


class Event(object):
    def __init__(self, type, arg_list=[]):
        """初始化事件, 指定事件类型和参数列表
        """
        self.type = type
        if isinstance(arg_list, list):
            self.arg_list = arg_list
        else:
            self.arg_list = [arg_list]
        pass
        
    def GetType(self):
        """获取事件类型
        """
        return self.type
        
    def GetArgList(self):
        """获取参数列表
        """
        return self.arg_list

        
class Listener(object):
    def __init__(self, type, handler):
        """初始化监听器, 指定监听事件类型和处理器, 同时指定函数参数个数便于校验
        """
        self.type = type
        self.handler = handler
    
    def GetType(self):
        """获取监听事件类型
        """
        return self.type
        
    def GetHandler(self):
        """获取事件处理器
        """
        return self.handler

    def Equals(self, other):
        """判断两个监听器是否相同
        """
        if self.type != other.GetType():
            return False
        if self.handler != other.GetHandler():
            return False
        return True
        
        
class Manager(threading.Thread):
    def __init__(self, queue_size=0, max_workers=10):
        """初始化事件管理器, 参数限制缓存事件数和子任务线程数
        """
        threading.Thread.__init__(self)
        
        self.event_queue = queue.Queue(maxsize=queue_size)
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)

        self.event_listener = {}
        self.__active = False
        pass
        
    def AddHandler(self, type, handler):
        """添加事件监听器
        """
        listener = Listener(type, handler)
        
        try:
            listener_list = self.event_listener[type]
        except Exception:
            listener_list = []

        for i in listener_list:
            if i.Equals(listener):
                return
            pass
            
        listener_list.append(listener)
        self.event_listener[type] = listener_list
        pass

    def DelHandler(self, type, handler=None):
        """删除事件监听器
        """
        listener = Listener(type, handler)

        try:
            listener_list = self.event_listener[type]

            if (handler is None):
                del self.event_listener[type]
                return 
            
            for i in listener_list:
                if i.Equals(listener):
                    listener_list.remove(i)
                pass

        except Exception:
            pass
        pass
    
    def PushEvent(self, type, arg_list=[]):
        """推送事件
        """
        self.event_queue.put(Event(type, arg_list=arg_list))
        pass

    def run(self):
        """循环调度处理事件
        """
        self.__active = True
        while self.__active:
            try:
                event = self.event_queue.get(timeout=1)
            except Exception:
                continue
            
            try:
                listener_list = self.event_listener[event.GetType()]
            except Exception:
                logging.error("Drop event['"+event.GetType()+"'] message without handler")
                continue
            
            for listener in listener_list:                
                stat = self.thread_pool.submit(listener.GetHandler(), *event.GetArgList())
            pass
        
        self.thread_pool.shutdown()


    def Start(self):
        """启动事件处理调度, 立即返回
        """
        self.start()
        
    def Stop(self):
        """终结事件调度
        """
        self.__active = False
        self.join()
    
    
if __name__ == "__main__":
	print("Welcome to " +  sys.argv[0] + " package.")
