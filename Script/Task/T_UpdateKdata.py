#-*-coding:utf-8-*-

import sys
import logging

from Script import *

full_update_date = '2019-08-06'
full_update_from = '2018-01-01'

def send_full_event_update_kdata(env):
    evtmgr = env.GetEventManager()
    evtmgr.PushEvent(Comm.EVT_UPDATE_KDATA, arg_list=[env.GetKdataDbs(), full_update_from])

def send_incr_event_update_kdata(env):
    today = Dtime.DateToday()
    start = Dtime.DateDelta(today, -60)
    
    evtmgr = env.GetEventManager()
    evtmgr.PushEvent(Comm.EVT_UPDATE_KDATA, arg_list=[env.GetKdataDbs(), start])
    
def register(env):
    """注册定时器
    """
    sched = env.GetSchedManager()
    sched.AddDatetimeTask(send_full_event_update_kdata, args=env, date=full_update_date, time='02:30:00')
    #sched.AddDatetimeTask(send_full_event_update_kdata, args=env, date=full_update_date)
    sched.AddCronweekTask(send_incr_event_update_kdata, args=env, day_of_week='0-5', hour='00', minute='50', second='00', start_date=Dtime.DateDelta(full_update_date, 1))
    #sched.AddDatetimeTask(send_incr_event_update_kdata, args=env)

TASK_REGISTER = register

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.")