#-*-coding:utf-8-*-

import sys
import logging

from Script import *

def push_event(env):
    evtmgr = env.GetEventManager()
    evtmgr.PushEvent(Comm.EVT_POLICY_HL_RISETREND, arg_list=[env.GetKdataDbMemory(), {}, Show.Excel('./Var')])

def register(env):
    """注册定时器
    """
    sched = env.GetSchedManager()
    sched.AddCronweekTask(push_event, args=env, day_of_week='0-5', hour='05', minute='00', second='00', start_date=Dtime.DateToday())
    #sched.AddDatetimeTask(push_event, args=env)

TASK_REGISTER = register

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.")