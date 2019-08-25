#-*-coding:utf-8-*-

from Script.Core import Sched
import time

def print_interval(args):
    print(str(time.time()) + " " + args)
    
def print_interval2(args):
    print(str(time.time()) + " 2 " + args)
    
mgr=Sched.Manager()


print("start sched already")

#mgr.AddIntervalTask(print_interval, args=['hello'], id='t1', seconds=2)
#mgr.AddIntervalTask(print_interval, args=['hello2'], id='t2', seconds=2)
#mgr.DelTask(print_interval, id='t1')

#mgr.AddDatetimeTask(print_interval, args=['once task'])

mgr.AddCronweekTask(print_interval, args=['cron test'], day_of_week='0,1,2,3,4,5,6', hour='13', minute='00', second='0-59')

mgr.Start()
