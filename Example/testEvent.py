#-*-coding:utf-8-*-

from Script.Core import Event

def print_msg1(arg1):
    print(arg1)

def print_msg2(arg1, arg2):
    print(arg1, arg2)

def print_msg3(arg1, arg2, arg3):
    print(arg1, arg2, arg3)
    
mgr = Event.Manager()
mgr.AddHandler('f1', print_msg1, 1)
mgr.AddHandler('f2', print_msg2, 2)
mgr.AddHandler('f3', print_msg3, 3)
mgr.PushEvent('f1', 'hello')
mgr.PushEvent('f2', ['hello', 'the world'])
mgr.PushEvent('f3', ['hello', 'the world', '!'])
#mgr.PushEvent(1, 'the')
#mgr.PushEvent(1, 'world')

mgr.LoopForever()