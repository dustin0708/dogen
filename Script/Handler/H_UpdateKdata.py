#-*-coding:utf-8-*-

import sys
import logging

from Script import *

def update_basics_to_db(dbs, basics):
    for db in dbs:
        db.write_basics(basics)
    pass

def update_kdata_to_db(dbs, code, kdata):
    for db in dbs:
        count = db.write_kdata(code, kdata)
        logging.debug("Write kdata " + str(count) + "/" + str(kdata.index.size) + " in " + str(db))
    pass
        
def update_kdata(dbs, start):
    """从网络侧下载最新数据，更新到文件数据库和内存数据库
    """    
    if (len(dbs) <= 0):
        logging.error("Kdata database is not assigned")
        return None
        
    basics = Kdata.download_basics()
    if (basics is None):
        logging.error("Kdata.download_basics() return none")
        return None
    else:
        basics.sort_index(inplace=True)
        update_basics_to_db(dbs,  basics)
    
    logging.info("Update kdata from " + start)
    
    count = 0
    for code in basics.index:
        basic = basics.loc[code]
        logging.debug("Downloading kdata of " + code)
        kdata = Kdata.download_kdata(basic, start=start)
        if (kdata is None) or (kdata.index.size <= 0):
            logging.warning("Fail in downloading kdata of " + code)
            continue
        logging.debug("Trade from " + kdata.index[-1] + " to " + kdata.index[0])
        update_kdata_to_db(dbs, code, kdata)
        count += 1
    logging.info("Update kdata " + str(count) + '/' + str(basics.index.size))

def event_handler(dbs, start):
    try:
        update_kdata(dbs, start)
    except Exception:
        logging.fatal(traceback.format_exc())
    return
    
## 事件监听适配定义:类型, 回调, 参数
EVENT_TYPE    = Comm.EVT_UPDATE_KDATA
EVENT_HANDLER = event_handler

if __name__ == "__main__":
	print("Welcome to " +  sys.argv[0] + " package.")