#-*-coding:utf-8-*-

import os
import sys
import logging

sys.path.append(os.getcwd())
import Script.Loader as Loader

def main():
    ## 日志默认配置
    LOG_LEVEL = logging.DEBUG
    LOG_FMT   = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s"
    LOG_FILE  = "Var/dogen.log"
    
    ## 添加日志文件
    logfile = logging.FileHandler(LOG_FILE)
    logfile.setLevel(LOG_LEVEL)
    logfile.setFormatter(logging.Formatter(LOG_FMT))

    ## 配置root日志
    logger = logging.getLogger()
    logger.setLevel(level=LOG_LEVEL)
    logger.addHandler(logfile)
    
    env = Loader.Env()
    env.AddKdataDbFile('Data/kdata_dbfile.h5')
    env.LoopExecute()
    
        
if __name__ == "__main__":
    main()