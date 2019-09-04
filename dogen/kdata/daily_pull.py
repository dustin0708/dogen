#-*-coding:utf-8-*-

import sys
import dogen
import pymongo
import traceback

from celery import Celery
from celery.utils.log import get_task_logger

### 从外部程序导入
from celery_dogen import app

### 日志句柄
logger = get_task_logger(__name__)

@app.task
def update_stock_kdata_from_network(codes, full=False, start=None, end=None):
    """ 从网络侧更新股票数据

        参数说明：
            codes - 更新股票列表
            full - 是否进行全量更新
            start - 起始交易日
            end - 截止交易日
        
        返回结果：
            成功更新数据列表
    """
    try:
        db = dogen.DbMongo()
    except Exception:
        logger.error(traceback.format_exc())
        return None

    ### 设置截止日期
    if end is None:
        end = dogen.date_today()
    
    ### 下载basic数据
    basics = dogen.download_basics()
    
    ### 下载参数指定股票数据
    success_list = []
    for code in codes:
        ### 如果全量下载, 则清除数据库
        try:
            if full:
                db.delete_stock_basic(code=code)
                db.delete_stock_kdata(code)

                from_trade = None
                last_trade = None            
            else:
                ### 读取数据区间
                from_trade, last_trade = db.lookup_stock_kdata_last(code)
        
            ### 区间数据不存在, 默认下载end之前一年数据
            if full or from_trade is None or last_trade is None:
                if start is None:
                    start = dogen.date_delta(end, -365)
                pass
            else:
                ### 增量下载需要保证数据处理加工的正确性（MA20）
                start = dogen.date_delta(last_trade, -30)
            
            ### 取basic
            basic = basics.loc[code]
        
            ### 下载日线数据
            kdata = dogen.download_kdata(basic, start=start, end=end)
            
            ### 截取新增数据插入, 数据已存在会导致出错
            if from_trade is not None and last_trade is not None:
                kdata = kdata.loc[(kdata.index<from_trade) | (kdata.index>last_trade)]
            
            ### 写数据库
            db.insert_stock_basic(code, basic)
            db.insert_stock_kdata(code, kdata, kdata.index)
            success_list.append(code)
        except Exception:
            continue
        pass
        
    return success_list

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.") 
    
