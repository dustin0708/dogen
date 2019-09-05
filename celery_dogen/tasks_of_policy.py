#-*-coding:utf-8-*-

import sys
import math
import dogen
import celery_dogen

### 导入当前模块app
from . import app

@app.task
def hl_fallback_match(codes, start=None, end=None, max_days=60, save_result=True, take_valid=0, maxi_trade=5, mini_scale=1.2, mini_falls=4):
    """ 调用hl_fallback策略
    """
    return dogen.hl_fallback.match(codes, start=start, end=end, max_days=max_days, save_result=save_result, take_valid=take_valid, maxi_trade=maxi_trade, mini_scale=mini_scale, mini_falls=mini_falls)

@app.task
def dispatcher_of_hl_fallback_match(codes=None, slice=1000):
    """ hl_fallback策略任务拆分
    """
    ### 初始化本地数据库连接
    try:
        db = dogen.DbMongo()
    except Exception:
        traceback.print_exc()
        return None
    
    ### 获取代码列表
    if codes is None:
        codes = db.lookup_stock_codes()

    ### 拆分任务
    tasks = (int)(math.ceil(len(codes)/slice))
    reply = []
    for i in range(0, tasks):
        reply.append(celery_dogen.hl_fallback_match.delay(codes[i*slice:(i+1)*slice]))

    ### 聚合子任务结果
    result = []
    for i in range(0, len(reply)):
        while not reply[i].ready():
            continue
        result.extend(reply[i].result)

    return result
