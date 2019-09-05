#-*-coding:utf-8-*-

import sys
import dogen

### 导入当前模块app
from . import app

@app.task
def hl_fallback_match(codes, start=None, end=None, max_days=60, save_result=True, take_valid=0, maxi_trade=5, mini_scale=1.2, mini_falls=4):
    return dogen.hl_fallback.match(codes, start=start, end=end, max_days=max_days, save_result=save_result, take_valid=take_valid, maxi_trade=maxi_trade, mini_scale=mini_scale, mini_falls=mini_falls)
