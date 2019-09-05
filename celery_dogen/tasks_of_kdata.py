#-*-coding:utf-8-*-

import sys
import dogen

### 导入当前模块app
from . import app

@app.task
def update_stock_kdata_from_network(codes, full=False, start=None, end=None):
    return dogen.update_stock_kdata_from_network(codes, full=full, start=start, end=end)