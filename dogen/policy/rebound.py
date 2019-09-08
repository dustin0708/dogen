#-*-coding:utf-8-*-

import sys
import math
import dogen
import traceback

### 导入日志句柄
from dogen import logger, mongo_server, mongo_database

""" 参数说明：
        * maxi_days: 自然日数（交易日和非交易日），若start取有效值，该字段无效
        * take_valid: 命中交易日有效期, 0表示最后一天命中有效
        * hl_valid: 最后一个涨停有效交易日数
        * volume_scale: 涨停后一交易日上涨时，放量最小倍数
        * mini_falls： 回调最小幅度，单位1%
        * maxi_prerise: 涨停之前最大涨幅
"""

### 策略参数名
MAXI_DAYS   = 'maxi_days'
TAKE_VALID  = 'take_valid'
HL_VALID    = 'hl_valid'
VOLUME_SCALE= 'volume_scale'
MINI_FALLS  = 'mini_falls'
MAXI_PRERISE= 'maxi_prerise'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAXI_DAYS: 30,      # 天
    TAKE_VALID: 0,      # 
    HL_VALID: 4,        #
    VOLUME_SCALE: 1.2,  # 倍
    MINI_FALLS: 3.99,   # 1%
    MAXI_PRERISE: 30,   # 1%
}

def __parse_policy_args(policy_args, arg_name):
    try:
        arg_value = policy_args[arg_name]
    except Exception:
        arg_value = ARGS_DEAULT_VALUE[arg_name]
    return arg_value

def match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 反弹策略, 有如下特征：
            * $MAXI_DAYS交易日内，下跌;
            * 涨停后紧接着最多上涨一天, 若上涨必须放量$mini_scale倍;
            * 累积下跌等于或大于$mini_falls;
            * 最后一日MA5上涨;

        参数说明：
            start - 样本起始交易日(数据库样本可能晚于该日期, 如更新不全)；若未指定默认取end-$max_days做起始日
            end - 样本截止交易日(数据库样本可能早于该日期, 如停牌)
            save_result - 保存命中结果
        
        返回结果：
            列表数据如[{item-1}, {item-2}, ..., {item-n}]，根据股票的流通市值、收盘价、成交量、涨跌幅等数据决策。
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None

    ### 股票代码过滤，如科创板
    codes = dogen.drop_codes(codes)

    ### 依次策略检查
    match_list = []
    for code in codes:
        try:
            ### 从数据库读取basic数据
            basic = db.lookup_stock_basic(code)

            ### 从数据库读取日线数据，必须按索引（日期）降序排列
            if end is None:
                end = dogen.date_today()
            if start is None:
                start = dogen.date_delta(end, -__parse_policy_args(policy_args, MAXI_DAYS))
            kdata = db.lookup_stock_kdata(code, start=start, end=end)
            kdata.sort_index(ascending=False, inplace=True)
            dogen.drop_fresh_stock_trades(basic, kdata)

            ### 策略分析
            logger.debug("Begin in analyzing %s from %s to %s" % (code, start, end))
            match = __policy_analyze(basic, kdata, policy_args)
            if match is None:
                continue
            
            ### 输出结果
            match_list.append(match)
        except Exception:
            logger.error('Trggered in handling code %s: %s' % (code, traceback.format_exc()))
            continue
        pass
    
    ### 保存结果到数据库
    if save_result and len(match_list) > 0:
        db.insert_policy_result(__name__.split('.')[-1], match_list, key_name=dogen.RST_COL_INDEX)

    return match_list
