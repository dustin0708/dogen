#-*-coding:utf-8-*-

import sys
import time
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


### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAXI_DAYS: 45,      # 天
    TAKE_VALID: 10,     # 
}

def __parse_policy_args(policy_args, arg_name):
    try:
        arg_value = policy_args[arg_name]
    except Exception:
        arg_value = ARGS_DEAULT_VALUE[arg_name]
    return arg_value

def exclude_analyze(basic, kdata, pick_index, take_index, policy_args):
    """ 根据日线做排除性校验
    """
    if kdata.iloc[0][dogen.MA5] < kdata.iloc[1][dogen.MA5]:
        logger.debug("Invalid MA5 at %s" % kdata.index[0])
        return True

    return False

def include_analyze(basic, kdata, policy_args):
    ### 参数解析
    take_valid = __parse_policy_args(policy_args, TAKE_VALID)

    ### 特征一
    time_market= time.strftime("%Y-%m-%d", time.strptime(basic.loc['timeToMarket'], "%Y%m%d"))
    if time_market > kdata.index[0] or time_market < kdata.index[-1]:
        logger.debug("Isn't a new-ipo stock")
        return None
    else:
        dogen.drop_fresh_stock_trades(basic, kdata)

    ### 特征三：
    take_index = None
    heap_rises = 0
    for i in range(kdata.index.size-1, -1, -1):
        if kdata.iloc[i][dogen.P_CLOSE] >= kdata.iloc[i][dogen.L_HIGH] > 0:
            take_index = i
        if kdata.iloc[i][dogen.R_CLOSE] < 0:
            heap_rises = 0
        else:
            heap_rises+= kdata.iloc[i][dogen.R_CLOSE]
        if heap_rises >= 9.99:
            take_index = i
        pass
    if take_index is None or take_index > take_valid:
        logger.debug("Don't get valid take-trade")
        return None
    
    return [take_index, take_index]

def stock_analyze(basic, kdata, policy_args):
    ### 基本条件选取
    get_index = include_analyze(basic, kdata, policy_args)
    if get_index is None:
        logger.debug("include_analyze() return None")
        return None
    else:
        [pick_index, take_index] = get_index

    ### 排它条件过滤
    if exclude_analyze(basic, kdata, pick_index, take_index, policy_args):
        logger.debug("exclude_analyze() return True")
        return None

    ### 构造结果
    result = {}
    result[dogen.RST_COL_CODE]        = basic.name # 股票代码
    result[dogen.RST_COL_NAME]        = basic[dogen.NAME] #  证券简写
    result[dogen.RST_COL_INDUSTRY]    = basic[dogen.INDUSTRY]
    result[dogen.RST_COL_TAKE_TRADE]  = kdata.index[-1] # 命中交易日
    result[dogen.RST_COL_LAST_CLOSE]  = kdata.iloc[0][dogen.P_CLOSE] # 最后一日收盘价
    result[dogen.RST_COL_OUTSTANDING] = round(kdata.iloc[0][dogen.P_CLOSE] * basic[dogen.OUTSTANDING], 2) # 流通市值
    result[dogen.RST_COL_SCORE]       = 0
    result[dogen.RST_COL_MATCH_TIME]  = dogen.datetime_now() # 选中时间
    result[dogen.RST_COL_INDEX]       = '%s_%s' % (basic.name, kdata.index[-1]) # 唯一标识，用于持久化去重

    return result

def match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 上市新股开板策略, 满足特征：
            一 $MAXI_DAYS交易日内开板
            二 买入信号take-trade:
                1) 涨停;
                2) 连续放量上涨超过10个点;
            三 上涨趋势：MA5上涨


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

    ### 依次策略检查
    match_list = []
    for code in codes:
        try:
            ### 从数据库读取basic数据
            basic = db.lookup_stock_basic(code)
            if dogen.drop_stock_check(code, basic):
                continue

            ### 从数据库读取日线数据，必须按索引（日期）降序排列
            if end is None:
                end = dogen.date_today()
            if start is None:
                start = dogen.date_delta(end, -__parse_policy_args(policy_args, MAXI_DAYS))
            kdata = db.lookup_stock_kdata(code, start=start, end=end)
            if kdata is None:
                continue
            kdata.sort_index(ascending=False, inplace=True)

            ### 策略分析
            if kdata is not None and kdata.index.size > 0:
                logger.debug("Begin in analyzing %s from %s to %s" % (code, kdata.index[-1], kdata.index[0]))
                match = stock_analyze(basic, kdata, policy_args)
                if match is None:
                    continue
                pass
            
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
