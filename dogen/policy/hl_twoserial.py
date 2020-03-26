#-*-coding:utf-8-*-

import sys
import math
import dogen
import numpy
import traceback

### 导入日志句柄
from dogen import logger, mongo_server, mongo_database

""" 参数说明：
        * max_trades: 自然日数（交易日和非交易日），若start取有效值，该字段无效
        * take_valid: 命中交易日有效期, 0表示最后一天命中有效
        * hl_valid: 最后一个涨停有效交易日数
        * volume_scale: 涨停后一交易日上涨时，放量最小倍数
        * mini_falls： 回调最小幅度，单位1%
        * max_rise: 涨停之前最大涨幅
"""

### 策略参数名
MAX_TRADES  = 'max_trades'
MINI_HL     = 'mini_hl'
HL_VALID    = 'hl_valid'
TAKE_VALID  = 'take_valid'
MAX_RISE    = 'max_rise'
MAX_PCLOSE  = 'max_pclose'
OUTSTANDING = 'outstanding'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAX_TRADES: 90,      # 天
    MINI_HL: 3,      # 
    HL_VALID: 7,        #
    TAKE_VALID: 0,  # 倍
    MAX_RISE: 50,   # 1%
    MAX_PCLOSE: 50,
    OUTSTANDING: 100,
}

def __parse_policy_args(policy_args, arg_name):
    try:
        arg_value = policy_args[arg_name]
    except Exception:
        arg_value = ARGS_DEAULT_VALUE[arg_name]
    return arg_value

def score_analyze(basic, kdata, pick_index, take_index, policy_args):
    """ 根据股票股价、市值、成交量等方面给股票打分:
            * 股价估分，总计25分；
            * 市值估分，总计25分；
            * 吸盘估分，总计50分；
    """
    max_pclose  = __parse_policy_args(policy_args, MAX_PCLOSE)
    outstanding = __parse_policy_args(policy_args, OUTSTANDING)
    hl_valid    = __parse_policy_args(policy_args, HL_VALID)

    score  = dogen.score_by_pclose(25, kdata.iloc[take_index][dogen.P_CLOSE], max_pclose)
    score += dogen.score_by_outstanding(25, kdata.iloc[take_index][dogen.P_CLOSE]*basic[dogen.OUTSTANDING], outstanding)

    temp_score = 50.0
    temp_slice = temp_score / hl_valid
    if (pick_index <= hl_valid):
        score += (pick_index * temp_slice)

    return (int)(score)

def exclude_analyze(basic, kdata, pick_index, take_index, policy_args):
    """ 根据日线做排除性校验
    """
    max_rise    = __parse_policy_args(policy_args, MAX_RISE)
    max_pclose   = __parse_policy_args(policy_args, MAX_PCLOSE)
    outstanding  = __parse_policy_args(policy_args, OUTSTANDING)

    ### 取回调最低价
    mini_index = dogen.get_last_column_min(kdata, dogen.P_CLOSE, sIdx=take_index, eIdx=pick_index)

    ### 净资产为负数的
    if basic[dogen.BVPS] < 0:
        logger.debug("Invalid bvps")
        #return True
        
    ### 特征三
    if kdata.iloc[take_index][dogen.P_CLOSE] > max_pclose:
        logger.debug("Too high close price at %s" % kdata.index[take_index])
        return True
    if kdata.iloc[take_index][dogen.P_CLOSE] * basic[dogen.OUTSTANDING] > outstanding:
        logger.debug("Too large outstanding at %s" % kdata.index[take_index])
        return True

    ### 特征四
    rise_range = dogen.get_last_rise_range(kdata, max_rise, max_fall=max_rise/2, eIdx=22)
    if rise_range is not None:
        [min_index, max_index, inc_close, get_lhigh, tmp_index] = rise_range
        if pick_index <= min_index and pick_index >= max_index:
            logger.debug("Too large rise-range")
            return True
        pass

    return False

def include_analyze(basic, kdata, policy_args):
    ### 策略参数处理
    mini_hl     = __parse_policy_args(policy_args, MINI_HL)
    hl_valid    = __parse_policy_args(policy_args, HL_VALID)
    take_valid  = __parse_policy_args(policy_args, TAKE_VALID)

    ### 特征一
    heap_trade = 0
    if kdata.index.size < hl_valid:
        hl_valid = kdata.index.size
    for temp_index in range(0, hl_valid):
        if kdata.iloc[temp_index][dogen.P_CLOSE] < kdata.iloc[temp_index][dogen.L_HIGH]:
            continue
        if kdata.iloc[temp_index+1][dogen.P_CLOSE] < kdata.iloc[temp_index+1][dogen.L_HIGH]:
            continue
        pick_index = temp_index
        heap_trade+= 1
    if heap_trade != 1:
        logger.debug("Don't include valid serial hl-trade")
        return None
    if pick_index == 0:
        low_index = dogen.get_last_column_min(kdata, dogen.P_CLOSE, eIdx=pick_index+1)
        if kdata.iloc[low_index][dogen.P_CLOSE] > kdata.iloc[pick_index][dogen.P_CLOSE]:
            logger.debug("Don't include valid fallback trade")
            return None

        ### 特征二
        take_index = None
        for temp_index in range(low_index, -1, -1):
            if dogen.caculate_incr_percentage(kdata.iloc[temp_index][dogen.P_CLOSE], kdata.iloc[pick_index][dogen.P_CLOSE]) < -3:
                continue
            take_index = temp_index
    else:
        ### 两连板
        take_index = pick_index
    if take_index is None or take_index > take_valid:
        logger.debug("Don't match valid take trade")
        return None
    
    return [pick_index, take_index]

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
    result[dogen.RST_COL_TAKE_TRADE]  = kdata.index[take_index] # 命中交易日
    result[dogen.RST_COL_LAST_CLOSE]  = kdata.iloc[0][dogen.P_CLOSE] # 最后一日收盘价
    result[dogen.RST_COL_OUTSTANDING] = round(kdata.iloc[0][dogen.P_CLOSE] * basic[dogen.OUTSTANDING], 2) # 流通市值
    result[dogen.RST_COL_SCORE]       = score_analyze(basic, kdata, pick_index, take_index, policy_args) # 打分
    result[dogen.RST_COL_MATCH_TIME]  = dogen.datetime_now() # 选中时间
    result[dogen.RST_COL_INDEX]       = '%s_%s' % (basic.name, kdata.index[take_index]) # 唯一标识，用于持久化去重

    return result

def match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 两连涨停策略, 满足条件：
        >>> 基本条件
            一 两周内两连板上涨;
                1) 存在次板收盘价之下的回调;
                2) 次板非烂板(暂无法判断)
            二 买入信号(take-trade)，有效期由take_valid限定:
                1) 收盘价在次板收盘价0.97倍之上，且涨幅小于7%；
        
        >>> 排它条件
            三 股价市值在outstanding(100亿)和maxi_close(50以下)限制范围内
            四 股价成本合理：
                1) 在最近一个月内，最高涨幅由maxi_rise限制； 
    
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

    ### 截至日期修正
    expon = db.lookup_stock_kdata(dogen.get_index_of_sh(), end=end)
    if expon is None:
        logger.error("Don't get expon data")
        return None
    else:
        end = expon.index[0]
        start = dogen.date_delta(end, -__parse_policy_args(policy_args, MAX_TRADES))

    ### 依次策略检查
    match_list = []
    for code in codes:
        try:
            ### 从数据库读取basic数据
            basic = db.lookup_stock_basic(code)
            if dogen.drop_stock_check(code, basic):
                continue

            ### 从数据库读取日线数据，必须按索引（日期）降序排列
            kdata = db.lookup_stock_kdata(code, start=start, end=end)
            if kdata is None:
                continue
            elif kdata.index[0] != end:
                logger.debug("End date don't match")
                continue
            else:
                dogen.drop_fresh_stock_trades(basic, kdata)

            ### 策略分析
            if kdata is not None and kdata.index.size > 0:
                logger.debug("Begin in analyzing %s from %s to %s" % (code, kdata.index[-1], kdata.index[0]))
                match = stock_analyze(basic, kdata, policy_args)
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

