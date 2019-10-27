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
        * maxi_prerise: 涨停之前最大涨幅
"""

### 策略参数名
MAX_TRADES  = 'max_trades'
TAKE_VALID  = 'take_valid'
PICK_VALID  = 'pick_valid'
MIN_LHIGH   = 'min_lhigh'
MAX_FALLEN  = 'max_fallen'
MIN_RISE    = 'min_rise'
MAX_RISE    = 'max_rise'
MAX_RCLOSE  = 'max_rclose'
MIN_RAMP    = 'min_ramp'
MAX_PCLOSE  = 'max_pclose'
OUTSTANDING = 'market_value'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAX_TRADES: 90,      # 天
    TAKE_VALID: 0,      # 
    PICK_VALID: 10,
    MIN_LHIGH: 0,
    MAX_FALLEN: 10,
    MIN_RISE: 6,
    MAX_RISE: 36,   # 1%
    MAX_RCLOSE: 5,
    MIN_RAMP: 5,
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
            * 涨停估分，总分25分，一个涨停板5分；
    """
    max_pclose  = __parse_policy_args(policy_args, MAX_PCLOSE)
    outstanding = __parse_policy_args(policy_args, OUTSTANDING)

    score  = dogen.score_by_pclose(25, kdata.iloc[take_index][dogen.P_CLOSE], max_pclose)
    score += dogen.score_by_outstanding(25, kdata.iloc[take_index][dogen.P_CLOSE]*basic[dogen.OUTSTANDING], outstanding)

    return (int)(score)

def exclude_analyze(basic, kdata, pick_index, take_index, policy_args):
    min_lhigh   = __parse_policy_args(policy_args, MIN_LHIGH)
    max_rclose  = __parse_policy_args(policy_args, MAX_RCLOSE)
    min_ramp    = __parse_policy_args(policy_args, MIN_RAMP)
    max_rise    = __parse_policy_args(policy_args, MAX_RISE)
    max_pclose  = __parse_policy_args(policy_args, MAX_PCLOSE)
    outstanding = __parse_policy_args(policy_args, OUTSTANDING)

    ### 净资产为负数的
    if basic[dogen.BVPS] < 0.5:
        logger.debug("Invalid bvps")
        return True

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
        logger.debug("Too large rise-range")
        return True

    ### 特征五
    if kdata.iloc[take_index][dogen.MA5] < kdata.iloc[take_index+1][dogen.MA5]\
    or kdata.iloc[take_index][dogen.MA5] < kdata.iloc[take_index][dogen.MA20]:
        logger.debug("Invalid MA5&MA20 at %s" % kdata.index[take_index])
        return True
    if kdata.iloc[take_index][dogen.VOLUME] > kdata.iloc[take_index+1][dogen.VOLUME]*1.1:
        h2l = dogen.caculate_incr_percentage(kdata.iloc[take_index][dogen.P_HIGH], kdata.iloc[take_index][dogen.P_LOW])
        c2l = dogen.caculate_incr_percentage(kdata.iloc[take_index][dogen.P_CLOSE], kdata.iloc[take_index][dogen.P_LOW])
        if c2l*2 < h2l:
            logger.debug("Get up shadow at %s" % kdata.index[take_index])
            return True
        pass

    ### 特征六
    for temp_index in range(pick_index, -1, -1):
        ### 下跌
        if kdata.iloc[temp_index][dogen.R_CLOSE] >= 0 or kdata.iloc[temp_index+1][dogen.R_CLOSE] <= 0:
            continue
        if kdata.iloc[temp_index][dogen.VOLUME] <= kdata.iloc[temp_index+1][dogen.VOLUME]:
            continue
        ### 放量下跌之后未被上涨突破
        maxi_index = dogen.get_last_column_max(kdata, dogen.P_CLOSE, eIdx=temp_index)
        if maxi_index is None or kdata.iloc[temp_index][dogen.P_OPEN] > kdata.iloc[maxi_index][dogen.P_CLOSE]:
            logger.debug("Invalid fall-trade at %s" % kdata.index[temp_index])
            return True
        pass

    ### 特征七
    temp_index = 10
    tdata = kdata[0: temp_index]
    if tdata[tdata[dogen.R_CLOSE] >= max_rclose].index.size > 0:
        logger.debug("Do include trade with %d percentage R-CLOSE since %s" % (max_rclose, kdata.index[temp_index]))
        return True
    for temp_index in range(temp_index, 0, -1):
        hl_price = dogen.caculate_l_high(kdata.iloc[temp_index][dogen.P_CLOSE])
        tdata = kdata[temp_index-4:temp_index-1]
        if tdata[tdata[dogen.P_CLOSE] >= hl_price].index.size > 0:
            logger.debug("Too large heap-close from %s to %s" % (tdata.index[-1], tdata.index[0]))
            return True
        pass

    ### 特征八
    heap_lhigh = 0
    temp_count = 0
    for temp_index in range(0, pick_index):
        if kdata.iloc[temp_index][dogen.P_CLOSE] >= kdata.iloc[temp_index][dogen.L_HIGH]:
            heap_lhigh+= 1
            temp_count+= 1
        else:
            heap_lhigh = 0
        if heap_lhigh > 1:
            logger.debug("Shouldn't include serial hl-trade")
            return True
        pass
    if temp_count < min_lhigh:
        logger.debug("Don't include hl-trade from %s" % (kdata.index[pick_index]))
        return True

    ### 特征九
    if kdata.iloc[pick_index][dogen.MACD] < 0:
        macd_count = 1
    else:
        macd_count = 0
    for temp_index in range(pick_index-1, -1, -1):
        if kdata.iloc[temp_index][dogen.MACD] < 0 and kdata.iloc[temp_index+1][dogen.MACD] > 0:
            macd_count += 1
        if macd_count > 1:
            logger.debug("Invalid MACD since %s" % kdata.index[pick_index])
            return True
        pass
    
    return False

def include_analyze(basic, kdata, policy_args):
    """ 
    """
    ### 参数解析
    take_valid = __parse_policy_args(policy_args, TAKE_VALID)
    pick_valid = __parse_policy_args(policy_args, PICK_VALID)
    min_rise   = __parse_policy_args(policy_args, MIN_RISE)
    max_fallen = __parse_policy_args(policy_args, MAX_FALLEN)

    ### 特征一
    rise_range = dogen.get_last_rise_range(kdata, min_rise, max_fall=max_fallen, eIdx=22)
    if rise_range is None:
        logger.debug("Don't get valid rise-range")
        return None
    else:
        [pick_index, high_index, inc_close, get_lhigh, tmpId] = rise_range
        if pick_index < pick_valid:
            logger.debug("Invalid rise-range from %s" % kdata.index[pick_index])
            return None
        pass

    ### 特征二
    heap_rises = 0
    take_index = None
    high_index = pick_index
    for temp_index in range(pick_index, -1, -1):
        if kdata.iloc[temp_index][dogen.P_CLOSE] > kdata.iloc[high_index][dogen.P_CLOSE]:
            high_index = temp_index
        temp_close = kdata.iloc[temp_index][dogen.R_CLOSE]
        if temp_close < 0:
            heap_rises = 0
        else:
            heap_rises += temp_close
        if heap_rises >= 5:
            if take_index is None or take_index > temp_index:
                take_index = temp_index
            pass
        if temp_close >= 3 and kdata.iloc[temp_index][dogen.R_AMP] >= 5:
            if take_index is None or take_index > temp_index:
                take_index = temp_index
            pass
        pass
    if take_index is not None:
        if high_index != take_index:
            logger.debug("Invalid pclose of take-trade at %s" % kdata.index[take_index])
            return None
        maxi_index = dogen.get_last_column_max(kdata, dogen.VOLUME, sIdx=take_index, eIdx=take_index+10)
        if maxi_index != take_index:
            logger.debug("Invalid volume of take-trade at %s" % kdata.index[take_index])
            return None
        ### take_index之后缩量下跌(限一个交易日)，也符合策略
        if take_index == 1\
        and kdata.iloc[take_index-1][dogen.R_CLOSE] < 0\
        and kdata.iloc[take_index-1][dogen.VOLUME]  < kdata.iloc[take_index][dogen.VOLUME]:
            take_index-= 1
        ### 最近收盘价比take_index(不能取更新后值)高更新
        elif take_index <= 3\
        and kdata.iloc[0][dogen.R_CLOSE] > 0\
        and kdata.iloc[0][dogen.P_CLOSE] > kdata.iloc[0][dogen.P_OPEN]\
        and kdata.iloc[0][dogen.P_CLOSE] >= kdata.iloc[take_index][dogen.P_CLOSE]:
            take_index = 0
        pass
    if take_index is None or take_index > take_valid:
        logger.debug("Don't get valid take-trade since %s" % kdata.index[pick_index])
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
    result[dogen.RST_COL_SCORE]       = score_analyze(basic, kdata, pick_index, take_index, policy_args)
    result[dogen.RST_COL_MATCH_TIME]  = dogen.datetime_now() # 选中时间
    result[dogen.RST_COL_INDEX]       = '%s_%s' % (basic.name, kdata.index[take_index]) # 唯一标识，用于持久化去重

    return result

def match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 上涨策略, 满足条件：
        >>> 基本条件
            一 维持上涨趋势达两周以上，上涨区间无10%以上的回调；
            二 买入信号(take-trade)，有效期由take_valid限定:
                1) 累积上涨超过5%；
                2) 单日涨幅超过3%，且振幅大于5%；
                3) 必须在区间内收盘价最高，且区间内收盘价最高
                4) 缩量回调修正和上涨突破修正

        >>> 排它条件
            三 股价市值在outstanding(100亿)和maxi_close(50以下)限制范围内
            四 股价成本合理：
                1) 在最近一个月内，最高涨幅由maxi_rise限制； 
            五 take-trade限制:
                1) 维持上涨趋势：MA5上涨且在MA20之上
                2) 排除放量上影线
            六 pick-trade之后若放量下跌必须突破开盘价
            七 回调最低价之后交易日必须满足下面条件:
                1) 没有超过5%的单日涨幅
                2) 每三日累积涨幅不超过前一日涨停价
            八 涨停检查：
                1) 限制最多涨停数
                2) 排除连板
            九 MACD不能存在多段阴线

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
                start = dogen.date_delta(end, -__parse_policy_args(policy_args, MAX_TRADES))
            kdata = db.lookup_stock_kdata(code, start=start, end=end)
            if kdata is None:
                continue
            kdata.sort_index(ascending=False, inplace=True)
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
