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
MAX_TAKE2LOW= 'max_take2low'
MIN_FALLS   = 'min_falls'
MAX_RCLOSE  = 'max_rclose'
MIN_RAMP    = 'min_ramp'
MIN_POLYF2  = 'min_polyf2'
MAX_PCLOSE  = 'max_pclose'
OUTSTANDING = 'outstanding'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAX_TRADES: 90,      # 天
    MINI_HL: 3,      # 
    HL_VALID: 15,        #
    TAKE_VALID: 0,  # 倍
    MAX_RISE: 42,   # 1%
    MAX_TAKE2LOW: 15,
    MIN_FALLS: 3,
    MAX_RCLOSE: 7,
    MIN_RAMP: 5,
    MIN_POLYF2: 0.008,
    MAX_PCLOSE: 50,
    OUTSTANDING: 300,
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
    max_take2low = __parse_policy_args(policy_args, MAX_TAKE2LOW)
    min_falls    = __parse_policy_args(policy_args, MIN_FALLS)
    max_rclose   = __parse_policy_args(policy_args, MAX_RCLOSE)
    min_ramp     = __parse_policy_args(policy_args, MIN_RAMP)
    min_polyf2   = __parse_policy_args(policy_args, MIN_POLYF2)
    max_pclose   = __parse_policy_args(policy_args, MAX_PCLOSE)
    outstanding  = __parse_policy_args(policy_args, OUTSTANDING)

    ### 取回调最低价
    mini_index = dogen.get_last_column_min(kdata, dogen.P_CLOSE, sIdx=take_index, eIdx=pick_index)

    ### 净资产为负数的
    if basic[dogen.BVPS] < 0:
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
        [min_index, max_index, inc_close, get_lhigh, tmp_index] = rise_range
        if max_rise < dogen.caculate_incr_percentage(kdata.iloc[take_index][dogen.P_CLOSE], kdata.iloc[min_index][dogen.P_CLOSE]):
            logger.debug("Too large rise-range")
            return True
        pass

    ### 特征五
    if pick_index >= 5:
        high_index = pick_index
        if kdata.iloc[pick_index-1][dogen.R_CLOSE] > 0:
            high_index = pick_index-1
        if dogen.caculate_incr_percentage(kdata.iloc[take_index][dogen.P_CLOSE], kdata.iloc[high_index][dogen.P_CLOSE]) < -min_falls:
            logger.debug("Too low P-CLOSE at take-trade %s" % kdata.index[take_index])
            return True
        if kdata.iloc[take_index][dogen.MA5] < kdata.iloc[take_index+1][dogen.MA5]:
            logger.debug("Invalid MA20 at %s" % kdata.index[take_index])
            return True
        if kdata.iloc[take_index][dogen.MA20] < kdata.iloc[take_index+1][dogen.MA20]:
            logger.debug("Invalid MA20 at %s" % kdata.index[take_index])
            return True
        if kdata.iloc[take_index][dogen.VOLUME] > kdata.iloc[take_index+1][dogen.VOLUME]*1.1:
            h2l = dogen.caculate_incr_percentage(kdata.iloc[take_index][dogen.P_HIGH], kdata.iloc[take_index][dogen.P_LOW])
            c2l = dogen.caculate_incr_percentage(kdata.iloc[take_index][dogen.P_CLOSE], kdata.iloc[take_index][dogen.P_LOW])
            if c2l*2 < h2l:
                logger.debug("Get up shadow at %s" % kdata.index[take_index])
                return True
            pass
        pass

    ### 特征七
    if pick_index >= 5:
        for temp_index in range(mini_index-1, -1, -1):
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
        pass

    ### 特征八
    if pick_index >= 5:
        tdata = kdata[0: mini_index]
        if tdata[tdata[dogen.R_AMP] >= min_ramp].index.size <= 0:
            logger.debug("Don't include trade with 5 percentage R-AMP since %s" % kdata.index[mini_index])
            return True
        for temp_index in range(mini_index, 0, -1):
            hl_price = dogen.caculate_l_high(kdata.iloc[temp_index][dogen.P_CLOSE])
            tdata = kdata[temp_index-4:temp_index-1]
            if tdata[tdata[dogen.P_CLOSE] >= hl_price].index.size > 0:
                logger.debug("Too large heap-close from %s to %s" % (tdata.index[-1], tdata.index[0]))
                return True
            pass
        pass

    return False

def include_analyze(basic, kdata, policy_args):
    ### 策略参数处理
    mini_hl     = __parse_policy_args(policy_args, MINI_HL)
    hl_valid    = __parse_policy_args(policy_args, HL_VALID)
    take_valid  = __parse_policy_args(policy_args, TAKE_VALID)

    ### 特征一
    index = dogen.get_highlimit_trades(kdata, eIdx=hl_valid+1)
    if index.size == 0:
        logger.debug("Don't match highlimit trades")
        return None
    else:
        pick_trade = index[0]
        pick_index = kdata.index.get_loc(pick_trade)
        pick_close = kdata.iloc[pick_index][dogen.P_CLOSE]
        mini_index = pick_index + 1
        if mini_index < kdata.index.size:
            mini_close = kdata.iloc[pick_index+1][dogen.P_CLOSE]
        else:
            mini_close = 0
    if pick_index < mini_hl:
        logger.debug("Invalid hl-trade at %s" % pick_trade)
        return None
    
    ### 特征二
    take_index = None
    if pick_index < 5 and pick_index >= 3:
        if dogen.caculate_incr_percentage(kdata.iloc[0][dogen.P_CLOSE], kdata.iloc[pick_index][dogen.P_CLOSE]) > 5:
            logger.debug("Invalid trade at %s" % kdata.index[0])
            return None
        tdata = kdata[0: pick_index]
        tdata = tdata[tdata[dogen.P_CLOSE] < pick_close]
        if tdata.index.size > 0:
            logger.debug("Invalid trade at %s" % tdata.index[0])
            return None
        take_index = 0
    else:
        heap_rises = 0
        for temp_index in range(pick_index-1, -1, -1):
            temp_close = kdata.iloc[temp_index][dogen.R_CLOSE]
            if temp_close < 0:
                heap_rises = 0
            else:
                heap_rises += temp_close
            if heap_rises >= 5:
                take_index = temp_index
            if temp_close >= 3 and kdata.iloc[temp_index][dogen.P_CLOSE] > kdata.iloc[temp_index][dogen.P_OPEN]:
                take_index = temp_index
            pass
        if take_index is not None:
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
        pass
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
    """ 涨停上涨策略, 满足条件：
        >>> 基本条件
            一 仅有一个涨停在[min_hl， max_hl]交易区间以内;
            二 买入信号(take-trade)，有效期由take_valid限定:
                1) 5日以内收盘价均维持在涨停价以上，且相对涨停价涨幅不高于5个点；
                2) 5日以外累积上涨幅度达5个点或单日涨幅3点以上，且收盘价突破涨停价, 下面情况更新take-trade;
                    a. 若take-trade之后限一个交易日缩量下跌；
                    b. 若take-trade之后最后交易日收盘价突破，更新为买入信号；
        
        >>> 排它条件
            三 股价市值在outstanding(100亿)和maxi_close(50以下)限制范围内
            四 股价成本合理：
                1) 在最近一个月内，最高涨幅由maxi_rise限制； 
            五 take-trade限制:
                1) take-trade交易日收盘价高于涨停价-3%，但不超过回调最低价+15%;
                2) 维持上涨趋势：MA5上涨，MA20上涨
                3) 排除放量上影线
            六 形态限制：
                1) 涨停之后保持碗底弧形上涨趋势, 碗底收盘价低于涨停价-3个点以上，二次拟合系数大于0.008；
                2) 涨停之后回调超过-3之后，只允许一次突破前高;(暂未实现)
            七 碗底之后若放量下跌必须突破开盘价
            八 回调最低价之后交易日必须满足下面条件:
                1) 没有超过7%的单日涨幅
                2) 存在振幅5个点以上交易日
                3) 每三日累积涨幅不超过前一日涨停价

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

