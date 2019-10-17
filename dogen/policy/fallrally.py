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
MIN_FALLEN  = 'min_fallen'
MAX_TAKE2low= 'max_take2low'
MAX_HIGH2FROM='max_high2from'
MAX_PICK2FROM='max_pick2from'
MIN_PICK2FROM='min_pick2from'
MIN_RCLOSE  = 'min_pclose'
MAX_PCLOSE  = 'max_pclose'
OUTSTANDING = 'market_value'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAX_TRADES: 150,      # 天
    TAKE_VALID: 0,      # 
    PICK_VALID: 10,
    MIN_FALLEN: 25,
    MAX_TAKE2low: 15,
    MAX_HIGH2FROM: 60,
    MAX_PICK2FROM: 5,
    MIN_PICK2FROM: -100,
    MIN_RCLOSE: -5,
    MAX_PCLOSE: 50,
    OUTSTANDING: 100,
}

def __parse_policy_args(policy_args, arg_name):
    try:
        arg_value = policy_args[arg_name]
    except Exception:
        arg_value = ARGS_DEAULT_VALUE[arg_name]
    return arg_value

def score_analyze(basic, kdata, pick_index, take_index, fall_range, policy_args):
    """ 根据股票股价、市值、成交量等方面给股票打分:
            * 股价估分，总计25分；
            * 市值估分，总计25分；
            * 跌幅估分，总分25分，按100%计算；
            * 区间估分，总分25分，按半年为上限计算(22*6)；
    """
    max_pclose  = __parse_policy_args(policy_args, MAX_PCLOSE)
    outstanding = __parse_policy_args(policy_args, OUTSTANDING)
    score = 0

    temp_score = 25.0
    temp_slice = max_pclose / temp_score
    take_price = kdata.iloc[take_index][dogen.P_CLOSE]
    if (take_price <= max_pclose):
        score += (temp_score - (int)(math.floor(take_price/temp_slice)))

    temp_score = 25.0
    temp_slice = outstanding / temp_score
    take_value = take_price * basic[dogen.OUTSTANDING]
    if (take_value <= outstanding):
        score += (temp_score - (int)(math.floor(take_value/temp_slice)))

    return (int)(score)

def exclude_analyze(basic, kdata, pick_index, take_index, fall_range, policy_args):
    max_take2low= __parse_policy_args(policy_args, MAX_TAKE2low)
    max_high2from=__parse_policy_args(policy_args, MAX_HIGH2FROM)
    max_pick2from=__parse_policy_args(policy_args, MAX_PICK2FROM)
    min_pick2from=__parse_policy_args(policy_args, MIN_PICK2FROM)
    min_rclose  = __parse_policy_args(policy_args, MIN_RCLOSE)
    max_pclose  = __parse_policy_args(policy_args, MAX_PCLOSE)
    outstanding = __parse_policy_args(policy_args, OUTSTANDING)
    [high_index, pick_index, dec_close, get_llow, tmpId] = fall_range
    from_index = high_index

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
    temp_index = dogen.get_last_column_max(kdata, dogen.P_CLOSE, eIdx=pick_index)
    if dogen.caculate_incr_percentage(kdata.iloc[temp_index][dogen.P_CLOSE], kdata.iloc[pick_index][dogen.P_CLOSE]) > max_take2low:
        logger.debug("Too high close at %s" % kdata.index[temp_index])
        return True
    if (pick_index+5) > high_index:
        temp_index = high_index
    else:
        temp_index = pick_index+5
    tdata = kdata[pick_index: temp_index]
    if tdata[tdata[dogen.R_CLOSE] <= min_rclose].index.size <= 0:
        logger.debug("Don't get trade with pclose lower than %d" % min_rclose)
        return True

    ### 特征五
    rise_range = dogen.get_last_rise_range(kdata, 15, max_fall=15, sIdx=high_index)
    if rise_range is not None:
        [from_index, max_index, inc_close, get_hl, tmpId] = rise_range
        if (max_index != high_index) or (inc_close > max_high2from):
            logger.debug("Invalid rise range from %s to %s" % (kdata.index[from_index], kdata.index[max_index]))
            return True
        tmp_pick2from = dogen.caculate_incr_percentage(kdata.iloc[pick_index][dogen.P_CLOSE], kdata.iloc[from_index][dogen.P_CLOSE])
        if (tmp_pick2from > max_pick2from) or (tmp_pick2from < min_pick2from):
            logger.debug("Invalid from/pick-trade(%s/%s)" % (kdata.index[from_index], kdata.index[pick_index]))
            return True
        pass
    if from_index < 22:
        logger.debug("Too short range from %s" % kdata.index[from_index])
        return True

    ### 特征六
    tdata = kdata[pick_index:high_index+15]
    if tdata[tdata[dogen.P_CLOSE] >= tdata[dogen.L_HIGH]].index.size <= 0:
        logger.debug("Don't include hl-trade from %s" % kdata.index[high_index])
        return True

    ### 特征七
    hl_count = 0
    for temp_index in range(high_index, from_index):
        if kdata.iloc[temp_index][dogen.P_CLOSE] >= kdata.iloc[temp_index][dogen.L_HIGH]:
            hl_count += 1
        else:
            hl_count  = 0
        if hl_count >= 2:
            logger.debug("get serial hl-trade from %s to %s" % (kdata.index[from_index], kdata.index[high_index]))
            return True
        pass

    return False

def include_analyze(basic, kdata, policy_args):
    """ 
    """
    ### 参数解析
    take_valid = __parse_policy_args(policy_args, TAKE_VALID)
    pick_valid = __parse_policy_args(policy_args, PICK_VALID)
    min_fallen = __parse_policy_args(policy_args, MIN_FALLEN)

    ### 特征一
    fall_range = dogen.get_last_fall_range(kdata, min_fallen, max_rise=min_fallen)
    if fall_range is None:
        logger.debug("Don't get valid fall-range")
        return None
    else:
        [high_index, pick_index, dec_close, get_llow, tmpId] = fall_range
        if pick_index > pick_valid:
            logger.debug("Invalid pick-trade at %s" % kdata.index[pick_index])
            return None
        pass

    ### 特征二
    heap_rises = 0
    take_index = None
    if pick_index+1 < 5:
        for temp_index in range(pick_index-1, -1, -1):
            if kdata.iloc[temp_index][dogen.P_CLOSE] >= dogen.caculate_l_high(kdata.iloc[pick_index][dogen.P_CLOSE]):
                take_index = temp_index
            pass
        pass
    else:
        if dogen.caculate_incr_percentage(kdata.iloc[0][dogen.P_CLOSE], kdata.iloc[0][dogen.MA5]) < 3:
            for temp_index in range(pick_index, -1, -1):
                if kdata.iloc[temp_index][dogen.R_CLOSE] > 0 and kdata.iloc[temp_index][dogen.R_AMP] >= 5:
                    if take_index is None or take_index > temp_index:
                        take_index = temp_index
                    pass
                pass
            pass
        for temp_index in range(pick_index, -1, -1):
            temp_close = kdata.iloc[temp_index][dogen.R_CLOSE]
            if temp_close < 0:
                heap_rises = 0
            else:
                heap_rises += temp_close
            if heap_rises >= 5:
                if take_index is None or take_index > temp_index:
                    take_index = temp_index
                pass
            if temp_close >= 3 and kdata.iloc[temp_index][dogen.P_CLOSE] > kdata.iloc[temp_index][dogen.P_OPEN]:
                if take_index is None or take_index > temp_index:
                    take_index = temp_index
                pass
            pass
    if take_index is not None:
        ### take_index之后缩量下跌(限一个交易日)，也符合策略
        if take_index == 1\
        and kdata.iloc[take_index-1][dogen.R_CLOSE] < 0\
        and kdata.iloc[take_index-1][dogen.VOLUME]  < kdata.iloc[take_index][dogen.VOLUME]:
            take_index-= 1
        ### 最近收盘价比take_index(不能取更新后值)高更新
        elif kdata.iloc[0][dogen.R_CLOSE] >= 0\
        and kdata.iloc[0][dogen.P_CLOSE] >= kdata.iloc[0][dogen.P_OPEN]\
        and kdata.iloc[0][dogen.P_CLOSE] >= kdata.iloc[take_index][dogen.P_CLOSE]:
            take_index = 0
        pass
    if take_index is None or take_index > take_valid:
        logger.debug("Don't get valid take-trade since %s" % kdata.index[pick_index])
        return None

    return [pick_index, take_index, fall_range]

def stock_analyze(basic, kdata, policy_args):
    ### 基本条件选取
    get_index = include_analyze(basic, kdata, policy_args)
    if get_index is None:
        logger.debug("include_analyze() return None")
        return None
    else:
        [pick_index, take_index, fall_range] = get_index

    ### 排它条件过滤
    if exclude_analyze(basic, kdata, pick_index, take_index, fall_range, policy_args):
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
    result[dogen.RST_COL_SCORE]       = score_analyze(basic, kdata, pick_index, take_index, fall_range, policy_args)
    result[dogen.RST_COL_MATCH_TIME]  = dogen.datetime_now() # 选中时间
    result[dogen.RST_COL_INDEX]       = '%s_%s' % (basic.name, kdata.index[take_index]) # 唯一标识，用于持久化去重

    return result

def match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 反弹策略, 满足条件：
        >>> 基本条件
            一 下跌10个点以上，且pick-trade之前10个交易日内有下跌10个点;
            二 买入信号(take-trade)，有效期由take_valid限定:
                1) 最低价后最多5个交易日，单日涨停（不限最小区间长度）；
                2) 最低价后至少5个交易日，累积上涨超过5个点，或者单日涨幅超过3个点(MA5上涨)；
                3) 最低价后至少5个交易日，保持横盘，出现振幅大于5%的上涨交易日(MA5上涨)；

        >>> 排它条件
            三 股价市值在outstanding(100亿)和maxi_close(50以下)限制范围内
            四 pick-trade校验:
                1) pick-trade之后最高价不超过15%;
                2) pick-trade之前一周回调区间存在跌5%以上交易日;
            五 上涨区间校验:
                1) 限制大幅上涨后的回调最低价必须不超过前低的150%;
                2) 无论上涨区间是否存在, 至少包括一个月数据;
            六 必须有涨停交易日:
                1) 下降区间在三个月以内，取收盘最高价前15个交易日区间；
                2) 下降区间在三个月以上，则三个月内必须有涨停;
            七 上涨区间无连板

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
