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
PICK_START  = 'pick_start'
PICK_END    = 'pick_end'
MIN_FALLEN  = 'min_fallen'
MAX_TAKE2LOW= 'max_take2low'
MAX_PCLOSE  = 'max_pclose'
OUTSTANDING = 'market_value'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAX_TRADES: 360,      # 天
    TAKE_VALID: 0,      # 
    PICK_START: 5,
    PICK_END: 10,
    MIN_FALLEN: 35,
    MAX_TAKE2LOW: 18,
    MAX_PCLOSE: 50,
    OUTSTANDING: 100,
}

def __parse_policy_args(policy_args, arg_name):
    try:
        arg_value = policy_args[arg_name]
    except Exception:
        arg_value = ARGS_DEAULT_VALUE[arg_name]
    return arg_value

def score_analyze(basic, kdata, high_index, pick_index, take_index, fall_range, policy_args):
    """ 根据股票股价、市值、成交量等方面给股票打分:
            * 股价估分，总计40分；
            * 市值估分，总计40分；
            * 涨停估分，总分20分，两个月内一个涨停板10分；
    """
    max_pclose  = __parse_policy_args(policy_args, MAX_PCLOSE)
    outstanding = __parse_policy_args(policy_args, OUTSTANDING)
    pick_start  = __parse_policy_args(policy_args, PICK_START)
    [high_index, pick_index, dec_close, get_llow, tmpId] = fall_range

    score  = dogen.score_by_pclose(30, kdata.iloc[take_index][dogen.P_CLOSE], max_pclose)
    score += dogen.score_by_outstanding(30, kdata.iloc[take_index][dogen.P_CLOSE]*basic[dogen.OUTSTANDING], outstanding)

    temp_score = 20
    temp_slice = 20
    tdata = kdata[0: high_index+1]
    count = tdata[tdata[dogen.P_CLOSE] >= tdata[dogen.L_HIGH]].index.size
    if (count > temp_score/temp_slice):
        count = temp_score/temp_slice
    if (count > 0):
        score += temp_slice*count

    temp_score = 20
    temp_slice = 10
    temp_index = pick_index
    for i in range(0, (int)(temp_score/temp_slice)):
        rise_range = dogen.get_last_rise_range(kdata, 10, sIdx=temp_index)
        if rise_range is None:
            break
        else:
            [min_index, max_index, inc_close, get_hl, tmpId] = rise_range
        score += temp_slice

    return (int)(score)

def exclude_analyze(basic, kdata, pick_index, take_index, fall_range, policy_args):
    pick_start  = __parse_policy_args(policy_args, PICK_START)
    max_take2low= __parse_policy_args(policy_args, MAX_TAKE2LOW)
    max_pclose  = __parse_policy_args(policy_args, MAX_PCLOSE)
    outstanding = __parse_policy_args(policy_args, OUTSTANDING)
    [high_index, pick_index, dec_close, get_llow, tmpId] = fall_range

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
    temp_index = dogen.get_last_column_max(kdata, dogen.P_CLOSE, eIdx=pick_index)
    if dogen.caculate_incr_percentage(kdata.iloc[temp_index][dogen.P_CLOSE], kdata.iloc[pick_index][dogen.P_CLOSE]) > max_take2low:
        logger.debug("Too high close at %s" % kdata.index[temp_index])
        return True

    ### 特征五
    if pick_index+1 >= pick_start:
        macd = dogen.forecast_macd(kdata[dogen.MACD])
        if kdata.iloc[0][dogen.MACD] < -0.1 and macd < -0.1:
            logger.debug("Invalid MACD at %s" % kdata.index[0])
            return True
        pass

    return False

def include_analyze(basic, kdata, policy_args):
    """ 
    """
    ### 参数解析
    take_valid = __parse_policy_args(policy_args, TAKE_VALID)
    pick_start = __parse_policy_args(policy_args, PICK_START)
    pick_end   = __parse_policy_args(policy_args, PICK_END)
    min_fallen = __parse_policy_args(policy_args, MIN_FALLEN)

    ### 预处理
    if kdata.iloc[0][dogen.MA5] > kdata.iloc[0][dogen.MA20]:
        logger.debug("Invalid MA5&MA20 at %s" % kdata.index[0])
        return None
    if dogen.get_last_column_min(kdata, dogen.P_CLOSE, eIdx=30) > pick_end:
        logger.debug("Invalid pick-trade")
        return None

    ### 特征一
    fall_range = dogen.get_last_fall_range(kdata, min_fallen, max_rise=min_fallen)
    if fall_range is None:
        logger.debug("Don't get valid fall-range")
        return None
    else:
        [high_index, pick_index, dec_close, get_llow, tmpId] = fall_range
        if pick_index >= pick_end:
            logger.debug("Too early pick-trade at %s" % kdata.index[pick_index])
            return None
        tdata = kdata[0:pick_index]
        if tdata[tdata[dogen.MA5] > tdata[dogen.MA20]].index.size > 0:
            logger.debug("Invalid pick-trade at %s" % kdata.index[pick_index])
            return None
        pass

    ### 特征二
    heap_rises = 0
    take_index = None
    if pick_index+1 < pick_start:
        for temp_index in range(pick_index-1, -1, -1):
            if kdata.iloc[temp_index][dogen.P_CLOSE] >= dogen.caculate_l_high(kdata.iloc[pick_index][dogen.P_CLOSE]):
                take_index = temp_index
            pass
        rise_range = dogen.get_last_rise_range(kdata, 5, eIdx=pick_index+1)
        if (rise_range is not None) and (dogen.forecast_macd(kdata[dogen.MACD]) >= 0):
            [min_index, max_index, inc_close, get_lhigh, tmpIdx] = rise_range
            if take_index is None or take_index > max_index:
                take_index = max_index 
            pass
        pass
    else:
        rise_range = dogen.get_last_rise_range(kdata, 5, eIdx=pick_index+1)
        if rise_range is not None:
            [min_index, max_index, inc_close, get_lhigh, tmpIdx] = rise_range
            if take_index is None or take_index > max_index:
                take_index = max_index 
            pass
        for temp_index in range(pick_index, -1, -1):
            if kdata.iloc[temp_index][dogen.R_CLOSE] >= 3:
                if take_index is None or take_index > temp_index:
                    take_index = temp_index
                pass
            if kdata.iloc[temp_index][dogen.R_AMP] >= 5 and kdata.iloc[temp_index][dogen.R_CLOSE] >= 0:
                if take_index is None or take_index > temp_index:
                    take_index = temp_index
                pass
            pass
        ### MACD点校验
        temp_index = 0
        if (kdata.iloc[temp_index][dogen.MACD]<-0.01) and (dogen.forecast_macd(kdata[temp_index:-1][dogen.MACD])>=-0.01):
            if take_index is None or take_index > temp_index:
                take_index = temp_index
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
        ### MACD点校验
        if (kdata.iloc[0][dogen.MACD]<=-0.01) and (dogen.forecast_macd(kdata[dogen.MACD])>=-0.01):
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
        [high_index, pick_index, dec_close, get_llow, tmpId] = fall_range

    ### 排它条件过滤
    if exclude_analyze(basic, kdata, pick_index, take_index, fall_range, policy_args):
        logger.debug("exclude_analyze() return True")
        return None

    ### 构造结果
    result = {}
    result[dogen.RST_COL_CODE]        = basic.name # 股票代码
    result[dogen.RST_COL_NAME]        = basic[dogen.NAME] #  证券简写
    result[dogen.RST_COL_INDUSTRY]    = basic[dogen.INDUSTRY]
    result[dogen.RST_COL_START]       = kdata.index[high_index]
    result[dogen.RST_COL_TAKE_TRADE]  = kdata.index[take_index] # 命中交易日
    result[dogen.RST_COL_LAST_CLOSE]  = kdata.iloc[0][dogen.P_CLOSE] # 最后一日收盘价
    result[dogen.RST_COL_OUTSTANDING] = round(kdata.iloc[0][dogen.P_CLOSE] * basic[dogen.OUTSTANDING], 2) # 流通市值
    result[dogen.RST_COL_INC_HL]      = dogen.get_highlimit_trades(kdata, eIdx=high_index+1).size
    result[dogen.RST_COL_SCORE]       = score_analyze(basic, kdata, high_index, pick_index, take_index, fall_range, policy_args)
    result[dogen.RST_COL_MATCH_TIME]  = dogen.datetime_now() # 选中时间
    result[dogen.RST_COL_INDEX]       = '%s_%s' % (basic.name, kdata.index[take_index]) # 唯一标识，用于持久化去重

    return result

def match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 反弹策略, 满足条件：
        >>> 基本条件
            一 下跌min_falls以上，最低价之后ma5不大于ma20;
            二 买入信号(take-trade)，有效期由take_valid限定:
                1) 最低价后最多5个交易日，单日涨停（不限最小区间长度）；
                2) 最低价后至少5个交易日，累积上涨超过5个点，或者单日涨幅超过3个点(MA5上涨)；
                3) 最低价后至少5个交易日，保持横盘，出现振幅大于5%的上涨交易日(MA5上涨)；

        >>> 排它条件
            三 股价市值在outstanding(100亿)和maxi_close(50以下)限制范围内
            四 pick-trade校验:
                1) pick-trade之后最高价不超过15%;
            五 若当前MACD值低于-0.1，那么其预测值必须大于-0.1

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
