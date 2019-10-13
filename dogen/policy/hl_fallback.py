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
        * maxi_rise: 涨停之前最大涨幅
"""

### 策略参数名
MAXI_DAYS   = 'maxi_days'
TAKE_VALID  = 'take_valid'
HL_VALID    = 'hl_valid'
VOLUME_SCALE= 'volume_scale'
MINI_FALLS  = 'mini_falls'
MAXI_RISE   = 'maxi_rise'
MAXI_CLOSE  = 'maxi_close'
OUTSTANDING = 'outstanding'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAXI_DAYS: 60,      # 天
    TAKE_VALID: 0,      # 
    HL_VALID: 4,        #
    VOLUME_SCALE: 1.5,  # 倍
    MINI_FALLS: 3.99,   # 1%
    MAXI_RISE: 30,   # 1%
    MAXI_CLOSE: 50,
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
            * 下跌估分，总计50分(默认最多四个下跌交易日)；
    """
    maxi_close  = __parse_policy_args(policy_args, MAXI_CLOSE)
    outstanding = __parse_policy_args(policy_args, OUTSTANDING)
    hl_valid    = __parse_policy_args(policy_args, HL_VALID)
    score = 0

    temp_score = 25.0
    temp_slice = maxi_close / temp_score
    take_price = kdata.iloc[take_index][dogen.P_CLOSE]
    if (take_price <= maxi_close):
        score += (temp_score - (int)(math.floor(take_price/temp_slice)))

    temp_score = 25.0
    temp_slice = outstanding / temp_score
    take_value = take_price * basic[dogen.OUTSTANDING]
    if (take_value <= outstanding):
        score += (temp_score - (int)(math.floor(take_value/temp_slice)))

    temp_score = 50.0
    temp_slice = temp_score / hl_valid
    if (pick_index <= hl_valid):
        score += (pick_index * temp_slice)

    return (int)(score)

def exclude_analyze(basic, kdata, pick_index, take_index, policy_args):
    """ 根据日线做排除性校验
    """
    maxi_rise    = __parse_policy_args(policy_args, MAXI_RISE)
    maxi_close   = __parse_policy_args(policy_args, MAXI_CLOSE)
    outstanding  = __parse_policy_args(policy_args, OUTSTANDING)

    ### 净资产为负数的
    if basic[dogen.BVPS] <= 0:
        logger.debug("Invalid bvps")
        return True

    ### 特征三
    if kdata.iloc[take_index][dogen.P_CLOSE] > maxi_close:
        logger.debug("Too high close price at %s" % kdata.index[take_index])
        return True
    if kdata.iloc[take_index][dogen.P_CLOSE] * basic[dogen.OUTSTANDING] > outstanding:
        logger.debug("Too large outstanding at %s" % kdata.index[take_index])
        return True
    
    ### 特征四
    rise_range = dogen.get_last_rise_range(kdata, maxi_rise, max_fall=maxi_rise/2, eIdx=22)
    if rise_range is not None:
        [min_index, max_index, inc_close, get_lhigh, tmp_index] = rise_range
        if pick_index <= min_index and pick_index >= max_index:
            logger.debug("Too large rise-range")
            return True
        pass

    ### 特征五
    if kdata.iloc[take_index][dogen.MA5] < kdata.iloc[take_index+1][dogen.MA5] and kdata.iloc[take_index][dogen.MA20] < kdata.iloc[take_index+1][dogen.MA20]:
        logger.debug("Invalid MA5&MA20 at %s" % kdata.index[take_index])
        return True

    ### 特征六
    if kdata.iloc[pick_index-1][dogen.P_OPEN] > kdata.iloc[pick_index][dogen.P_CLOSE]:
        logger.debug("Invalid open price at %s" % kdata.index[pick_index-1])
        return True

    return False

def include_analyze(basic, kdata, policy_args):
    ### 策略参数处理
    take_valid  = __parse_policy_args(policy_args, TAKE_VALID)
    hl_valid    = __parse_policy_args(policy_args, HL_VALID)
    volume_scale= __parse_policy_args(policy_args, VOLUME_SCALE)
    mini_falls  = __parse_policy_args(policy_args, MINI_FALLS)

    ### 特征一
    index = dogen.get_highlimit_trades(kdata, eIdx=hl_valid+1)
    if index.size != 1:
        logger.debug("Don't match highlimit trades")
        return None
    else:
        pick_trade = index[0]
        ### 若最后一天涨停忽略
        pick_index = kdata.index.get_loc(pick_trade)
        if pick_index == 0:
            logger.debug("Fallback didn't occur")
            return None
        ### 涨停振幅需大于5
        if kdata.iloc[pick_index][dogen.R_AMP] < 5:
            logger.debug("Invalid R_AMP at %s" % kdata.index[pick_index])
            return None
        ### 取最低回调价
        if (pick_index+1) >= kdata.index.size:
            mini_close = 0
        else:
            mini_close = kdata.iloc[pick_index+1][dogen.P_CLOSE]
    if kdata.iloc[pick_index-1][dogen.R_CLOSE] > 0:
        pick_index -= 1
        if pick_index == 0:
            logger.debug("Fallback didn't occur")
            return None
        if (kdata.iloc[pick_index+1][dogen.VOLUME] * volume_scale) > kdata.iloc[pick_index][dogen.VOLUME]:
            logger.debug("Too small volume at " + kdata.index[pick_index])
            return None
        if dogen.caculate_incr_percentage(kdata.iloc[pick_index][dogen.P_CLOSE], kdata.iloc[pick_index][dogen.P_OPEN]) <= -5:
            logger.debug("Invalid open&close at %s" % kdata.index[pick_index])
            return None
        pass

    ### 特征二
    heap_falls = 0
    take_index = None
    for temp_index in range(pick_index-1, -1, -1):
        if kdata.iloc[temp_index][dogen.P_CLOSE] < mini_close:
            logger.debug("Get invalid fall trade at %s" % kdata.index[temp_index])
            return None
        this_close = kdata.iloc[temp_index][dogen.R_CLOSE]
        if this_close > 0:
            if take_index is not None:
                take_index = temp_index
            break
        else:
            heap_falls+= abs(this_close)
        ### 达到回调要求, 命中
        if heap_falls >= mini_falls:
            take_index = temp_index
        ### 若放量下跌即终止
        if kdata.iloc[temp_index][dogen.VOLUME] > kdata.iloc[temp_index+1][dogen.VOLUME]:
            break
        pass
    if take_index is None or take_index > take_valid:
        logger.debug("Don't match valid fallback trade")
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
    """ 涨停回调策略，满足条件：
        >>> 基本条件
            一 仅有一个涨停在hl_valid交易日内, 涨停后限一个交易日上涨，放量限制最小volume_scale倍；
            二 买入信号(take-trade)，有效期由take_valid限定:
                1) 连续缩量下跌[mini_falls, maxi_falls]，放量下跌或上涨即终止；
        
        >>> 排它条件
            三 股价市值在outstanding(100亿)和maxi_close(50以下)限制范围内
            四 股价成本合理：
                1) 在最近一个月内，最高涨幅由maxi_rise限制（默认35%）； 
                2) 不可回调过高，take-trade收盘价高于涨停前交易日
            五 维持上涨趋势：take-trade交易日MA5或MA20上涨;
            六 pick-trade后一交易日开盘价不可超过pick-trade收盘价

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

