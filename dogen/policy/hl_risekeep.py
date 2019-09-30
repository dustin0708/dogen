#-*-coding:utf-8-*-

import sys
import math
import dogen
import numpy
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
MINI_HL     = 'mini_hl'
MAXI_HL     = 'maxi_hl'
TAKE_VALID  = 'take_valid'
MAXI_RISE   = 'maxi_rise'
MAX_TAKE2HL = 'maxi_take2hl'
MAXI_CLOSE  = 'maxi_close'
OUTSTANDING = 'outstanding'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAXI_DAYS: 60,      # 天
    MINI_HL: 3,      # 
    MAXI_HL: 15,        #
    TAKE_VALID: 0,  # 倍
    MAXI_RISE: 35,   # 1%
    MAX_TAKE2HL: 15, 
    MAXI_CLOSE: 50,
    OUTSTANDING: 100,
}

def __parse_policy_args(policy_args, arg_name):
    try:
        arg_value = policy_args[arg_name]
    except Exception:
        arg_value = ARGS_DEAULT_VALUE[arg_name]
    return arg_value

def score_analyze(basic, kdata, pick_index, take_index):
    """ 根据股票股价、市值、成交量等方面给股票打分:
            * 基准分值50分，累积加分项；
            * 股价限高50元，区间定为(50,45],(45,40],...,(5,0]，分值由1~10递增；
            * 市值限高50亿，区间定为(50,45],(45,40],...,(5,0]，分值由1~10递增；
            * 量变限低一倍，区间定为(1.0,1.1],(1.1,1.2],...,(1.9, +Inf)，分值由1~10递增；
            * take最高涨幅，区间定位(0,1],(1,2],...,(9,10],分值由1~10递增;
            * 最后5交易日， 连续放量上涨10%，每个交易日2分；
    """
    score = 50

    take_price = kdata.iloc[take_index][dogen.P_CLOSE]
    if (take_price < 50):
        score += (10 - (int)(math.floor(take_price/5)))

    take_value = take_price * basic[dogen.OUTSTANDING]
    if (take_value < 50):
        score += (10 - (int)(math.floor(take_value/5)))

    vary_volume = kdata.iloc[take_index][dogen.VOLUME] / kdata.iloc[take_index+1][dogen.VOLUME]
    if (vary_volume > 2):
        score += 10
    elif (vary_volume > 1):
        score += (int)(math.ceil(10 * (vary_volume - 1)))
    
    take_highx = dogen.caculate_incr_percentage(kdata.iloc[take_index][dogen.P_HIGH], kdata.iloc[take_index+1][dogen.P_CLOSE])
    if take_highx > 0:
        score += (int)(math.ceil(take_highx))

    for temp_index in range(4, -1, -1):
        if kdata.iloc[temp_index+1][dogen.VOLUME]*1.1 <= kdata.iloc[temp_index][dogen.VOLUME]:
            score += 2
        pass

    return score

def exclude_analyze(basic, kdata, pick_index, take_index, policy_args):
    """ 根据日线做排除性校验
    """
    maxi_rise    = __parse_policy_args(policy_args, MAXI_RISE)
    maxi_take2hl = __parse_policy_args(policy_args, MAX_TAKE2HL)
    maxi_close   = __parse_policy_args(policy_args, MAXI_CLOSE)
    outstanding  = __parse_policy_args(policy_args, OUTSTANDING)

    ### 净资产为负数的
    if basic[dogen.BVPS] <= 0:
        logger.debug("Invalid bvps")
        return True
        
    ### 特征三
    try:
        this_range = dogen.get_last_rise_range(kdata, maxi_rise, max_fall=maxi_rise/2, eIdx=22)
        if this_range is not None:
            logger.debug("Too large rise-range")
            return True
    except Exception:
        traceback.print_exc()
        pass    
    if dogen.caculate_incr_percentage(kdata.iloc[take_index][dogen.P_CLOSE], kdata.iloc[pick_index][dogen.P_CLOSE]) > maxi_take2hl:
        logger.debug("Too large rise at %s" % kdata.index[take_index])
        return True

    ### 特征四
    if kdata.iloc[take_index][dogen.MA5] < kdata.iloc[take_index+1][dogen.MA5]:
        logger.debug("Invalid MA5 at %s" % kdata.index[take_index])
        return True
    if kdata.iloc[take_index][dogen.P_CLOSE] < kdata.iloc[take_index][dogen.MA20]:
        logger.debug("Invalid take trade at %s" % kdata.index[take_index])
        return True

    ### 特征五
    if kdata.iloc[take_index][dogen.P_CLOSE] > maxi_close:
        logger.debug("Too high close price at %s" % kdata.index[take_index])
        return True
    if kdata.iloc[take_index][dogen.P_CLOSE] * basic[dogen.OUTSTANDING] > outstanding:
        logger.debug("Too large outstanding at %s" % kdata.index[take_index])
        return True

    ### 特征六
    if pick_index >= 5:
        temp_index = pick_index
        if kdata.iloc[pick_index][dogen.P_CLOSE] < kdata.iloc[pick_index-1][dogen.P_CLOSE]:
            temp_index = pick_index - 1
        tdata = kdata[take_index:temp_index+1].sort_index()
        polyf = numpy.polyfit(range(0, tdata.index.size), tdata[dogen.P_CLOSE], 2)
        if polyf[0] < 0.01:
            logger.debug("Invalid polyfit(2) shape from %s to %s" % (kdata.index[temp_index], kdata.index[take_index]))
            return True
        pass

    return False

def include_analyze(basic, kdata, policy_args):
    ### 策略参数处理
    mini_hl     = __parse_policy_args(policy_args, MINI_HL)
    maxi_hl     = __parse_policy_args(policy_args, MAXI_HL)
    take_valid  = __parse_policy_args(policy_args, TAKE_VALID)

    ### 特征一
    index = dogen.get_highlimit_trades(kdata, eIdx=maxi_hl+1)
    if index.size != 1:
        logger.debug("Don't match highlimit trades")
        return None
    else:
        pick_trade = index[0]
        pick_index = kdata.index.get_loc(pick_trade)
        pick_close = kdata.iloc[pick_index][dogen.P_CLOSE]
    if pick_index < mini_hl or kdata.iloc[pick_index][dogen.R_AMP] < 5:
        logger.debug("Invalid hl-trade at %s" % pick_trade)
        return None
    
    ### 特征二
    take_index = None
    if pick_index < 5:
        if dogen.caculate_incr_percentage(kdata.iloc[0][dogen.P_CLOSE], kdata.iloc[pick_index][dogen.P_CLOSE]) > 3:
            logger.debug("Invalid trade at %s" % kdata.index[0])
            return None
        tdata = kdata[0: pick_index]
        tdata = tdata[tdata[dogen.P_CLOSE] < pick_close]
        if tdata.index.size > 0:
            logger.debug("Invalid trade at %s" % tdata.index[0])
            return None
        pass ###take_index = 0
    else:
        heap_rises = 0
        for temp_index in range(pick_index-1, -1, -1):
            temp_close = kdata.iloc[temp_index][dogen.R_CLOSE]
            if temp_close < 0:
                heap_rises = 0
            else:
                heap_rises += temp_close
            ### 上涨take交易日收盘价必须超过涨停价回调3个点的价位
            if kdata.iloc[temp_index][dogen.P_CLOSE] < pick_close*(1-0.03):
                continue
            if heap_rises >= 5:
                take_index = temp_index
            if temp_close >= 3 and kdata.iloc[temp_index][dogen.R_AMP] >= 5:
                take_index = temp_index
            pass
        if take_index is not None:
            ### take_index之后缩量下跌(限一个交易日)，也符合策略
            if take_index == 1\
            and kdata.iloc[take_index-1][dogen.R_CLOSE] < 0\
            and kdata.iloc[take_index-1][dogen.VOLUME]  < kdata.iloc[take_index][dogen.VOLUME]:
                take_index-= 1
            ### 最近收盘价比take_index(不能取更新后值)高更新
            elif kdata.iloc[0][dogen.P_CLOSE] > kdata.iloc[take_index][dogen.P_CLOSE]:
                take_index = 0
            pass
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
    result[dogen.RST_COL_SCORE]       = score_analyze(basic, kdata, pick_index, take_index) # 打分
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
            三 股价成本合理：
                1) 在maxi_days交易日内，最高涨幅由maxi_rise限制（默认35%）；
                2) take-trade相对于涨停日收盘价涨幅由maxi_take2hl限制（默认15%）
            四 维持上涨趋势：MA5上涨，且take-trade收盘价高于MA20
            五 股价市值在outstanding(100亿)和maxi_close(50以下)限制范围内
            六 涨停之后保持碗底弧形上涨趋势

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
            if kdata is None:
                continue
            kdata.sort_index(ascending=False, inplace=True)
            dogen.drop_fresh_stock_trades(basic, kdata)

            ### 策略分析
            logger.debug("Begin in analyzing %s from %s to %s" % (code, start, end))
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

