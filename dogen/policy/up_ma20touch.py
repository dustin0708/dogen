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
MAX_FALLEN  = 'max_fallen'
MIN_RISE    = 'min_rise'
MAX_RISE    = 'max_rise'
MIN_LHIGH   = 'min_lhigh'
MAX_RCLOSE  = 'max_rclose'
MIN_RAMP    = 'min_ramp'
MAX_PCLOSE  = 'max_pclose'
OUTSTANDING = 'market_value'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAX_TRADES: 90,      # 天
    TAKE_VALID: 0,      # 
    PICK_VALID: 9,
    MAX_FALLEN: 10,
    MIN_RISE: 6,
    MAX_RISE: 80,   # 1%
    MIN_LHIGH: 0,
    MAX_RCLOSE: 7,
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

    score  = dogen.score_by_pclose(40, kdata.iloc[take_index][dogen.P_CLOSE], max_pclose)
    score += dogen.score_by_outstanding(40, kdata.iloc[take_index][dogen.P_CLOSE]*basic[dogen.OUTSTANDING], outstanding)

    temp_score = 20
    temp_slice = 20
    tdata = kdata[0: pick_index+1]
    count = tdata[tdata[dogen.P_CLOSE] >= tdata[dogen.L_HIGH]].index.size
    if (count > temp_score/temp_slice):
        count = temp_score/temp_slice
    if (count > 0):
        score += temp_slice*count

    return (int)(score)

def exclude_analyze(basic, kdata, pick_index, take_index, policy_args):
    max_rclose  = __parse_policy_args(policy_args, MAX_RCLOSE)
    min_lhigh   = __parse_policy_args(policy_args, MIN_LHIGH)
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
        [min_index, max_index, inc_close, get_lhigh, tmp_index] = rise_range
        if max_rise < dogen.caculate_incr_percentage(kdata.iloc[take_index][dogen.P_CLOSE], kdata.iloc[min_index][dogen.P_CLOSE]):
            logger.debug("Too large rise-range")
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

    pick_index = 0
    take_index = None
    
    for temp_index in range(0, take_valid+1):
        if kdata.iloc[temp_index][dogen.P_LOW] <= kdata.iloc[temp_index][dogen.MA20]+0.01:
            take_index = temp_index
            break
        pass

    if take_index is None:
        logger.debug("Don't get valid take-trade")
        return None
    elif kdata.iloc[take_index][dogen.MA20]<kdata.iloc[take_index+1][dogen.MA20]:
        logger.debug("Invalid ma20 at %s" % kdata.index[take_index])
        return None

    for temp_index in range(take_index, kdata.index.size):
        if kdata.iloc[temp_index][dogen.MA5] < kdata.iloc[temp_index][dogen.MA20]:
            break
        pick_index = temp_index

    if pick_index < pick_valid:
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

    rise_range = dogen.get_last_rise_range(kdata, 0, sIdx=pick_index)
    if rise_range is not None:
        [pick_index, max_index, inc_close, get_lhigh, tmp_index] = rise_range

    ### 构造结果
    result = {}
    result[dogen.RST_COL_CODE]        = basic.name # 股票代码
    result[dogen.RST_COL_NAME]        = basic[dogen.NAME] #  证券简写
    result[dogen.RST_COL_INDUSTRY]    = basic[dogen.INDUSTRY]
    result[dogen.RST_COL_START]       = kdata.index[pick_index]
    result[dogen.RST_COL_TAKE_TRADE]  = kdata.index[take_index] # 命中交易日
    result[dogen.RST_COL_LAST_CLOSE]  = kdata.iloc[0][dogen.P_CLOSE] # 最后一日收盘价
    result[dogen.RST_COL_OUTSTANDING] = round(kdata.iloc[0][dogen.P_CLOSE] * basic[dogen.OUTSTANDING], 2) # 流通市值
    result[dogen.RST_COL_INC_HL]      = dogen.get_highlimit_trades(kdata, eIdx=pick_index+1).size
    result[dogen.RST_COL_SCORE]       = score_analyze(basic, kdata, pick_index, take_index, policy_args)
    result[dogen.RST_COL_MATCH_TIME]  = dogen.datetime_now() # 选中时间
    result[dogen.RST_COL_INDEX]       = '%s_%s' % (basic.name, kdata.index[take_index]) # 唯一标识，用于持久化去重

    return result

def match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 上涨均线策略, 股价维持在某一均线之上，调整回踩均线：
        >>> 基本条件
            一 ma5维持在ma20之上；
            二 买入交易日take-trade，满足条件：
                1) 最低价回踩ma20（如何精细定义???）；
            

        >>> 排它条件


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
