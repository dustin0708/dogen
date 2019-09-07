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
MAXI_DAYS   = 'maxi_days'
TAKE_VALID  = 'take_valid'
HL_VALID    = 'hl_valid'
VOLUME_SCALE= 'volume_scale'
MINI_FALLS  = 'mini_falls'
MAXI_PRERISE= 'maxi_prerise'

ARGS_DEAULT_VALUE = {
    MAXI_DAYS: 60,      # 天
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

def __score_analyze(basic, kdata, pick_index, take_index):
    """ 根据股票股价、市值、成交量等方面给股票打分:
            * 基准分值60分，累积加分项；
            * 股价限高50元，区间定为(50,45],(45,40],...,(5,0]，分值由1~10递增；
            * 市值限高40亿，区间定为(40,36],(36,32],...,(4,0]，分值由1~10递增；
            * 量变限低一倍，区间定为(1.0,1.1],(1.1,1.2],...,(1.9, +Inf)，分值由1~10递增；
            * 收盘价限-3点，一个交易日2分
    """
    score = 60

    take_price = kdata.iloc[take_index][dogen.P_CLOSE]
    if (take_price < 50):
        score += (10 - (int)(math.floor(take_price/5)))

    take_value = take_price * basic[dogen.OUTSTANDING]
    if (take_value < 40):
        score += (10 - (int)(math.floor(take_value/4)))

    vary_volume = kdata.iloc[take_index][dogen.VOLUME] / kdata.iloc[take_index+1][dogen.VOLUME]
    if (vary_volume > 2):
        score += 10
    elif (vary_volume > 1):
        score += (int)(math.ceil(10 * (vary_volume - 1)))

    temp_kdata = kdata[0:pick_index]
    if temp_kdata is not None:
        score += temp_kdata[temp_kdata[dogen.R_CLOSE]>-3].index.size * 2

    return score

def __exclude_analyze(basic, kdata, pick_index, take_index, maxi_prerise):
    """ 根据日线做排除性校验
    """
    ### 检查区间最高涨幅
    try:
        [min_index, max_index, inc_close, get_lhigh] = dogen.get_last_rise_range(kdata, 30, max_fall=20)
        if inc_close > maxi_prerise:
            logger.debug("Too large rise-range from %s to %s" % (kdata.index[min_index], kdata.index[max_index]))
            return True
    except Exception:
        pass

    return False

def __policy_analyze(basic, kdata, policy_args):
    ### 策略参数处理
    take_valid  = __parse_policy_args(policy_args, TAKE_VALID)
    hl_valid    = __parse_policy_args(policy_args, HL_VALID)
    volume_scale= __parse_policy_args(policy_args, VOLUME_SCALE)
    mini_falls  = __parse_policy_args(policy_args, MINI_FALLS)
    maxi_prerise= __parse_policy_args(policy_args, MAXI_PRERISE)

    ### 特征一校验
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
        pass
    
    ### 特征二校验
    if kdata.iloc[pick_index-1][dogen.R_CLOSE] > 0:
        if (kdata.iloc[pick_index][dogen.VOLUME] * volume_scale) > kdata.iloc[pick_index-1][dogen.VOLUME]:
            logger.debug("Too small volume at " + kdata.index[pick_index-1])
            return None
        ### 更正pick_index
        pick_index = pick_index-1
        if pick_index == 0:
            logger.debug("Fallback didn't occur")
            return None
        pass
    
    ### 特征三校验
    heap_falls = 0
    take_index = None
    for this_index in range(pick_index-1, -1, -1):        
        this_close = kdata.iloc[this_index][dogen.R_CLOSE]        
        ### 若上涨停止
        if  this_close > 0:
            ### 更新take_index
            if take_index is not None:
                take_index = this_index
            break
        ### 达到回调要求, 命中
        heap_falls += abs(this_close)
        if heap_falls >= mini_falls:
            take_index = this_index
        pass
    if take_index is None or take_index > take_valid:
        logger.debug("Don't match valid fallback trade")
        return None
    
    ### 特征四校验
    if kdata.iloc[take_index+1][dogen.MA5] >= kdata.iloc[take_index][dogen.MA5]:
        logger.debug("Don't match valid MA5 at " + kdata.index[take_index])
        return None
    
    ### 结果最后排它校验
    if __exclude_analyze(basic, kdata, pick_index, take_index, maxi_prerise):
        logger.debug("__exclude_analyze() return True")
        return None

    ### 构造结果
    result = {}
    result['code'] = basic.name # 股票代码
    result['name'] = basic[dogen.NAME] #  证券简写
    result['score'] = __score_analyze(basic, kdata, pick_index, take_index) # 打分
    result['industry'] = basic[dogen.INDUSTRY]
    result['take-trade'] = kdata.index[take_index] # 命中交易日
    result['last-close'] = kdata.iloc[0][dogen.P_CLOSE] # 最后一日收盘价
    result['outstanding'] = round(result['last-close'] * basic[dogen.OUTSTANDING], 2) # 流通市值
    result['match-time'] = dogen.datetime_now() # 选中时间

    return result

def match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 涨停回调策略, 有如下特征：
            * 涨停在$maxi_trade个交易日之内;
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
    try:
        db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    except Exception:
        logger.error(traceback.format_exc())
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
                start = dogen.date_delta(end, -__parse_policy_args(policy_args, MAXI_DAYS)
            kdata = db.lookup_stock_kdata(code, start=start, end=end)
            kdata.sort_index(ascending=False, inplace=True)
            
            ### 策略分析
            match = __policy_analyze(basic, kdata, policy_args)
            if match is None:
                continue
            
            ### 输出结果
            match_list.append(match)
        except Exception:
            continue
        pass
    
    ### 保存结果到数据库
    if save_result and len(match_list) > 0:
        db.insert_policy_result(__name__.split('.')[-1], match_list)

    return match_list

