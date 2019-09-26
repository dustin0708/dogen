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

### 策略参数名
MAXI_DAYS   = 'maxi_days'
MINI_HL     = 'mini_hl'
MAXI_HL     = 'maxi_hl'
TAKE_VALID  = 'take_valid'
MAXI_PRERISE= 'maxi_prerise'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAXI_DAYS: 30,      # 天
    MINI_HL: 3,      # 
    MAXI_HL: 14,        #
    TAKE_VALID: 0,  # 倍
    MAXI_PRERISE: 35,   # 1%
}

def __parse_policy_args(policy_args, arg_name):
    try:
        arg_value = policy_args[arg_name]
    except Exception:
        arg_value = ARGS_DEAULT_VALUE[arg_name]
    return arg_value

def __score_analyze(basic, kdata, pick_index, take_index):
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

def __exclude_analyze(basic, kdata, pick_index, take_index, maxi_prerise):
    """ 根据日线做排除性校验
    """
    ### 检查区间最高涨幅
    try:
        [min_index, max_index, inc_close, get_lhigh, tmpId] = dogen.get_last_rise_range(kdata, 30, max_fall=20)
        if inc_close > maxi_prerise:
            logger.debug("Too large rise-range from %s to %s" % (kdata.index[min_index], kdata.index[max_index]))
            return True
    except Exception:
        traceback.print_exc()
        pass
    
    ### taketrade收盘价相对涨停不能过高
    if dogen.caculate_incr_percentage(kdata.iloc[take_index][dogen.P_CLOSE], kdata.iloc[pick_index][dogen.P_CLOSE]) > 15:
        logger.debug("Too large rise at %s" % kdata.index[take_index])
        return True

    ### take交易日在ma20之上
    if kdata.iloc[take_index][dogen.P_CLOSE] < kdata.iloc[take_index][dogen.MA20]:
        logger.debug("Invalid take trade at %s" % kdata.index[take_index])
        return True

    return False

def __policy_analyze(basic, kdata, policy_args):
    ### 策略参数处理
    mini_hl     = __parse_policy_args(policy_args, MINI_HL)
    maxi_hl     = __parse_policy_args(policy_args, MAXI_HL)
    take_valid  = __parse_policy_args(policy_args, TAKE_VALID)
    maxi_prerise= __parse_policy_args(policy_args, MAXI_PRERISE)

    ### 特征一校验
    index = dogen.get_highlimit_trades(kdata, eIdx=maxi_hl+1)
    if index.size != 1:
        logger.debug("Don't match highlimit trades")
        return None
    else:
        pick_trade = index[0]
        pick_index = kdata.index.get_loc(pick_trade)
        pick_close = kdata.iloc[pick_index][dogen.P_CLOSE]
    if pick_index < mini_hl:
        logger.debug("Too close hl-trade at %s" % pick_trade)
        return None
    
    ### 特征二校验
    take_index = None
    if pick_index < 5:
        tdata = kdata[0: pick_index]
        tdata = tdata[tdata[dogen.P_CLOSE] < pick_close]
        if tdata.index.size > 0:
            logger.debug("Invalid trade at %s" % tdata.index[0])
            return None
        take_index = 0
    else:
        heap_rises = 0
        for temp_index in range(4, -1, -1):
            temp_close = kdata.iloc[temp_index][dogen.R_CLOSE]
            if temp_close < 0:
                heap_rises = 0
            else:
                heap_rises += temp_close
            if heap_rises >= 5:
                take_index = temp_index
            if temp_close >= 3 and kdata.iloc[temp_index][dogen.R_AMP] >= 5:
                take_index = temp_index
            pass
        ### take_index之后缩量下跌，也符合策略
        if take_index is not None and take_index > 0\
        and kdata.iloc[take_index-1][dogen.R_CLOSE] < 0\
        and kdata.iloc[take_index-1][dogen.VOLUME]  < kdata.iloc[take_index][dogen.VOLUME]:
            take_index-= 1
        ### 最近收盘价比take_index高更新
        if take_index is not None\
        and kdata.iloc[0][dogen.P_CLOSE] > kdata.iloc[take_index][dogen.P_CLOSE]:
            take_index = 0
    if take_index is None or take_index > take_valid or kdata.iloc[take_index][dogen.P_CLOSE] < pick_close:
        logger.debug("Don't match valid fallback trade")
        return None
    
    ### 结果最后排它校验
    if __exclude_analyze(basic, kdata, pick_index, take_index, maxi_prerise):
        logger.debug("__exclude_analyze() return True")
        return None

    ### 构造结果
    result = {}
    result[dogen.RST_COL_CODE]        = basic.name # 股票代码
    result[dogen.RST_COL_NAME]        = basic[dogen.NAME] #  证券简写
    result[dogen.RST_COL_INDUSTRY]    = basic[dogen.INDUSTRY]
    result[dogen.RST_COL_TAKE_TRADE]  = kdata.index[take_index] # 命中交易日
    result[dogen.RST_COL_LAST_CLOSE]  = kdata.iloc[0][dogen.P_CLOSE] # 最后一日收盘价
    result[dogen.RST_COL_OUTSTANDING] = round(kdata.iloc[0][dogen.P_CLOSE] * basic[dogen.OUTSTANDING], 2) # 流通市值
    result[dogen.RST_COL_SCORE]       = __score_analyze(basic, kdata, pick_index, take_index) # 打分
    result[dogen.RST_COL_MATCH_TIME]  = dogen.datetime_now() # 选中时间
    result[dogen.RST_COL_INDEX]       = '%s_%s' % (basic.name, kdata.index[take_index]) # 唯一标识，用于持久化去重

    return result

def match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 涨停上涨策略, 有如下特征：
            * 涨停在[min_hl， max_hl]交易区间以内;
            * 买入信号: 5日以内收盘价均维持在涨停价以上；5日以上累积上涨幅度达5个点或单日涨幅3点振幅5点以上;

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
            kdata.sort_index(ascending=False, inplace=True)
            dogen.drop_fresh_stock_trades(basic, kdata)

            ### 策略分析
            logger.debug("Begin in analyzing %s from %s to %s" % (code, start, end))
            match = __policy_analyze(basic, kdata, policy_args)
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

