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
PICK_VALID  = 'pick_valid'
TAKE_VALID  = 'take_valid'
MAXI_RISES  = 'maxi_rises'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAXI_DAYS: 90,      # 天
    PICK_VALID: 4,      
    TAKE_VALID: 0,      # 
    MAXI_RISES: 35,
}

def __parse_policy_args(policy_args, arg_name):
    try:
        arg_value = policy_args[arg_name]
    except Exception:
        arg_value = ARGS_DEAULT_VALUE[arg_name]
    return arg_value

def __score_analyze(basic, kdata, pick_index, take_index):
    """ 根据股票股价、市值、成交量等方面给股票打分:
            * 基准分值50分，累积加分项;
            * 股价限高50元，区间定为(50,45],(45,40],...,(5,0]，分值由1~10递增;
            * 市值限高50亿，区间定为(50,45],(45,40],...,(5,0]，分值由1~10递增；
            * 最大涨幅加分，区间定位(0,1],(1,2],...,(9,10],分值由1~10递增;
            * 最大回调加分，区间定位(0,1],(1,2],...,(9,10],分值由1~10递增;
            * 最后5交易日， 连续放量上涨10%，每个交易日2分;
    """
    score = 50

    take_price = kdata.iloc[take_index][dogen.P_CLOSE]
    if (take_price < 50):
        score += (10 - (int)(math.floor(take_price/5)))

    take_value = take_price * basic[dogen.OUTSTANDING]
    if (take_value < 50):
        score += (10 - (int)(math.floor(take_value/5)))

    maxi_index = dogen.get_last_column_max(kdata, dogen.R_CLOSE, eIdx=5)
    if maxi_index is not None:
        score += (int)(math.floor(kdata.iloc[maxi_index][dogen.R_CLOSE]))

    mini_index = dogen.get_last_column_min(kdata, dogen.R_CLOSE, eIdx=5)
    if mini_index is not None:
        score += (int)(math.floor(abs(kdata.iloc[mini_index][dogen.R_CLOSE])))
    
    for temp_index in range(4, -1, -1):
        if kdata.iloc[temp_index+1][dogen.VOLUME]*1.1 <= kdata.iloc[temp_index][dogen.VOLUME]:
            score += 2
        pass

    return score

def __exclude_analyze(basic, kdata, pick_index, take_index, maxi_rises):
    """ 根据日线做排除性校验
    """
    ### taketrade收盘价相对涨停不能过高
    rise_range = dogen.get_last_rise_range(kdata, maxi_rises, max_fall=maxi_rises/2, eIdx=pick_index+1)
    if rise_range is not None:
        logger.debug("Too large rise after %s" % kdata.index[pick_index])
        return True

    ### 三个月内必须有涨停
    tdata = kdata[kdata[dogen.P_CLOSE] >= kdata[dogen.L_HIGH]]
    if tdata.index.size <= 0:
        logger.debug("Don't include hl-trade")
        return True

    return False

def __policy_analyze(basic, kdata, policy_args):
    """ 
    """
    ### 参数解析
    take_valid = __parse_policy_args(policy_args, TAKE_VALID)
    pick_valid = __parse_policy_args(policy_args, PICK_VALID)
    maxi_rises = __parse_policy_args(policy_args, MAXI_RISES)

    ### 特征一：
    for temp_index in range(pick_valid, -1, -1):
        if kdata.iloc[temp_index][dogen.MA5] < kdata.iloc[temp_index][dogen.MA20]:
            logger.debug("Invalid MA5&MA20 at %s" % kdata.index[temp_index])
            return None
        if kdata.iloc[temp_index][dogen.MA20] < kdata.iloc[temp_index+1][dogen.MA20]:
            logger.debug("Invalid MA20 at %s" % kdata.index[temp_index])
            return None
        pass
    
    ### 特征二校验
    pick_index = 0
    for pick_index in range(0, kdata.index.size-1):
        if kdata.iloc[pick_index][dogen.MA5] < kdata.iloc[pick_index][dogen.MA20]:
            pick_index -= 1
            break
        ### 排除放量下跌2个点以上股票
        if kdata.iloc[pick_index][dogen.R_CLOSE] <= -2 and kdata.iloc[pick_index+1][dogen.R_CLOSE]>0:
            if kdata.iloc[pick_index][dogen.VOLUME] > kdata.iloc[pick_index+1][dogen.VOLUME] * 1.1:
                logger.debug("Invalid fall-trade at %s" % kdata.index[pick_index])
                return None
            pass
        pass

    ### 特征三校验
    take_index = None
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
    ### 最近收盘价比take_index高更新, 且放量上涨
    if take_index is not None\
    and kdata.iloc[0][dogen.P_CLOSE] > kdata.iloc[take_index][dogen.P_CLOSE]\
    and kdata.iloc[0][dogen.VOLUME] > kdata.iloc[1][dogen.VOLUME]:
        take_index = 0
    if take_index is None or take_index > take_valid:
        logger.debug("Don't match valid take-trade")
        return None

    ### 结果最后排它校验
    if __exclude_analyze(basic, kdata, pick_index, take_index, maxi_rises):
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
    result[dogen.RST_COL_SCORE]       = __score_analyze(basic, kdata, pick_index, take_index)
    result[dogen.RST_COL_MATCH_TIME]  = dogen.datetime_now() # 选中时间
    result[dogen.RST_COL_INDEX]       = '%s_%s' % (basic.name, kdata.index[take_index]) # 唯一标识，用于持久化去重

    return result

def match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 上涨策略, 有如下特征：
            * 最近交易日MA5突破MA20, 且MA20上涨趋势;
            * pick_valid区间内无放量(10%以上)下跌;
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
