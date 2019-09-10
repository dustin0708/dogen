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
HIGH_VALID  = 'high_valid'
VOLUME_SCALE= 'volume_scale'
MINI_FALLS  = 'mini_falls'
MAXI_RISES  = 'maxi_rises'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MAXI_DAYS: 90,      # 天
    PICK_VALID: 9,       #
    TAKE_VALID: 0,      # 
    VOLUME_SCALE: 1.2,  # 倍
    MINI_FALLS: 25,   # 1%
    MAXI_RISES: 15,
}

def __parse_policy_args(policy_args, arg_name):
    try:
        arg_value = policy_args[arg_name]
    except Exception:
        arg_value = ARGS_DEAULT_VALUE[arg_name]
    return arg_value

def __policy_analyze(basic, kdata, policy_args):
    """ 
    """
    ### 参数解析
    pick_valid = __parse_policy_args(policy_args, PICK_VALID)
    take_valid = __parse_policy_args(policy_args, TAKE_VALID)
    mini_falls = __parse_policy_args(policy_args, MINI_FALLS)
    maxi_rises = __parse_policy_args(policy_args, MAXI_RISES)

    ### 特征一：获取有效跌幅区间
    trange = dogen.get_last_fall_range(kdata, mini_falls, max_rise=mini_falls)
    if trange is None:
        logger.debug("Don't get valid fall-range")
        return None
    else:
        [max_index, min_index, dec_close, get_llow, tmpId] = trange
    ### 校验min_index之后涨幅区间
    trange = dogen.get_last_rise_range(kdata, maxi_rises, max_fall=maxi_rises, eIdx=min_index)
    if trange is not None:
        logger.debug("Get invalid rise-range")
        return None
    ### min_index在有效期内
    if min_index > pick_valid:
        logger.debug("Too long from min-trade at %s" % kdata.index[min_index])
        return None
    
    ### 特征二：校验区间[max_index+HIGH_VALID,min_index是否有涨停]
    tdata = kdata[kdata[dogen.P_CLOSE] >=  kdata[dogen.L_HIGH]]
    if tdata.index.size <= 0:
        logger.debug("Don't include highlimit trade from %s to %s" % (kdata.index[max_index], kdata.index[min_index]))
        return None

    ### 特征三：
    ma5__index = None
    rise_index = None
    take_index = None
    for i in range(min_index, -1, -1):
        if kdata.iloc[i][dogen.MA5] >= kdata.iloc[i+1][dogen.MA5]:
            ma5__index = i
        if kdata.iloc[i][dogen.R_AMP] >= kdata.iloc[i][dogen.R_CLOSE] > 0:
            rise_index = i
        if ma5__index is not None and rise_index is not None:
            take_index = [ma5__index, rise_index][rise_index < ma5__index]
        pass
    if take_index is None or take_index > take_valid:
        logger.debug("Don't get valid take-trade")
        return None

    ### 构造结果
    result = {}
    result[dogen.RST_COL_CODE]        = basic.name # 股票代码
    result[dogen.RST_COL_NAME]        = basic[dogen.NAME] #  证券简写
    result[dogen.RST_COL_INDUSTRY]    = basic[dogen.INDUSTRY]
    result[dogen.RST_COL_TAKE_TRADE]  = kdata.index[take_index] # 命中交易日
    result[dogen.RST_COL_LAST_CLOSE]  = kdata.iloc[0][dogen.P_CLOSE] # 最后一日收盘价
    result[dogen.RST_COL_OUTSTANDING] = round(kdata.iloc[0][dogen.P_CLOSE] * basic[dogen.OUTSTANDING], 2) # 流通市值
    result[dogen.RST_COL_SCORE]       = 60
    result[dogen.RST_COL_MATCH_TIME]  = dogen.datetime_now() # 选中时间
    result[dogen.RST_COL_INDEX]       = '%s_%s' % (basic.name, kdata.index[take_index]) # 唯一标识，用于持久化去重

    return result

def match(codes, start=None, end=None, save_result=False, policy_args=None):
    """ 反弹策略, 有如下特征：
            * $MAXI_DAYS交易日内，区间[max_index, min_index]下跌30%以上;
            * [max_index+10, min_index]区间至少有一个涨停;
            * min_index之后出现MA5上涨，振幅大于5且上涨交易日；

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
