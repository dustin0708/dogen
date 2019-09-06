#-*-coding:utf-8-*-

import sys
import dogen
import traceback

### 导入日志句柄
from dogen import logger

def __policy_analyze(basic, kdata, take_valid, hl_valid, mini_scale, mini_falls):
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
        if (kdata.iloc[pick_index][dogen.VOLUME] * mini_scale) > kdata.iloc[pick_index-1][dogen.VOLUME]:
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

    ### 打分

    ### 构造结果
    result = {}
    result['code'] = basic.name # 股票代码
    result['name'] = basic[dogen.NAME] #  证券简写
    result['score'] = 0 # 估分
    result['industry'] = basic[dogen.INDUSTRY]
    result['take-trade'] = kdata.index[take_index] # 命中交易日
    result['last-close'] = kdata.iloc[0][dogen.P_CLOSE] # 最后一日收盘价
    result['outstanding'] = round(result['last-close'] * basic[dogen.OUTSTANDING], 2) # 流通市值
    result['match-time'] = dogen.datetime_now() # 选中时间

    return result

def parse_policy_args(policy_args):
    """ 解析策略参数

        参数说明：
            policy_args - 策略参数，参数项有：
                            * maxi_days: 自然日数（交易日和非交易日），若start取有效值，该字段无效
                            * take_valid: 命中交易日有效期, 0表示最后一天命中有效
                            * hl_valid: 最后一个涨停有效交易日数
                            * mini_scale: 涨停后一交易日上涨时，放量最小倍数
                            * mini_falls： 回调最小幅度，单位1%
        
        返回结果：
            参数值列表：[maxi_days, take_valid, hl_valid, mini_scale, mini_falls]
    """
    try:
        maxi_days = policy_args['maxi_days']
    except Exception:
        maxi_days=60
    
    try:
        take_valid = policy_args['take_valid']
    except Exception:
        take_valid = 0

    try:
        hl_valid = policy_args['hl_valid']
    except Exception:
        hl_valid = 4
    try:
        mini_scale = policy_args['mini_scale']
    except Exception:
        mini_scale = 1.2
    
    try:
        mini_falls = policy_args['mini_falls']
    except Exception:
        mini_falls = 4 

    return  [maxi_days, take_valid, hl_valid, mini_scale, mini_falls]

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
        db = dogen.DbMongo()
    except Exception:
        logger.error(traceback.format_exc())
        return None
    
    ### 策略参数处理
    [maxi_days, take_valid, hl_valid, mini_scale, mini_falls] = parse_policy_args(policy_args)

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
                start = dogen.date_delta(end, -maxi_days)
            kdata = db.lookup_stock_kdata(code, start=start, end=end)
            kdata.sort_index(ascending=False, inplace=True)
            
            ### 策略分析
            match = __policy_analyze(basic, kdata, take_valid, hl_valid, mini_scale, mini_falls)
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

