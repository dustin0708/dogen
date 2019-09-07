#-*-coding:utf-8-*-

import sys
import dogen
import traceback

### 导入日志句柄
from dogen import logger, mongo_server, mongo_database

def __statistics_analyze(basic, kdata, mini_rise, mini_hl):
    """ 统计单只股票上涨区间

        参数说明：
            basic - 股票基本信息
            kdata - 股票交易数据
            mini_rise - 区间最小涨幅
            mini_hl - 区间最少涨停板数
    """
    tmpId = 0
    match = []

    while True:
        range = dogen.get_last_rise_range(kdata, mini_rise, max_fall=round(mini_rise/2, 2), sIdx=tmpId)
        if range is None:
            break
        else:
            [min_index, max_index, inc_close, get_hl, tmpId] = range

        ### 忽略不符合涨停要求的区间
        if get_hl < mini_hl:
            continue
            
        ### 保存区间结果
        result = {}
        result['code'] = basic.name
        result['name'] = basic[dogen.NAME] #  证券简写
        result['industry'] = basic[dogen.INDUSTRY]
        result['start'] = kdata.index[min_index]
        result['end'] = kdata.index[max_index]
        result['rise_rate'] = inc_close
        result['inc_hl'] = get_hl
        result['start-close'] = kdata.iloc[min_index][dogen.P_CLOSE] # 最后一日收盘价
        result['outstanding'] = round(result['start-close'] * basic[dogen.OUTSTANDING], 2) # 流通市值
        result['match-time'] = dogen.datetime_now() # 选中时间

        match.append(result)

    return match

def find_largerise_range(codes, start=None, end=None, save_result=False, args=[20, 2]):
    """ 查找大涨的股票数据

        参数说明：
            start - 起始日期，未设置默认分析3个月以内的数据
            end - 截止日期
            save_result - 是否保存结果
            args - args[0]表示最少30个点涨幅；arg[1]表示至少包括两个涨停板

        返回结果：
            区间列表如[{item-1}, {item-2}, ... , {itemn-}]
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None

    ### 股票代码过滤，如科创板
    codes = dogen.drop_codes(codes)

    ### 依次统计检查
    match_list = []
    for code in codes:
        try:
            ### 从数据库读取basic数据
            basic = db.lookup_stock_basic(code)

            ### 从数据库读取日线数据，必须按索引（日期）降序排列
            if end is None:
                end = dogen.date_today()
            if start is None:
                start = dogen.date_delta(end, -90)
            kdata = db.lookup_stock_kdata(code, start=start, end=end)
            kdata.sort_index(ascending=False, inplace=True)
            
            ### 统计分析
            logger.debug("Begin in analyzing %s from %s to %s" % (code, start, end))
            match = __statistics_analyze(basic, kdata, args[0], args[1])
            if match is None:
                continue
            
            ### 输出结果
            match_list.extend(match)
        except Exception:
            continue
        pass
    
    ### 保存结果到数据库
    if save_result and len(match_list) > 0:
        pass

    return match_list
