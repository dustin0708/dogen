#-*-coding:utf-8-*-

import sys
import dogen
import traceback

### 导入日志句柄
from dogen import logger, mongo_server, mongo_database

### 策略参数名
MINI_RISE  = 'mini_rise'
MINI_HL = 'mini_hl'

### 策略参数经验值(默认值)
ARGS_DEAULT_VALUE = {
    MINI_RISE: 50,      # 天
    MINI_HL: 3, # 最少涨停数
}

def __parse_policy_args(policy_args, arg_name):
    try:
        arg_value = policy_args[arg_name]
    except Exception:
        arg_value = ARGS_DEAULT_VALUE[arg_name]
    return arg_value

def __statistics_analyze(db, basic, kdata, save_result, args):
    """ 统计单只股票上涨区间

        参数说明：
            basic - 股票基本信息
            kdata - 股票交易数据
            mini_rise - 区间最小涨幅
            mini_hl - 区间最少涨停板数
    """
    ### 参数处理
    mini_rise = __parse_policy_args(args, MINI_RISE)
    mini_hl = __parse_policy_args(args, MINI_HL)

    ### 循环检查
    tmpId = 0
    match = []
    while True:
        rise_range = dogen.get_last_rise_range(kdata, mini_rise, max_fall=round(mini_rise/2, 2), sIdx=tmpId)
        if rise_range is None:
            break
        else:
            [min_index, max_index, inc_close, get_hl, tmpId] = rise_range

        ### 忽略不符合连板要求的区间
        max_hl_serial = 0
        tmp_hl_serial = 0
        for temp_index in range(min_index, max_index-1, -1):
            if kdata.iloc[temp_index][dogen.P_CLOSE] < kdata.iloc[temp_index][dogen.L_HIGH]:
                tmp_hl_serial = 0
                continue
            tmp_hl_serial += 1
            if tmp_hl_serial > max_hl_serial:
                max_hl_serial = tmp_hl_serial
            pass
        if max_hl_serial < mini_hl:
            continue
            
        ### 保存区间结果
        result = {}
        result[dogen.RST_COL_CODE]        = basic.name
        result[dogen.RST_COL_NAME]        = basic[dogen.NAME] #  证券简写
        result[dogen.RST_COL_INDUSTRY]    = basic[dogen.INDUSTRY]
        result[dogen.RST_COL_START]       = kdata.index[min_index]
        result[dogen.RST_COL_END]         = kdata.index[max_index]
        result[dogen.RST_COL_START_CLOSE] = kdata.iloc[min_index][dogen.P_CLOSE] # 起始收盘价
        result[dogen.RST_COL_LAST_CLOSE]  = kdata.iloc[max_index][dogen.P_CLOSE] # 最高收盘价
        result[dogen.RST_COL_RISE_RATE]   = dogen.caculate_incr_percentage(result[dogen.RST_COL_LAST_CLOSE], result[dogen.RST_COL_START_CLOSE])
        result[dogen.RST_COL_INC_HL]      = get_hl
        result[dogen.RST_COL_OUTSTANDING] = round(kdata.iloc[min_index][dogen.P_CLOSE] * basic[dogen.OUTSTANDING], 2) # 流通市值
        result[dogen.RST_COL_MATCH_TIME]  = dogen.datetime_now() # 选中时间
        result[dogen.RST_COL_INDEX]       = '%s_%s' % (basic.name, kdata.index[min_index])

        match.append(result)

        ### 保存结果
        if save_result:
            rdata = db.lookup_statistics_largerise_range(cond={dogen.RST_COL_CODE:basic.name}, descending_by=dogen.RST_COL_START)
            if rdata is None or len(rdata)<=0:
                db.insert_statistics_largerise_range([result], key_name=dogen.RST_COL_INDEX)
                continue
            rlast = rdata[0]
            if rlast[dogen.RST_COL_END] < result[dogen.RST_COL_START]:
                db.insert_statistics_largerise_range([result], key_name=dogen.RST_COL_INDEX)
                continue
            ### 重叠合并
            tdata = kdata[(kdata.index > rlast[dogen.RST_COL_END])&(kdata.index <= result[dogen.RST_COL_END])]

            rlast[dogen.RST_COL_INC_HL] = rlast[dogen.RST_COL_INC_HL] + tdata[tdata[dogen.P_CLOSE]>=tdata[dogen.L_HIGH]].index.size
            rlast[dogen.RST_COL_END] = result[dogen.RST_COL_END]
            rlast[dogen.RST_COL_LAST_CLOSE] = result[dogen.RST_COL_LAST_CLOSE]
            rlast[dogen.RST_COL_RISE_RATE] = dogen.caculate_incr_percentage(rlast[dogen.RST_COL_LAST_CLOSE], rlast[dogen.RST_COL_START_CLOSE])
            rlast[dogen.RST_COL_MATCH_TIME] = result[dogen.RST_COL_MATCH_TIME]
            db.insert_statistics_largerise_range([rlast], key_name=dogen.RST_COL_INDEX)

    return match

def find_largerise_range(codes, start=None, end=None, save_result=False, args=None):
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
                start = dogen.date_delta(end, -30)
            kdata = db.lookup_stock_kdata(code, start=start, end=end)
            if kdata is None or kdata.index.size <= 0:
                continue
            dogen.drop_fresh_stock_trades(basic, kdata)
            
            ### 统计分析
            if kdata is not None and kdata.index.size > 0:
                logger.debug("Begin in analyzing %s from %s to %s" % (code, start, end))
                match = __statistics_analyze(db, basic, kdata, save_result, args)
                if match is None:
                    continue            
                ### 输出结果
                match_list.extend(match)
                
        except Exception:
            logger.error('Trggered in handling code %s: %s' % (code, traceback.format_exc()))
            continue
        pass

    return match_list
