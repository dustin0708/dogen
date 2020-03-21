#-*-coding:utf-8-*-

import sys
import dogen
import redis
import traceback

### 导入日志句柄
from dogen import logger, mongo_server, mongo_database, redis_server

def update_kdata(codes, full=False, start=None, end=None):
    """ 从网络侧更新股票数据

        参数说明：
            codes - 更新股票列表
            full - 是否进行全量更新
            start - 起始交易日
            end - 截止交易日
        
        返回结果：
            成功更新数据列表
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None

    ### 设置截止日期
    if end is None:
        end = dogen.date_today()
    
    ### 下载basic数据
    basics = dogen.download_basics()
    
    ### 下载参数指定股票数据
    success_list = []
    for code in codes:
        ### 如果全量下载, 则清除数据库
        try:
            if full:
                db.delete_stock_basic(code=code)
                db.delete_stock_kdata(code)

                from_trade = None
                last_trade = None            
            else:
                ### 读取数据区间
                from_trade, last_trade = db.lookup_stock_kdata_range(code)
        
            ### 区间数据不存在, 默认下载end之前两年数据
            if full or from_trade is None or last_trade is None:
                if start is None:
                    start = dogen.date_delta(end, -365*2)
                pass
            else:
                ### 增量下载需要保证数据处理加工的正确性（MA20/MACD）
                start = dogen.date_delta(last_trade, -90)
        
            ### 下载日线数据
            logger.debug("Begin download %s's kdata from %s to %s." % (code, start, end))
            kdata = dogen.download_kdata(code, start=start, end=end)
            if kdata is None or kdata.index.size <= 0:
                continue

            ### 截取新增数据插入, 数据已存在会导致出错
            if from_trade is not None and last_trade is not None:
                kdata = kdata.loc[(kdata.index<from_trade) | (kdata.index>last_trade)]
            
            ### 写数据库
            if kdata is not None and kdata.index.size > 0:
                ### 非指数写基本信息
                if code.isdigit():
                    basic = basics.loc[code]
                    db.insert_stock_basic(code, basic)
                db.insert_stock_kdata(code, kdata, kdata.index)
                success_list.append(code)

            logger.debug("Success update %s with %d items." % (code, kdata.index.size))
        except Exception:
            logger.error('Trggered in handling code %s: %s' % (code, traceback.format_exc()))
            continue
        pass
        
    return success_list

def update_hot_concept(start=None, end=None, num=1, save_result=False):
    """ 找热点概念

        参数：
            start: 开始日期
            end: 截止时间，None取最近交易日
            num: 计算日期数，0表示所有的
 
    """
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None

    rd = dogen.DbRedis()
    if not rd.connect():
        logger.error("Cannot connect to redis-server %s" % redis_server)
        return None

    ### 修正日期
    index = db.lookup_stock_kdata(dogen.get_index_of_sh(), start=start, end=end)

    ### 修正num参数
    if num==0 or num > index.index.size:
        num = index.index.size

    ### 读取代码
    codes = db.lookup_stock_codes()

    for code in codes:
        kdata = db.lookup_stock_kdata(code, start=start,end=end)
        if kdata.iloc[0][dogen.P_CLOSE] < kdata.iloc[0][dogen.L_HIGH]:
            continue

        ### 行业&概念
        indt = dogen.lookup_industry(db, code)
        cnpt = dogen.lookup_concept(db, code)
        if indt is None or cnpt is None:
            continue

        ### 概念计数
        for temp_trade in kdata.index:
            rd.incry_hot_concept(kdata.loc[temp_trade], indt)
            rd.incry_hot_concept(kdata.loc[temp_trade], cnpt)

        pass

    rst = []

    ### 排序获取结果&清除临时数据
    for temp_index in range(0, num):
        hots = rd.fetch_hot_concept(index.index[temp_index])

        ### 写数据库
        if save_result:
            db.insert_hot_concept(index.index[temp_index], hots)
        rst.append((index.index[temp_index], hots))
        rd.clear_hot_concept(index.index[temp_index])

    return rst

def update_all_concept_from_html(filename='thsgn.html'):
    """ 从同花顺文件更新概念
    """
    ### 数据库连接初始化
    db = dogen.DbMongo(uri=mongo_server, database=mongo_database)
    if not db.connect():
        logger.error("Cannot connect to mongo-server %s" % mongo_server)
        return None

    try:
        cnpt = dogen.parse_thsgn_file(filename)
        db.delete_stock_concept()
        db.insert_stock_concept(cnpt)
    except Exception:
        pass

    return None