#-*-coding:utf-8-*-

import sys
import copy
import pandas
import traceback
import pymongo
import redis

class DbRedis():
    ZSET_HOT_CONCEPT = 'hot_concept'

    def __init__(self, host='localhost', port=6379):
        try:
            self.redis = redis.Redis(host=host, port=6379)
        except Exception:
            self.redis = None
        pass

    def connect(self):
        return self.redis

    def keyof_hot_concept(self, date):
        return self.ZSET_HOT_CONCEPT+'.'+date

    def incry_hot_concept(self, date, cnpt_name):
        if self.redis is None:
            return None

        rst = None
        if isinstance(cnpt_name, str):
            rst = self.redis.zincrby(self.keyof_hot_concept(date), 1, cnpt_name)
        elif isinstance(cnpt_name, list):
            for temp in cnpt_name:
                rst = self.redis.zincrby(self.keyof_hot_concept(date), 1, temp)
            pass
        else:
            pass
        return rst

    def fetch_hot_concept(self, date, num=5000, ascending=False):
        if self.redis is None:
            return None
        cnpt = self.redis.zrevrangebyscore(self.keyof_hot_concept(date), 5000, 0, start=0, num=num, withscores=False)

        hots = []
        for temp in range(0, len(cnpt)):
            hots.append(cnpt[temp].decode('utf-8'))
        return hots

    def clear_hot_concept(self, date):
        if self.redis is None:
            return None
        if isinstance(date, str):
            self.redis.zremrangebyrank(self.keyof_hot_concept(date), 0, -1)
        elif isinstance(date, list):
            for temp in date:
                self.redis.zremrangebyrank(self.keyof_hot_concept(temp), 0, -1)
            pass
        pass

class DbMongo():

    TBL_BASICS = "basics" # 股票基本信息数据表
    TBL_ALL_CONCEPT = 'all_concept'
    TBL_HOT_CONCEPT = 'hot_concept'
    TBL_HOT_DATE    = 'hot_date'
    TBL_HOT_CNPT    = 'hot_cnpt'
    TBL_KDATA_PREFIX = "kdata" # 股票交易数据表前缀
    TBL_POLICY_PREFIX = "policy" # 策略结果表前缀
    TBL_STAT_LR_RANGE = "stat_lr_range" # 大涨统计表前缀
    
    def __init__(self, uri="mongodb://127.0.0.1:27017", database="Dogen"):
        """ 初始化函数
        
            参数说明:
                uri - mongodb数据库服务器地址，多个用“;”号隔开
                database - 默认数据库名
        """
        try:
            self.client = pymongo.MongoClient(uri)
            self.database = self.client[database]
        except Exception:
            self.client = None
            self.database = None
            traceback.print_exc()
        pass
    
    def connect(self):
        """ 连接数据库服务器

            返回结果:
                成功返回True，失败返回False
        """
        try:
            self.client.admin.command('ismaster')
        except Exception:
            self.client = None
            self.database = None
            traceback.print_exc()
            return False
        return True


    def insert_stock_basic(self, code, basic, field='_id'):
        """ 保存股票基本数据，以股票代码为键值，tushare下载basics数据timeToMarket类型不能为object.

            参数说明：
                code - 股票代码
                basic - 股票基本数据，Series类型
                field - 在数据中插入唯一键标识

            返回值：
                保存成功返回True，否则返回False
        """
        if self.database is None:
            return False

        ### Series数据转dict存储
        data = basic.to_dict()
        data[field] = code

        ### 先删除后添加
        self.delete_stock_basic(code)
               
        try:
            base = self.database[self.TBL_BASICS]
            base.insert_one(data)
            return True
        except Exception:
            pass
            
        return False
    
    def delete_stock_basic(self, code=None, field='_id'):
        """ 删除股票基本数据，不指定code将删除所有股票的basic数据

            参数说明：
                code - 股票代码
                field - 键值标识，必须与保存时一致
            
            返回值：
                None
        """
        if self.database is None:
            return None

        cond = {}
        if code is not None:
            cond[field] = code
        
        try:
            base = self.database[self.TBL_BASICS]
            base.delete_many(cond)
        except Exception:
            pass
            
        return None

    def lookup_stock_basic(self, code, field='_id'):
        """ 获取股票元数据

            参数说明：
                code - 股票代码
                field - 键值标识，必须与保存时一致
            
            返回值：
                若查询成功，返回基本信息（Series类型）；否则返回None
        """
        if self.database is None:
            return None
            
        cond = {field: code}
        
        try:
            base = self.database[self.TBL_BASICS]
            for data in base.find(cond):
                del data[field]
                
                ### dict转Series返回
                data = pandas.Series(data)
                
                ### Series的name属性丢失, 用code还原
                data.name = code
                return data
            pass
        except Exception:
            pass
        return None
    
    def lookup_stock_basics(self, field='_id'):
        """ 获取股票元数据

            参数说明：
                field - 键值标识，必须与保存时一致
            
            返回值：
                若查询成功，返回基本信息（Series类型）；否则返回None
        """
        if self.database is None:
            return None
        
        try:
            base = self.database[self.TBL_BASICS]
            array = []
            for row in base.find():
                row['code'] = row[field]
                del row[field]
                array.append(row)

            basics = pandas.DataFrame.from_dict(array, orient="columns")
            basics.set_index('code', inplace=True)
            return basics
        except Exception:
            pass
        return None

    def lookup_stock_codes(self, field='_id'):
        """ 返回数据库中股票代码列表

            参数说明：
                field - 键值标识，必须与保存时一致
            
             返回值：
                成功返回代码列表，否则返回None
        """
        if self.database is None:
            return None

        try:
            code = []
            base = self.database[self.TBL_BASICS]
            for data in base.find({}, {field:1}):
                code.append(data[field])
        except Exception:
            pass
        return code

    def _return_kdata_collection(self, code):
        """ 返回股票k线数据集合
        """
        return self.TBL_KDATA_PREFIX+'_'+code
    
    def insert_stock_kdata(self, code, kdata, key_index, key_field='_id'):
        """ 保存股票日线数据，每只股票单独存储一个collection，以“prefix_code”为集合标识

            参数说明：
                code - 股票代码
                kdata - 交易日线数据，DataFrame类型
                key_index - 数据键值索引，Series类型
                key_field - 在数据中插入唯一键值标识

            返回结果：
                保存成功返回True，否则返回False
        """
        if self.database is None:
            return False
        
        ### dataframe数据处理
        kdata.insert(0, key_field, key_index)
        data = kdata.to_dict(orient='records')
        
        try:
            ### 插入数据（索引丢失）
            coll = self.database[self._return_kdata_collection(code)]
            coll.insert_many(data)
            return True
        except Exception:
            pass
            
        return False
        
    def delete_stock_kdata(self, code):
        """ 清理指定股票日线数据

            参数说明：
                code - 股票代码

            返回结果：
                None
        """
        if self.database is None:
            return None
        
        try:
            coll = self.database[self._return_kdata_collection(code)]
            coll.drop()
        except Exception:
            pass
            
        return None
    
    def lookup_stock_kdata(self, code, start=None, end=None, key_field='_id'):
        """ 读取股票交易数据

            参数说明：
                code - 股票代码
                start - 起始交易日
                end - 截止交易日
                key_field - 键值标识，必须与保存时一致

            返回结果：
                成功返回交易日数据（DataFrame类型），否则返回None
        """
        if self.database is None:
            return None
        
        ### 条件处理
        cond = {}
        if start is not None:
            cond['$gte'] = start
        if end is not None:
            cond['$lte'] = end
        if start is not None or end is not None:
            cond = {key_field: cond}
        
        try:
            ### 提取字典数据
            data = []
            coll = self.database[self._return_kdata_collection(code)]

            for x in coll.find(cond):
                data.append(x)
            
            ### 字典数据转换为DataFrame
            data = pandas.DataFrame.from_dict(data, orient='columns')
            data.set_index(key_field, inplace=True)
            data.sort_index(ascending=False, inplace=True)
        except Exception:
            data = None            
            
        return data
    
    def lookup_stock_kdata_range(self, code, key_field='_id'):
        """ 获取指定股票样本数据区间

            参数说明：
                code - 股票代码
                key_field - 键值标识，必须与保存时一致
            
            返回结果：
                成功返回键值元组(start, end)，否则返回None
        """ 
        if self.database is None:
            return None
        
        coll = self.database[self._return_kdata_collection(code)]
        
        try:
            ### 获取最早数据
            from_data = None
            for x in coll.find().sort(key_field, pymongo.ASCENDING).limit(1):
                from_data = x
            
            ### 获取最晚数据
            last_data = None
            for x in coll.find().sort(key_field, pymongo.DESCENDING).limit(1):
                last_data = x
            
            if from_data is not None and last_data is not None:
                return (from_data[key_field], last_data[key_field])
        except Exception:
            pass
            
        return (None, None)

    def _return_policy_collection(self, policy):
        """ 返回策略数据集合
        """
        return self.TBL_POLICY_PREFIX+'_'+policy

    def insert_policy_result(self, policy, result, key_name=None):
        """ 保存策略结果到数据库

            参数说明：
                policy - 策略名
                result - 结果列表, 子项为dict
                key_name - 结果子项中唯一键标识
            
            返回结果：
                成功返回True，否则返回False
        """
        if self.database is None:
            return False

        coll = self.database[self._return_policy_collection(policy)]

        try:
            ### 根据数据键值逐个去重
            if key_name is not None:
                for item in result:
                    cond = {key_name: item[key_name]}
                    coll.delete_one(cond)
                pass

            ### 插入多条数据
            coll.insert_many(copy.deepcopy(result))
            return True
        except Exception:
            pass
        return False
    
    def _return_stat_lr_range_collection(self):
        return self.TBL_STAT_LR_RANGE

    def lookup_statistics_largerise_range(self, cond={}, descending_by=None, key_field='_id'):
        if self.database is None:
            return False

        coll = self.database[self._return_stat_lr_range_collection()]

        result = []
        try:
            if descending_by is not None:
                for data in coll.find(cond).sort([(descending_by, pymongo.DESCENDING)]):
                    del data[key_field]
                    result.append(data)
            else:
                for data in coll.find(cond):
                    del data[key_field]
                    result.append(data)
        except Exception:
            result = None

        return result

    def insert_statistics_largerise_range(self, result, key_name=None):
        """ 保存大涨区间统计结果
        """
        if self.database is None:
            return False

        coll = self.database[self._return_stat_lr_range_collection()]

        try:
            ### 根据数据键值逐个去重
            if key_name is not None:
                for item in result:
                    cond = {key_name: item[key_name]}
                    coll.delete_one(cond)
                pass

            coll.insert_many(copy.deepcopy(result))
            return True
        except Exception:
            pass
        return False

    def delete_stock_concept(self):
        if self.database is None:
            return False
        
        coll = self.database[self.TBL_ALL_CONCEPT]
        coll.drop()

        return None

    def insert_stock_concept(self, cnpt, key_field='_id'):
        """ 概念数据

            参数说明：
                cnpt - 股票代码
                key_field - 在数据中插入唯一键标识

            返回值：
                保存成功返回True，否则返回False
        """
        if self.database is None:
            return False
               
        try:
            coll = self.database[self.TBL_ALL_CONCEPT]
            coll.insert_many(copy.deepcopy(cnpt))
            return True
        except Exception:
            pass
            
        return False
    
    def lookup_stock_concept(self, cond={}, key_field='_id'):
        """
        """
        if self.database is None:
            return False

        try:
            recs = []
            coll = self.database[self.TBL_ALL_CONCEPT]
            for data in coll.find(cond):
                del data[key_field]
                recs.append(data)
            return recs
        except Exception:
            pass
        
        return None

    def delete_hot_concept(self):
        if self.database is None:
            return False
        
        coll = self.database[self.TBL_HOT_CONCEPT]
        coll.drop()

        return None

    def insert_hot_concept(self, date, cnpt, key_field='_id'):
        """ 概念数据

            参数说明：
                date - 日期
                cnpt - 概念列表

            返回值：
                保存成功返回True，否则返回False
        """
        if self.database is None:
            return False
               
        item = {}
        item[self.TBL_HOT_DATE] = date
        item[self.TBL_HOT_CNPT] = cnpt

        try:
            coll = self.database[self.TBL_HOT_CONCEPT]
            coll.insert_one(copy.deepcopy(item))
            return True
        except Exception:
            pass
            
        return False
    
    def lookup_hot_concept(self, date=None, key_field='_id'):
        """
        """
        if self.database is None:
            return False

        if date is not None:
            cond = {key_field: date}
            try:
                date = None
                cnpt = None
                coll = self.database[self.TBL_HOT_CONCEPT]
                for data in coll.find(cond):
                    date = data[self.TBL_HOT_DATE]
                    cnpt = data[self.TBL_HOT_CNPT]
                    if date is not None and cnpt is not None:
                        return [date, cnpt]
                    pass
            except Exception:
                pass
        else:
            cond = {}
            try:
                recs = []
                coll = self.database[self.TBL_HOT_CONCEPT]
                for data in coll.find(cond):
                    date = data[self.TBL_HOT_DATE]
                    cnpt = data[self.TBL_HOT_CNPT]
                    if date is not None and cnpt is not None:
                        recs.append([date, cnpt])
                    pass
            except Exception:
                pass
        
        return None