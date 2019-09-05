#-*-coding:utf-8-*-

import sys
import pandas
import traceback
import pymongo

class DbMongo():

    TBL_BASICS = "basics"
    TBL_KDATA_PREFIX = "kdata"
    TBL_POLICY_PREFIX = 'policy'
    
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
        except Exception:
            data = None            
            
        return data
    
    def lookup_stock_kdata_last(self, code, key_field='_id'):
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

    def insert_policy_result(self, policy, result):
        """ 保存策略结果到数据库

            参数说明：
                policy - 策略名
                result - 结果列表
            
            返回结果：
                成功返回True，否则返回False
        """
        if self.database is None:
            return False

        coll = self.database[self._return_policy_collection(policy)]

        try:
            coll.insert_many(result)
        except Exception:
            pass
        return False