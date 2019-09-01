#-*-coding:utf-8-*-

import sys
import pandas
import traceback
import pymongo

class DbMongo():

    BASICS   = "basics"
    METADATA = "metadata"
    KDATA    = "kdata"

    
    def __init__(self, uri="mongodb://127.0.0.1:27017", database="Dogen"):
        try:
            self.client = pymongo.MongoClient(uri)
            self.database = self.client[database]
        except Exception:
            self.client = None
            self.database = None
            traceback.print_exc()
        pass

    def insert_stock_basic(self, code, basic):
        """ 插入股票基本数据, tushare下载basics数据timeToMarket类型不能为object
        """
        if self.database is None:
            return None

        ### Series数据转dict存储
        data = basic.to_dict()
        data['_id'] = code

        ### 先删除后添加
        self.delete_stock_basic(code)
               
        base = self.database[self.BASICS]
        base.insert_one(data)
        return None
    
    def delete_stock_basic(self, code=None):
        """ 删除股票基本数据
        """
        if self.database is None:
            return None

        cond = {}
        if code is not None:
            cond['_id'] = code
            
        base = self.database[self.BASICS]
        base.delete_many(cond)
        return None

    def lookup_stock_basic(self, code):
        """ 获取股票元数据
        """
        if self.database is None:
            return None
            
        cond = {'_id': code}
        base = self.database[self.BASICS]
        for data in base.find(cond):
            del data['_id']
            
            ### dict转Series返回
            data = pandas.Series(data)
            
            ### Series的name属性丢失, 用code还原
            data.name = code
            return data
        return None
    
    def return_stock_collection(self, code):
        """ 返回股票k线数据集合
        """
        return self.KDATA+'_'+code
    
    def insert_stock_kdata(self, code, kdata):
        """ 插入股票日线数据
        """
        if self.database is None:
            return None
        
        ### dataframe数据处理
        kdata.insert(0, '_id', kdata.index)
        data = kdata.to_dict(orient='records')
        
        ### 插入数据（索引丢失）
        coll = self.database[self.return_stock_collection(code)]
        coll.insert_many(data)
        return None
        
    def delete_stock_kdata(self, code):
        """ 清理股票日线数据, 默认清除所以数据
        """
        if self.database is None:
            return None
        
        coll = self.database[self.return_stock_collection(code)]
        coll.drop()
        return None
    
    def lookup_stock_kdata(self, code, start=None, end=None):
        """ 读取股票交易数据
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
            cond = {'_id': cond}
        
        ### 提取字典数据
        data = []
        coll = self.database[self.return_stock_collection(code)]
        for x in coll.find(cond):
            data.append(x)
        
        ### 字典数据转换为DataFrame
        data = pandas.DataFrame.from_dict(data, orient='columns')
        data.set_index('_id', inplace=True)
        return data
    
    def lookup_stock_kdata_last(self, code):
        """ 获取最后一个交易日区间
        """ 
        if self.database is None:
            return None
        
        coll = self.database[self.return_stock_collection(code)]
        
        ### 获取最早数据
        from_data = None
        for x in coll.find().sort('_id', pymongo.ASCENDING).limit(1):
            from_data = x
        
        ### 获取最晚数据
        last_data = None
        for x in coll.find().sort('_id', pymongo.DESCENDING).limit(1):
            last_data = x
        
        if from_data is not None and last_data is not None:
            return (from_data['_id'], last_data['_id'])
            
        return (None, None)
    
if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.")