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

    def insert_stock_basic(self, code, basic, field='_id'):
        """ 插入股票基本数据, tushare下载basics数据timeToMarket类型不能为object
        """
        if self.database is None:
            return False

        ### Series数据转dict存储
        data = basic.to_dict()
        data[field] = code

        ### 先删除后添加
        self.delete_stock_basic(code)
               
        try:
            base = self.database[self.BASICS]
            base.insert_one(data)
            return True
        except Exception:
            pass
            
        return False
    
    def delete_stock_basic(self, code=None, field='_id'):
        """ 删除股票基本数据
        """
        if self.database is None:
            return None

        cond = {}
        if code is not None:
            cond[field] = code
        
        try:
            base = self.database[self.BASICS]
            base.delete_many(cond)
        except Exception:
            pass
            
        return None

    def lookup_stock_basic(self, code, field='_id'):
        """ 获取股票元数据
        """
        if self.database is None:
            return None
            
        cond = {field: code}
        
        try:
            base = self.database[self.BASICS]
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
    
    def return_stock_collection(self, code):
        """ 返回股票k线数据集合
        """
        return self.KDATA+'_'+code
    
    def insert_stock_kdata(self, code, kdata, key_index, key_field='_id'):
        """ 插入股票日线数据
        """
        if self.database is None:
            return False
        
        ### dataframe数据处理
        kdata.insert(0, key_field, key_index)
        data = kdata.to_dict(orient='records')
        
        try:
            ### 插入数据（索引丢失）
            coll = self.database[self.return_stock_collection(code)]
            coll.insert_many(data)
            return True
        except Exception:
            pass
            
        return False
        
    def delete_stock_kdata(self, code):
        """ 清理股票日线数据, 默认清除所以数据
        """
        if self.database is None:
            return None
        
        try:
            coll = self.database[self.return_stock_collection(code)]
            coll.drop()
        except Exception:
            pass
            
        return None
    
    def lookup_stock_kdata(self, code, start=None, end=None, key='_id'):
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
            cond = {key: cond}
        
        try:
            ### 提取字典数据
            data = []
            coll = self.database[self.return_stock_collection(code)]
            for x in coll.find(cond):
                data.append(x)
            
            ### 字典数据转换为DataFrame
            data = pandas.DataFrame.from_dict(data, orient='columns')
            data.set_index(key, inplace=True)
        except Exception:
            data = None            
            
        return data
    
    def lookup_stock_kdata_last(self, code, key='_id'):
        """ 获取最后一个交易日区间
        """ 
        if self.database is None:
            return None
        
        coll = self.database[self.return_stock_collection(code)]
        
        try:
            ### 获取最早数据
            from_data = None
            for x in coll.find().sort(key, pymongo.ASCENDING).limit(1):
                from_data = x
            
            ### 获取最晚数据
            last_data = None
            for x in coll.find().sort(key, pymongo.DESCENDING).limit(1):
                last_data = x
            
            if from_data is not None and last_data is not None:
                return (from_data[key], last_data[key])
        except Exception:
            pass
            
        return (None, None)
    
if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.")
