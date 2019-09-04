#-*-coding:utf-8-*-

import sys
import abc
import time
import pandas
import datetime


class ShowBase(metaclass=abc.ABCMeta):
    
    @abc.abstractmethod
    def InitTable(self):
        pass
    
    @abc.abstractmethod
    def AddRecord(self):
        pass
    
    @abc.abstractmethod
    def SortValues(self):
        pass
        
    @abc.abstractmethod
    def Display(self):
        pass


class Stdout(ShowBase):
    def __init__(self):
        self.table = None
        
    def InitTable(self, column, name=None):
        self.column = column
        self.table  = pandas.DataFrame(columns=self.column)
        self.name   = name
    
    def AddRecord(self, record):
        self.table = self.table.append(pandas.DataFrame([record], columns=self.column))
    
    def SortValues(self, column, ascending=True):
        if isinstance(column, list):
            self.table = self.table.sort_values(by=column, ascending=ascending)
        elif isinstance(column, str):
            self.table = self.table.sort_values(by=[column], ascending=ascending)
        pass
        
    def Display(self):
        print(self.table)

        
class Excel(ShowBase):
    def __init__(self, path):
        self.path  = path
        self.table = None
    
    def __init_filename(self, name=None):
        if name is not None:
            self.name  = name + '_' + datetime.date.today().strftime('%Y%m%d') + '_' + time.strftime("%H%M%S") + '.xlsx'
        else:
            self.name  = datetime.date.today().strftime('%Y%m%d') + '_' + time.strftime("%H%M%S") + '.xlsx'
        return self.name
    
    def InitTable(self, column, name=None):
        self.column = column
        self.table = pandas.DataFrame(columns=self.column)
        self.__init_filename(name=name)
    
    def AddRecord(self, record):
        self.table = self.table.append(pandas.DataFrame([record], columns=self.column))

    def SortValues(self, column, ascending=True):
        if isinstance(column, list):
            self.table = self.table.sort_values(by=column, ascending=ascending)
        elif isinstance(column, str):
            self.table = self.table.sort_values(by=[column], ascending=ascending)
        pass
        
    def Display(self):
        pathname = self.path + '/' + self.name
        self.table.to_excel(pathname, index=False)
        

if __name__ == "__main__":
    print("Welcome to " +  sys.argv[0] + " package.")