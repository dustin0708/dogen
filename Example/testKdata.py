#-*-coding:utf-8-*-

from Script.Core import Kdata

bs=Kdata.download_basics()
print(bs.shape)
db = Kdata.DbH5file('./tmp.h5')
#db=Kdata.DbMemory()
db.write_basics(bs)
bs=db.read_basics()
print(bs.shape)

code='600687'
gt=Kdata.download_kdata(bs.loc[code], start='2018-10-01', end='2018-12-18')
db.write_kdata(code, gt)
gt=Kdata.download_kdata(bs.loc[code], start='2018-11-15')
db.write_kdata(code, gt)
gt=db.read_kdata(code)
print(gt)